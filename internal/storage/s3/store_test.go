/*
 * Copyright 2026 by Andy Lo-A-Foe
 *
 * This file is part of kimistore-agent.
 *
 * Licensed under the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package s3

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"kimistore/internal/storage"
)

// These tests drive the real AWS client against the repository's S3 shim,
// because the thing worth testing is the wire behaviour of the conditional
// writes the writer lease is built on: If-None-Match and If-Match as the SDK
// actually sends them, and 412 as the SDK actually reports it. A hand-written
// fake would only prove that the fake agrees with itself.
//
// They are skipped unless the shim can be started.
func startShim(t *testing.T) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available; skipping the S3 client tests")
	}
	repo := repoRoot(t)
	root := t.TempDir()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	cmd := exec.Command(python, filepath.Join(repo, "test", "s3shim.py"), strconv.Itoa(port), root)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start shim: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	endpoint := "http://127.0.0.1:" + strconv.Itoa(port)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), time.Second)
		if err == nil {
			conn.Close()
			t.Setenv("S3_ENDPOINT", endpoint)
			t.Setenv("AWS_ACCESS_KEY_ID", "test")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
			t.Setenv("AWS_REGION", "us-east-1")
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("shim did not become reachable")
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for dir := wd; dir != "/" && dir != "."; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
	}
	t.Fatalf("could not find the repository root from %s", wd)
	return ""
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewStore(context.Background(), "kimistore", "us-east-1")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return store
}

// The lease depends on create-if-absent being atomic: two agents starting at
// once must not both believe they created the claim.
func TestPutVersion_CreateIfAbsentIsAtomic(t *testing.T) {
	startShim(t)
	store := newTestStore(t)
	ctx := context.Background()

	key := "_meta/test-create.json"
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("seed cleanup: %v", err)
	}

	if _, err := store.PutVersion(ctx, key, []byte(`{"epoch":1}`), ""); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, err := store.PutVersion(ctx, key, []byte(`{"epoch":2}`), ""); !errors.Is(err, storage.ErrVersionMismatch) {
		t.Fatalf("second create-if-absent must be refused, got %v", err)
	}

	body, _, _, _ := store.GetVersion(ctx, key)
	if string(body) != `{"epoch":1}` {
		t.Fatalf("refused write still landed: object is %s", body)
	}
}

// A renewal must only succeed against the version it read, and a takeover that
// has already happened must not be overwritten by a renewal in flight.
func TestPutVersion_UpdateIsConditional(t *testing.T) {
	startShim(t)
	store := newTestStore(t)
	ctx := context.Background()

	key := "_meta/test-update.json"
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("seed cleanup: %v", err)
	}

	if _, err := store.PutVersion(ctx, key, []byte("first"), ""); err != nil {
		t.Fatalf("create: %v", err)
	}
	_, version, found, err := store.GetVersion(ctx, key)
	if err != nil || !found {
		t.Fatalf("GetVersion: found=%v err=%v", found, err)
	}

	if _, err := store.PutVersion(ctx, key, []byte("second"), version); err != nil {
		t.Fatalf("conditional update: %v", err)
	}
	if _, err := store.PutVersion(ctx, key, []byte("third"), version); !errors.Is(err, storage.ErrVersionMismatch) {
		t.Fatalf("stale conditional update must be refused, got %v", err)
	}

	body, _, _, _ := store.GetVersion(ctx, key)
	if string(body) != "second" {
		t.Fatalf("refused update still landed: object is %q", body)
	}
}

// "Not there yet" is a normal state for a lease, not an error.
func TestGetVersion_MissingKeyIsNotAnError(t *testing.T) {
	startShim(t)
	store := newTestStore(t)

	body, version, found, err := store.GetVersion(context.Background(), "_meta/definitely-absent.json")
	if err != nil {
		t.Fatalf("missing key returned an error: %v", err)
	}
	if found || version != "" || body != nil {
		t.Fatalf("missing key reported as present: body=%q version=%q found=%v", body, version, found)
	}
}

// A hung endpoint must not pin the store: the HTTP client carries a deadline of
// its own, under whatever the engine asks for.
func TestStore_TimeoutBoundsHungRequest(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	// Accept and hold every connection, answering none of them. The loop
	// matters: the client retries, and a listener that stops accepting would
	// make the later attempts block in dial instead of at the read, which is
	// a different timeout than the one under test.
	var mu sync.Mutex
	var held []net.Conn
	accepting := make(chan struct{})
	go func() {
		defer close(accepting)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-accepting
		mu.Lock()
		defer mu.Unlock()
		for _, c := range held {
			_ = c.Close()
		}
	})

	t.Setenv("S3_ENDPOINT", "http://"+ln.Addr().String())
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	// One attempt, so what is measured is the deadline rather than the SDK's
	// retry budget on top of it.
	store, err := newStore(context.Background(), "kimistore", "us-east-1", 200*time.Millisecond, 1)
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}

	start := time.Now()
	if _, err := store.List(context.Background(), "anything"); err == nil {
		t.Fatal("a request to a silent endpoint should fail")
	}
	elapsed := time.Since(start)

	if elapsed > 5*time.Second {
		t.Fatalf("one attempt took %s against a 200ms deadline; the timeout is not bounding it", elapsed)
	}
	t.Logf("gave up after %s", elapsed.Round(time.Millisecond))
}

// With the default retry budget the request still has to terminate. It costs
// more than one deadline because the SDK retries, which is correct: the point
// is that it ends, not that it ends immediately.
func TestStore_RetriesStillTerminate(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			defer func() { _ = conn.Close() }()
		}
	}()

	t.Setenv("S3_ENDPOINT", "http://"+ln.Addr().String())
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	store, err := NewStoreWithTimeout(context.Background(), "kimistore", "us-east-1", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("NewStoreWithTimeout: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := store.List(context.Background(), "anything")
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a request to a silent endpoint should fail")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the retried request never gave up")
	}
}
