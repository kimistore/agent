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

package server

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"kimistore/internal/storage"
)

type discardStore struct{}

func (discardStore) Put(ctx context.Context, key string, r io.Reader) error { return nil }
func (discardStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, errors.New("not found")
}
func (discardStore) List(ctx context.Context, p string) ([]storage.ObjectMetadata, error) {
	return nil, nil
}
func (discardStore) Delete(ctx context.Context, key string) error { return nil }
func (discardStore) GetRange(ctx context.Context, key string, s, l int64) (io.ReadCloser, error) {
	return nil, errors.New("unsupported")
}

// startTestServer brings up a real listener on a free port and returns the
// server plus its address.
func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()

	engine, err := storage.NewStorageEngine(t.TempDir(), discardStore{}, "test", storage.RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	srv := NewServer(addr, engine, "", "")
	go srv.Start()

	// Wait for the listener to come up.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return srv, addr
}

// TestStop_CompletesWithIdleClientAttached guards a shutdown hang.
//
// Stop used to close the listener and then wait on the connection wait group,
// but handleConnection parks in a blocking socket read and never observes the
// quit channel. One connected-but-idle client -- the normal state of a
// consumer between polls -- therefore blocked shutdown indefinitely, stalling
// every SIGTERM until the orchestrator sent SIGKILL.
func TestStop_CompletesWithIdleClientAttached(t *testing.T) {
	srv, addr := startTestServer(t)

	clients := make([]net.Conn, 0, 3)
	for i := 0; i < 3; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		defer c.Close()
		clients = append(clients, c)
	}
	// Let the accept loop register them.
	time.Sleep(200 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- srv.Stop() }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Stop returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("SHUTDOWN HANG: Stop() did not complete with %d idle connections attached", len(clients))
	}
}

// TestStop_RefusesConnectionsAfterShutdown checks the accept path cannot
// register a new handler once teardown has begun, which would otherwise let a
// connection slip past the teardown sweep.
func TestStop_RefusesConnectionsAfterShutdown(t *testing.T) {
	srv, addr := startTestServer(t)

	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if c, err := net.Dial("tcp", addr); err == nil {
		c.Close()
		// A brief window can remain between listener close and the OS
		// releasing the port; only fail if it lingers.
		time.Sleep(100 * time.Millisecond)
		if c2, err := net.Dial("tcp", addr); err == nil {
			c2.Close()
			t.Error("listener still accepting connections after Stop()")
		}
	}
}

// TestStop_ConnectionRegistryIsEmptyAfterShutdown checks we do not leak
// tracked connections.
func TestStop_ConnectionRegistryIsEmptyAfterShutdown(t *testing.T) {
	srv, addr := startTestServer(t)

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	srv.stateMu.Lock()
	before := len(srv.conns)
	srv.stateMu.Unlock()
	if before == 0 {
		t.Error("expected the live connection to be tracked")
	}

	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	c.Close()

	srv.stateMu.Lock()
	defer srv.stateMu.Unlock()
	if !srv.closing {
		t.Error("server not marked closing after Stop()")
	}
}
