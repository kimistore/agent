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

package storage

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
)

// faultStore fails a configurable number of Put calls before succeeding, so
// tests can simulate a transient object-storage outage.
type faultStore struct {
	mu       sync.Mutex
	failNext int
	puts     int
	saved    map[string]string
}

func newFaultStore(failNext int) *faultStore {
	return &faultStore{failNext: failNext, saved: map[string]string{}}
}

func (f *faultStore) Put(ctx context.Context, key string, r io.Reader) error {
	b, _ := io.ReadAll(r)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	if f.failNext > 0 {
		f.failNext--
		return errors.New("simulated object storage failure")
	}
	f.saved[key] = string(b)
	return nil
}

func (f *faultStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, errors.New("not found")
}
func (f *faultStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	return nil, nil
}
func (f *faultStore) Delete(ctx context.Context, key string) error { return nil }
func (f *faultStore) GetRange(ctx context.Context, key string, s, l int64) (io.ReadCloser, error) {
	return nil, errors.New("unsupported")
}

func (f *faultStore) get(key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.saved[key]
	return v, ok
}

func (f *faultStore) putCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts
}

func (f *faultStore) bufferedCount(se *StorageEngine) int {
	se.offsetBufMu.Lock()
	defer se.offsetBufMu.Unlock()
	return len(se.offsetBuf)
}

// TestFlushOffsets_RetriesAfterTransientFailure guards a silent data-loss bug.
//
// flushOffsets used to copy the pending map and then clear it before writing.
// Any failed write was logged and forgotten, so a brief object-storage blip
// permanently discarded a consumer-group commit that the client had already
// been told succeeded.
func TestFlushOffsets_RetriesAfterTransientFailure(t *testing.T) {
	store := newFaultStore(1) // first Put fails
	se, err := NewStorageEngine(t.TempDir(), store, "test", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	if err := se.SaveOffset("g", "orders", 0, 42); err != nil {
		t.Fatalf("SaveOffset: %v", err)
	}

	// First flush: the write fails transiently.
	se.flushOffsets()

	if _, ok := store.get("_offsets/g/orders/0"); ok {
		t.Fatal("offset should not have been persisted yet")
	}
	if n := store.bufferedCount(se); n != 1 {
		t.Errorf("buffered entries after a failed flush = %d, want 1 (must be retained for retry)", n)
	}

	// Second flush: the store has recovered.
	se.flushOffsets()

	got, ok := store.get("_offsets/g/orders/0")
	if !ok {
		t.Fatal("OFFSET LOST: acked offset 42 never reached object storage despite the store recovering")
	}
	if got != "42" {
		t.Errorf("persisted offset = %q, want \"42\"", got)
	}
	if n := store.bufferedCount(se); n != 0 {
		t.Errorf("buffered entries after a successful flush = %d, want 0", n)
	}
}

// TestFlushOffsets_KeepsNewerCommit checks that a slow flush cannot clobber a
// commit that arrived while it was in flight.
func TestFlushOffsets_KeepsNewerCommit(t *testing.T) {
	store := newFaultStore(0)
	se, err := NewStorageEngine(t.TempDir(), store, "test", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	se.SaveOffset("g", "orders", 0, 10)
	se.flushOffsets()

	// A newer commit for the same partition supersedes the flushed value.
	se.SaveOffset("g", "orders", 0, 20)
	se.flushOffsets()

	got, _ := store.get("_offsets/g/orders/0")
	if got != "20" {
		t.Errorf("persisted offset = %q, want \"20\"", got)
	}
}

// TestFlushOffsets_RetriesOnShutdown verifies Close makes a bounded effort to
// persist acked commits rather than dropping them on exit.
func TestFlushOffsets_RetriesOnShutdown(t *testing.T) {
	// Fail the first two attempts, succeed on the third.
	store := newFaultStore(2)
	se, err := NewStorageEngine(t.TempDir(), store, "test", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	se.SaveOffset("g", "orders", 0, 77)

	done := make(chan struct{})
	go func() { se.Close(); close(done) }()

	select {
	case <-done:
	case <-context.Background().Done():
	}

	if _, ok := store.get("_offsets/g/orders/0"); !ok {
		t.Errorf("LOST ON SHUTDOWN: acked offset 77 was not persisted during Close() after %d attempts", store.putCount())
	}
}
