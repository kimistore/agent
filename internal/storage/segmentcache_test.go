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
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowListStore holds every LIST until it is released, and counts the calls, so
// a test can hold a cache fill open and observe what else it blocks.
// slowListStore blocks and counts only LISTs of blockPrefix, so the engine's own
// startup listing (recovery, retention offsets, checkpoint) does not pollute the
// counts or block on the gate.
type slowListStore struct {
	inner       ObjectStore
	blockPrefix string
	gate        chan struct{}
	started     chan struct{}
	startOne    sync.Once
	lists       atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
}

func newSlowListStore(inner ObjectStore, blockPrefix string) *slowListStore {
	return &slowListStore{
		inner:       inner,
		blockPrefix: blockPrefix,
		gate:        make(chan struct{}),
		started:     make(chan struct{}),
	}
}

func (s *slowListStore) Put(ctx context.Context, key string, r io.Reader) error {
	return s.inner.Put(ctx, key, r)
}
func (s *slowListStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}
func (s *slowListStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}
func (s *slowListStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return s.inner.GetRange(ctx, key, start, length)
}
func (s *slowListStore) GetVersion(ctx context.Context, key string) ([]byte, string, bool, error) {
	if c, ok := s.inner.(ConditionalObjectStore); ok {
		return c.GetVersion(ctx, key)
	}
	return nil, "", false, ErrUnsupported
}
func (s *slowListStore) PutVersion(ctx context.Context, key string, data []byte, version string) (string, error) {
	if c, ok := s.inner.(ConditionalObjectStore); ok {
		return c.PutVersion(ctx, key, data, version)
	}
	return "", ErrUnsupported
}

func (s *slowListStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	if prefix != s.blockPrefix {
		return s.inner.List(ctx, prefix)
	}
	s.lists.Add(1)
	now := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	// Record the high-water mark: the gate is still shut, so any second
	// concurrent LIST parks alongside the first and shows up here.
	for old := s.maxInFlight.Load(); now > old; old = s.maxInFlight.Load() {
		if s.maxInFlight.CompareAndSwap(old, now) {
			break
		}
	}
	s.startOne.Do(func() { close(s.started) })
	select {
	case <-s.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.inner.List(ctx, prefix)
}

func (s *slowListStore) release() { close(s.gate) }

// The regression this guards: cachedSegments used to hold cacheMu across the
// LIST. handleUpload needs that same lock to record a finished upload, so a
// slow LIST under concurrent reads could starve every upload worker, the
// upload queue would fill, and acks=all producers would time out with nothing
// uploading. That is what happened on knode3: the pool was wedged for 17 hours
// with 15,133 segments stranded on local disk.
//
// The uploader here stands in for handleUpload: it needs the same lock. It must
// make progress while a LIST is still blocked.
func TestCachedSegments_DoesNotHoldLockAcrossList(t *testing.T) {
	store := newSlowListStore(newCASStore(), "orders/0/")
	engine := newEngine(t, store)
	defer engine.Close()
	store.lists.Store(0)

	const readers = 8

	// Hold one fill open. This reader is inside the LIST, and with the old
	// implementation it would be holding cacheMu while it waited.
	go func() {
		_, _ = engine.cachedSegments(context.Background(), "orders/0/", "orders/0")
	}()
	<-store.started

	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = engine.cachedSegments(context.Background(), "orders/0/", "orders/0")
		}()
	}

	waitFor(t, func() bool { return store.inFlight.Load() >= 1 })

	// The uploader's critical section: exactly what handleUpload does after a
	// successful PUT. Under the old code this blocked behind the LIST.
	uploader := make(chan struct{})
	go func() {
		defer close(uploader)
		engine.cacheMu.Lock()
		engine.segmentCache["orders/0"] = append(engine.segmentCache["orders/0"], "orders/0/new-e1.log")
		engine.cacheMu.Unlock()
	}()

	select {
	case <-uploader:
	case <-time.After(5 * time.Second):
		t.Fatal("cacheMu was still held 5s after a LIST began: the uploader cannot " +
			"record an upload while a read is listing, which is the deadlock that " +
			"stranded 15,133 segments on knode3")
	}

	store.release()
	wg.Wait()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within 5s")
}

// Concurrent misses on one key must produce one LIST, not one per reader. The
// old serialising refill achieved this too, but only by holding the global
// lock across the call; per-key flights achieve it without that cost.
func TestCachedSegments_CollapsesConcurrentMisses(t *testing.T) {
	store := newSlowListStore(newCASStore(), "orders/0/")
	engine := newEngine(t, store)
	defer engine.Close()
	// Engine startup recovers log position by listing the partition once.
	// That call is not part of what is being measured.
	store.lists.Store(0)

	const readers = 32
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keys, err := engine.cachedSegments(context.Background(), "orders/0/", "orders/0")
			if err != nil {
				t.Errorf("cachedSegments: %v", err)
				return
			}
			if len(keys) != 0 {
				t.Errorf("got %d keys, want 0", len(keys))
			}
		}()
	}

	waitFor(t, func() bool { return store.inFlight.Load() >= 1 })
	store.release()
	wg.Wait()

	// At most one LIST at a time is the property that matters: a second
	// concurrent LIST would mean the readers each issued their own. Counting
	// total calls is not usable here, because the engine's background loops
	// list this prefix during startup and the exact count is timing-dependent.
	if got := store.maxInFlight.Load(); got != 1 {
		t.Errorf("%d readers had %d LISTs in flight at once, want 1", readers, got)
	}
	if got := store.lists.Load(); got > readers {
		t.Errorf("%d readers produced %d LISTs, want fewer than one each", readers, got)
	}
}

// A second call after a successful fill must be served from the cache.
func TestCachedSegments_SecondCallIsCached(t *testing.T) {
	store := newSlowListStore(newCASStore(), "orders/0/")
	engine := newEngine(t, store)
	defer engine.Close()
	store.release()

	for i := 0; i < 3; i++ {
		if _, err := engine.cachedSegments(context.Background(), "orders/0/", "orders/0"); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := store.lists.Load(); got != 1 {
		t.Errorf("got %d LISTs, want 1", got)
	}
}

// A failed LIST must not be retried once per reader. A store that is down would
// otherwise make every reader queue behind a call that is going to fail.
func TestCachedSegments_BacksOffAfterFailure(t *testing.T) {
	store := newFailListStore(newCASStore())
	engine := newEngine(t, store)
	defer engine.Close()
	store.lists.Store(0)

	for i := 0; i < 10; i++ {
		if _, err := engine.cachedSegments(context.Background(), "orders/0/", "orders/0"); err == nil {
			t.Fatal("want an error from a failing store")
		}
	}
	if got := store.lists.Load(); got != 1 {
		t.Errorf("a failing LIST was retried %d times in the backoff window, want 1", got)
	}
}

// failListStore fails every LIST, counting the attempts.
type failListStore struct {
	inner ObjectStore
	lists atomic.Int64
}

func newFailListStore(inner ObjectStore) *failListStore {
	return &failListStore{inner: inner}
}

func (s *failListStore) Put(ctx context.Context, key string, r io.Reader) error {
	return s.inner.Put(ctx, key, r)
}
func (s *failListStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}
func (s *failListStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}
func (s *failListStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return s.inner.GetRange(ctx, key, start, length)
}
func (s *failListStore) GetVersion(ctx context.Context, key string) ([]byte, string, bool, error) {
	if c, ok := s.inner.(ConditionalObjectStore); ok {
		return c.GetVersion(ctx, key)
	}
	return nil, "", false, ErrUnsupported
}
func (s *failListStore) PutVersion(ctx context.Context, key string, data []byte, version string) (string, error) {
	if c, ok := s.inner.(ConditionalObjectStore); ok {
		return c.PutVersion(ctx, key, data, version)
	}
	return "", ErrUnsupported
}
func (s *failListStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	if prefix != "orders/0/" {
		return s.inner.List(ctx, prefix)
	}
	s.lists.Add(1)
	return nil, fmt.Errorf("list refused")
}

// A fill that was in flight when retention invalidated the key must not
// republish the segments retention just deleted. Generation counting is what
// prevents that, and this is the test for it: without it, a slow LIST would
// resurrect deleted segments and consumers would be sent to objects that no
// longer exist.
func TestCachedSegments_InFlightFillIsDiscarded(t *testing.T) {
	store := newSlowListStore(newCASStore(), "orders/0/")
	engine := newEngine(t, store)
	defer engine.Close()
	store.lists.Store(0)

	if err := store.inner.Put(context.Background(), "orders/0/0-1.log", strings.NewReader("x")); err != nil {
		t.Fatalf("seed: %v", err)
	}

	filled := make(chan struct{})
	go func() {
		defer close(filled)
		// This LIST returns one segment, and it lands after the invalidation.
		_, _ = engine.cachedSegments(context.Background(), "orders/0/", "orders/0")
	}()
	<-store.started

	// Retention sweeps while the LIST is still running.
	engine.invalidateCache("orders/0")

	store.release()
	<-filled

	engine.cacheMu.RLock()
	cached, ok := engine.segmentCache["orders/0"]
	engine.cacheMu.RUnlock()
	if ok {
		t.Fatalf("a fill that started before invalidation republished %v", cached)
	}

	// The next read must go back to the store rather than serve the discarded
	// result, and now see the deletion.
	keys, err := engine.cachedSegments(context.Background(), "orders/0/", "orders/0")
	if err != nil {
		t.Fatalf("reread after invalidation: %v", err)
	}
	if len(keys) != 1 || keys[0] != "orders/0/0-1.log" {
		t.Errorf("reread returned %v, want the one segment that still exists", keys)
	}
}
