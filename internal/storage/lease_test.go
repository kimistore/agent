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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// casStore is an in-memory object store with real compare-and-swap semantics,
// which is what the lease is built on. A version is a counter bumped on every
// write, so a conditional write that names a stale version fails the same way
// S3's If-Match does.
type casStore struct {
	mu      sync.Mutex
	data    map[string][]byte
	version map[string]int64
	failPut error
	blocks  chan struct{} // when non-nil, every Put waits on it
}

func newCASStore() *casStore {
	return &casStore{data: map[string][]byte{}, version: map[string]int64{}}
}

func (s *casStore) Put(ctx context.Context, key string, r io.Reader) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if s.failPut != nil {
		return s.failPut
	}
	if s.blocks != nil {
		<-s.blocks
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = body
	s.version[key]++
	return nil
}

func (s *casStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (s *casStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ObjectMetadata
	for k := range s.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, ObjectMetadata{Key: k, Size: int64(len(s.data[k])), LastModified: time.Now().Unix()})
		}
	}
	return out, nil
}

func (s *casStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.version, key)
	return nil
}

func (s *casStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	if start > int64(len(b)) {
		return nil, fmt.Errorf("range beyond end")
	}
	end := int64(len(b))
	if length > 0 && start+length < end {
		end = start + length
	}
	return io.NopCloser(bytes.NewReader(b[start:end])), nil
}

func (s *casStore) GetVersion(ctx context.Context, key string) ([]byte, string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.data[key]
	if !ok {
		return nil, "", false, nil
	}
	return b, fmt.Sprintf("v%d", s.version[key]), true, nil
}

func (s *casStore) PutVersion(ctx context.Context, key string, data []byte, version string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.version[key]
	switch {
	case version == "" && exists:
		return "", ErrVersionMismatch
	case version != "":
		if !exists || fmt.Sprintf("v%d", current) != version {
			return "", ErrVersionMismatch
		}
	}
	s.data[key] = data
	s.version[key]++
	return fmt.Sprintf("v%d", s.version[key]), nil
}

// lease reads the lease record back out of the store.
func (s *casStore) lease(t *testing.T, key string) Lease {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.data[key]
	if !ok {
		t.Fatalf("no lease object at %s", key)
	}
	var l Lease
	if err := json.Unmarshal(raw, &l); err != nil {
		t.Fatalf("lease object is not valid JSON: %v", err)
	}
	return l
}

// plainStore is an ObjectStore with no conditional writes at all. It wraps
// the same backing data without embedding it, so the conditional methods are
// genuinely absent from its method set rather than merely unused.
type plainStore struct{ inner *casStore }

func (p *plainStore) Put(ctx context.Context, key string, r io.Reader) error {
	return p.inner.Put(ctx, key, r)
}
func (p *plainStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return p.inner.Get(ctx, key)
}
func (p *plainStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	return p.inner.List(ctx, prefix)
}
func (p *plainStore) Delete(ctx context.Context, key string) error {
	return p.inner.Delete(ctx, key)
}
func (p *plainStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return p.inner.GetRange(ctx, key, start, length)
}

func newEngine(t *testing.T, store ObjectStore, opts ...Option) *StorageEngine {
	t.Helper()
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{}, opts...)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	return engine
}

func testLease(holder string, ttl time.Duration) LeaseConfig {
	return LeaseConfig{Enabled: true, Holder: holder, TTL: ttl}
}

// A second agent pointed at the same bucket must be refused at startup. The
// corruption this prevents is silent: both agents would hand out the same
// offsets and write the same segment keys.
func TestLease_SecondWriterIsRefused(t *testing.T) {
	store := newCASStore()

	first := newEngine(t, store, WithLease(testLease("agent-a", 30*time.Second)))
	if !first.lease.Owned() {
		t.Fatal("first engine did not take the lease")
	}

	_, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithLease(testLease("agent-b", 30*time.Second)))
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("second engine should have been refused with ErrLeaseHeld, got %v", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A graceful shutdown hands the log over, so a replacement does not have to
// wait out the TTL.
func TestLease_ReleasedOnClose(t *testing.T) {
	store := newCASStore()

	first := newEngine(t, store, WithLease(testLease("agent-a", time.Hour)))
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithLease(testLease("agent-b", time.Hour)))
	if err != nil {
		t.Fatalf("replacement was blocked after a clean shutdown: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close replacement: %v", err)
	}
}

// The fencing epoch has to survive a clean restart. It is stamped into the
// checkpoint, so an epoch that resets to 1 on every handover would make the
// next agent refuse a perfectly good log as superseded -- a restart broken by
// the mechanism meant to protect it.
func TestLease_EpochIsMonotonicAcrossRestarts(t *testing.T) {
	store := newCASStore()
	cond, ok := ObjectStore(store).(ConditionalObjectStore)
	if !ok {
		t.Fatal("the test store must support conditional writes")
	}

	first := newEngine(t, store, WithLease(testLease("agent-a", time.Hour)))

	// Push the epoch forward the way a renewal would, so the checkpoint is
	// written at an epoch higher than a fresh agent would start at.
	if _, err := first.lease.claim(context.Background(), cond); err != nil {
		t.Fatalf("renew: %v", err)
	}
	firstEpoch := first.lease.Epoch()
	if err := first.SaveCheckpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithLease(testLease("agent-b", time.Hour)))
	if err != nil {
		t.Fatalf("a restart must not be refused as superseded: %v", err)
	}

	if got := second.lease.Epoch(); got <= firstEpoch {
		t.Fatalf("epoch went backwards across a restart: %d after %d", got, firstEpoch)
	}

	// And it must keep climbing, handover after handover.
	secondEpoch := second.lease.Epoch()
	if err := second.Close(); err != nil {
		t.Fatalf("close second: %v", err)
	}
	third, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithLease(testLease("agent-c", time.Hour)))
	if err != nil {
		t.Fatalf("second restart refused: %v", err)
	}
	defer third.Close()
	if got := third.lease.Epoch(); got <= secondEpoch {
		t.Fatalf("epoch went backwards on the second handover: %d after %d", got, secondEpoch)
	}
}

// A crashed holder blocks its replacement only until the claim expires, and
// the replacement then takes over at a higher epoch.
func TestLease_TakeoverAfterExpiry(t *testing.T) {
	store := newCASStore()
	ls, err := acquireLease(context.Background(), store, testLease("crashed-agent", 50*time.Millisecond))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer ls.close()

	time.Sleep(80 * time.Millisecond) // the holder stops renewing, as a crash would

	next, err := acquireLease(context.Background(), store, testLease("replacement", 30*time.Second))
	if err != nil {
		t.Fatalf("expired lease did not free the log: %v", err)
	}
	defer next.close()

	if next.Epoch() <= ls.Epoch() {
		t.Fatalf("takeover epoch %d must exceed the previous epoch %d", next.Epoch(), ls.Epoch())
	}
	if got := store.lease(t, DefaultLeaseKey); got.Holder != "replacement" {
		t.Fatalf("lease record holder = %q, want replacement", got.Holder)
	}
}

// A lease that cannot be fenced is either a startup failure or a loud warning,
// never silence.
func TestLease_StoreWithoutConditionalWrites(t *testing.T) {
	store := &plainStore{inner: newCASStore()}

	_, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithLease(LeaseConfig{Enabled: true, Holder: "a", TTL: time.Minute, Require: true}))
	if !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Require=true should refuse a store that cannot fence, got %v", err)
	}

	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithLease(LeaseConfig{Enabled: true, Holder: "a", TTL: time.Minute}))
	if err != nil {
		t.Fatalf("Require=false should still start, got %v", err)
	}
	// An unfenced engine must not claim it may write, or a later lease loss
	// would go unnoticed.
	if engine.lease.WriterAllowed() {
		t.Error("an unfenced engine must not report the write path as safe")
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// Losing the claim must stop writes. This is the property that makes the lease
// a fence rather than bookkeeping.
func TestLease_LostLeaseRefusesWrites(t *testing.T) {
	store := newCASStore()
	engine := newEngine(t, store, WithLease(testLease("agent-a", time.Hour)))
	defer engine.Close()

	// Simulate a takeover that this agent never observed: the claim is gone
	// and the epoch has moved on.
	store.mu.Lock()
	store.data[DefaultLeaseKey] = []byte(`{"holder":"agent-b","epoch":99,"expires_at":` +
		fmt.Sprint(time.Now().Add(time.Hour).Unix()) + `}`)
	store.version[DefaultLeaseKey]++
	store.mu.Unlock()

	// The renewal loop is slower than the test, so drive the decision the same
	// way it drives itself: a renewal that cannot renew, aged past the TTL.
	engine.lease.held.Store(false)

	if _, err := engine.Append("t", 0, []byte("x"), 1, false); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("append after lease loss = %v, want ErrLeaseLost", err)
	}
}

// Durable state written by a newer writer must stop this agent from serving,
// because its offsets are no longer authoritative.
func TestLease_CheckpointFromNewerEpochIsRefused(t *testing.T) {
	store := newCASStore()

	// A checkpoint left behind by a writer that held the log after us.
	checkpoint := MetadataCache{
		WriterEpoch: 42,
		Writer:      "agent-newer",
		Topics:      map[string]*TopicState{},
	}
	data, err := checkpoint.ToJSON()
	if err != nil {
		t.Fatalf("marshal checkpoint: %v", err)
	}
	if err := store.Put(context.Background(), "_meta/checkpoint.json", bytes.NewReader(data)); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	// Our epoch is 1, whatever the store happens to hold.
	ls, err := acquireLease(context.Background(), store, testLease("agent-old", time.Minute))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer ls.close()
	if ls.Epoch() >= 42 {
		t.Fatalf("test needs a lower epoch than the checkpoint's; got %d", ls.Epoch())
	}

	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{}, WithLease(testLease("agent-old", time.Minute)))
	if err == nil {
		engine.Close()
		t.Fatal("an agent should refuse to serve a log whose checkpoint is from a newer writer")
	}
	if !strings.Contains(err.Error(), "newer writer") {
		t.Fatalf("error should name the cause, got %v", err)
	}
}

// The epoch is monotonic across renewals, which is what makes it usable as a
// fencing token in durable records.
func TestLease_EpochIncreasesOnRenewal(t *testing.T) {
	store := newCASStore()
	ls, err := acquireLease(context.Background(), store, testLease("agent-a", 200*time.Millisecond))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer ls.close()

	first := ls.Epoch()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ls.Epoch() > first {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("epoch never advanced past %d; renewal is not happening", first)
}

// Every object-store call must be bounded. A store that never answers must not
// be able to pin the engine's shutdown.
func TestOperationTimeout_BoundsHungStore(t *testing.T) {
	store := newBlockingStore()
	engine := newEngine(t, store, WithOperationTimeout(50*time.Millisecond))
	defer func() { close(store.release); engine.Close() }()

	store.armed.Store(true)

	done := make(chan error, 1)
	go func() {
		_, err := engine.LoadOffset("group", "topic", 0)
		done <- err
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("LoadOffset ignored the operation timeout; the call is unbounded")
	}
}

// A hung store must not hold shutdown open either: the engine cancels its
// in-flight calls when Close runs.
func TestOperationTimeout_ShutdownReleasesHungStore(t *testing.T) {
	store := newBlockingStore()
	engine := newEngine(t, store, WithOperationTimeout(time.Hour))

	store.armed.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = engine.LoadOffset("group", "topic", 0)
	}()
	time.Sleep(50 * time.Millisecond)

	closed := make(chan struct{})
	go func() { engine.Close(); close(closed) }()

	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		close(store.release)
		t.Fatal("Close waited on an in-flight object-store call it should have cancelled")
	}

	// The cancelled read must come back too, not linger until the store does.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(store.release)
		t.Fatal("the cancelled read never returned")
	}
	close(store.release)
}

// Startup recovery must also be bounded, or an agent cannot be restarted while
// the object store is slow.
func TestOperationTimeout_BoundsStartupRecovery(t *testing.T) {
	store := newBlockingStore()
	store.armed.Store(true)

	done := make(chan error, 1)
	go func() {
		engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
			WithOperationTimeout(50*time.Millisecond))
		if engine != nil {
			defer engine.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("engine should start despite a slow store: %v", err)
		}
	case <-time.After(10 * time.Second):
		close(store.release)
		t.Fatal("startup recovery ignored the operation timeout")
	}
	close(store.release)
}

// blockingStore answers every read with silence once armed. It has to be armed
// explicitly, because engine construction reads the log too and a test about
// shutdown must not hang in startup.
type blockingStore struct {
	inner   *casStore
	armed   atomic.Bool
	release chan struct{}
}

func newBlockingStore() *blockingStore {
	return &blockingStore{inner: newCASStore(), release: make(chan struct{})}
}

func (b *blockingStore) Put(ctx context.Context, key string, r io.Reader) error {
	return b.inner.Put(ctx, key, r)
}
func (b *blockingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if b.armed.Load() {
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return b.inner.Get(ctx, key)
}
func (b *blockingStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	return b.inner.List(ctx, prefix)
}
func (b *blockingStore) Delete(ctx context.Context, key string) error {
	return b.inner.Delete(ctx, key)
}
func (b *blockingStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return b.inner.GetRange(ctx, key, start, length)
}
func (b *blockingStore) GetVersion(ctx context.Context, key string) ([]byte, string, bool, error) {
	return b.inner.GetVersion(ctx, key)
}
func (b *blockingStore) PutVersion(ctx context.Context, key string, data []byte, version string) (string, error) {
	return b.inner.PutVersion(ctx, key, data, version)
}

// A cancelled caller context must stop the read it started.
func TestReadBatchContext_CancelledByCaller(t *testing.T) {
	store := newCASStore()
	engine := newEngine(t, store)
	defer engine.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := engine.ReadBatchContext(ctx, "topic", 0, 0, 1024)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read = %v, want context.Canceled", err)
	}
}
