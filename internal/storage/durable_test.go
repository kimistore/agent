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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kimistore/internal/storage/wal"
)

// failingUploadStore lets object writes for log segments fail while the
// metadata writes still succeed, which is the state an acks=all producer has
// to survive: the record is in the WAL, the segment never reaches storage, and
// the offset must not be acknowledged.
type failingUploadStore struct{ *MockStore }

func (s *failingUploadStore) Put(ctx context.Context, key string, r io.Reader) error {
	if strings.HasSuffix(key, ".log") {
		return errors.New("object store unavailable")
	}
	return s.MockStore.Put(ctx, key, r)
}

// TestDurable_AdvancesWhenSegmentUploads checks the happy path: an acks=all
// wait is released once the segment holding the offset is in object storage.
func TestDurable_AdvancesWhenSegmentUploads(t *testing.T) {
	store := NewMockStore()
	restore := sealAfterEveryAppend(t)
	defer restore()

	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithAgentID("durable"), WithFlushInterval(5*time.Millisecond))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer se.Close()

	if err := se.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	const n = 3
	target := int64(0)
	for i := 0; i < n; i++ {
		off, err := se.Append("orders", 0, recordBatch(int64(i), 1), 1, true)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		target = off + 1
	}
	// A final append rolls the segment containing the previous record, so
	// everything up to target is sealed and will be uploaded.
	if _, err := se.Append("orders", 0, recordBatch(n, 1), 1, true); err != nil {
		t.Fatalf("append roll: %v", err)
	}

	if err := se.WaitDurable(context.Background(), "orders", 0, target, 5*time.Second); err != nil {
		t.Fatalf("WaitDurable: %v", err)
	}
	if got := se.DurableOffset("orders", 0); got < target {
		t.Errorf("durable offset = %d, want at least %d", got, target)
	}
}

// TestDurable_TimesOutWhenUploadsFail is the safety property: if the segment
// never reaches object storage, the wait fails rather than acknowledging an
// offset the producer could not recover.
func TestDurable_TimesOutWhenUploadsFail(t *testing.T) {
	store := &failingUploadStore{NewMockStore()}
	restore := sealAfterEveryAppend(t)
	defer restore()

	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithAgentID("durable"), WithFlushInterval(5*time.Millisecond))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer se.Close()

	if err := se.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	off, err := se.Append("orders", 0, recordBatch(0, 1), 1, true)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	err = se.WaitDurable(context.Background(), "orders", 0, off+1, 200*time.Millisecond)
	if !errors.Is(err, ErrDurableTimeout) {
		t.Fatalf("WaitDurable error = %v, want ErrDurableTimeout", err)
	}
}

// TestDurable_FrontierOnlyMovesContiguously pins the out-of-order guard: the
// upload pool can finish a later segment before an earlier one, and the
// frontier must not run ahead of the gap. Otherwise a producer waiting on the
// gap would be acknowledged against a hole in the log.
func TestDurable_FrontierOnlyMovesContiguously(t *testing.T) {
	se := &StorageEngine{
		durable:   make(map[string]*durableState),
		waiters:   make(map[string]int64),
		durableCh: make(chan struct{}),
	}
	se.ensureDurable("orders", 0, 0)

	// The later segment lands first.
	se.markSegmentDurable("orders", 0, 100, 200)
	if got := se.DurableOffset("orders", 0); got != 0 {
		t.Fatalf("frontier after a gap = %d, want 0", got)
	}

	// Filling the gap should advance through both segments.
	se.markSegmentDurable("orders", 0, 0, 100)
	if got := se.DurableOffset("orders", 0); got != 200 {
		t.Fatalf("frontier after the gap closed = %d, want 200", got)
	}
}

// TestDurable_WaitReturnsImmediatelyBelowFrontier checks the cheap path: a
// waiter whose offset is already recoverable does not park.
func TestDurable_WaitReturnsImmediatelyBelowFrontier(t *testing.T) {
	se := &StorageEngine{
		durable:   make(map[string]*durableState),
		waiters:   make(map[string]int64),
		durableCh: make(chan struct{}),
	}
	se.markSegmentDurable("orders", 0, 0, 50)

	done := make(chan error, 1)
	go func() {
		done <- se.WaitDurable(context.Background(), "orders", 0, 40, 5*time.Second)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitDurable: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitDurable parked even though the offset was already durable")
	}
}

// TestDurable_FrontierRecoversFromStaleSeed covers the restart wedge.
//
// The frontier is seeded once per partition from the log end offset. When a
// restart leaves that seed behind the partition's real durable position, every
// segment that later lands has a base past the frontier. Those go to pending,
// and pending is only drained by a segment whose base is already at or below
// the frontier -- one that will never come. The partition then wedges and every
// acks=all producer on it times out forever, even though its records are
// already in object storage.
//
// This is what a live mirror looked like: "offset 130 not in object storage
// after 10s (durable offset 42)" while segments 128-130 sat in the bucket.
func TestDurable_FrontierRecoversFromStaleSeed(t *testing.T) {
	store := NewMockStore()
	tmp := t.TempDir()
	se, err := NewStorageEngine(tmp, store, "bucket", RetentionConfig{},
		WithAgentID("durable"), WithFlushInterval(5*time.Millisecond))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer se.Close()

	// The partition directory exists but holds no sealed segment: a restart
	// after every earlier segment was uploaded and trimmed from local disk.
	dir := filepath.Join(tmp, "orders", "0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// A restart seeded the frontier from a log end offset that under-reported
	// where the partition actually was.
	se.ensureDurable("orders", 0, 42)
	if got := se.DurableOffset("orders", 0); got != 42 {
		t.Fatalf("seeded frontier = %d, want 42", got)
	}

	// No sealed segment sits below 128, so everything below it is already in
	// object storage and the frontier must skip forward rather than wedge.
	se.markSegmentDurable("orders", 0, 128, 130)

	if got := se.DurableOffset("orders", 0); got != 130 {
		t.Errorf("durable offset = %d, want 130 (frontier wedged behind a stale seed)", got)
	}
	if err := se.WaitDurable(context.Background(), "orders", 0, 130, time.Second); err != nil {
		t.Errorf("WaitDurable after repair: %v", err)
	}
}

// TestDurable_FrontierHoldsForRealGap is the guard on the repair: the frontier
// may only skip ahead when nothing is actually waiting below it. A sealed
// segment still on local disk has not been uploaded, so the frontier must stop
// short of it rather than acknowledge an offset that is not recoverable.
func TestDurable_FrontierHoldsForRealGap(t *testing.T) {
	store := NewMockStore()
	tmp := t.TempDir()
	se, err := NewStorageEngine(tmp, store, "bucket", RetentionConfig{},
		WithAgentID("durable"), WithFlushInterval(5*time.Millisecond))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer se.Close()

	// A sealed segment at base 43 is still on local disk, so offsets from 43
	// are not in object storage yet.
	dir := filepath.Join(tmp, "orders", "0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	pending := filepath.Join(dir, wal.SegmentName(42, 1))
	if err := os.WriteFile(pending, []byte("not uploaded"), 0o644); err != nil {
		t.Fatalf("write pending segment: %v", err)
	}

	se.ensureDurable("orders", 0, 42)
	se.markSegmentDurable("orders", 0, 128, 130)

	if got := se.DurableOffset("orders", 0); got >= 128 {
		t.Errorf("durable offset = %d, want it held below 128 while a segment is unuploaded", got)
	}
	// Filling that gap advances the frontier by exactly that segment. It must
	// still not jump to 130: offsets 43..128 are covered by no segment that has
	// landed, so acknowledging them would invent durability.
	se.markSegmentDurable("orders", 0, 42, 43)
	if got := se.DurableOffset("orders", 0); got != 43 {
		t.Errorf("durable offset = %d, want 43 (only the gap segment is covered)", got)
	}
}
