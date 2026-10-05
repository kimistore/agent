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
	"strings"
	"testing"
	"time"
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
