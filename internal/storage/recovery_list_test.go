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
	"sync"
	"testing"
	"time"
)

// deadlineRecordingStore records the deadline it is handed, so a test can assert
// the budget a call actually received rather than inferring it from timing.
type deadlineRecordingStore struct {
	mu       sync.Mutex
	budgets  []time.Duration
	pageCost time.Duration
}

func (s *deadlineRecordingStore) record(ctx context.Context) {
	deadline, ok := ctx.Deadline()
	if !ok {
		s.mu.Lock()
		s.budgets = append(s.budgets, 0)
		s.mu.Unlock()
		return
	}
	s.mu.Lock()
	s.budgets = append(s.budgets, time.Until(deadline))
	s.mu.Unlock()
}

// reset drops what has been recorded so far. Used rather than indexing: the
// engine LISTs during its own construction, so any fixed offset into the
// recorded budgets is coupled to the engine's internals rather than to the
// property under test.
func (s *deadlineRecordingStore) reset() {
	s.mu.Lock()
	s.budgets = nil
	s.mu.Unlock()
}

// budget returns the single budget recorded since the last reset.
func (s *deadlineRecordingStore) budget() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.budgets) == 0 {
		return -1
	}
	return s.budgets[0]
}

func (s *deadlineRecordingStore) Put(ctx context.Context, key string, r io.Reader) error {
	_, _ = io.ReadAll(r)
	return nil
}

func (s *deadlineRecordingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not found")
}

func (s *deadlineRecordingStore) Delete(ctx context.Context, key string) error { return nil }

func (s *deadlineRecordingStore) GetRange(ctx context.Context, key string, a, b int64) (io.ReadCloser, error) {
	return nil, fmt.Errorf("unsupported")
}

func (s *deadlineRecordingStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	s.record(ctx)
	if s.pageCost > 0 {
		select {
		case <-time.After(s.pageCost):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	out := make([]ObjectMetadata, 0, 4)
	for i := 0; i < 4; i++ {
		out = append(out, ObjectMetadata{Key: fmt.Sprintf("%s%016d-e1.log", prefix, i)})
	}
	return out, nil
}

// TestRecoveryListing_GetsItsOwnBudget is the regression for the second outage
// on 2026-10-17.
//
// v1.0.27 made a lost claim recoverable, but recovery has to enumerate every
// segment of the partition, and it ran that enumeration under the same 30s bound
// that protects request handlers. knode3's prefix held over 12,000 objects at
// roughly 1000 per four seconds, so the listing needed about a minute and could
// never finish. The agent re-took the claim every renewal and then failed the
// position check every time, so the partition stayed fenced for as long as the
// bucket stayed large -- the same outage v1.0.27 was meant to end.
//
// Staying fenced given a position it could not vouch for was correct. Running
// out of budget and being unable to obtain one was not.
func TestRecoveryListing_GetsItsOwnBudget(t *testing.T) {
	store := &deadlineRecordingStore{}

	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	defer engine.Close()

	// A normal listing -- the one serving request handlers -- must stay on the
	// per-operation bound. That is the point of the bound and this test must not
	// quietly remove it.
	store.reset()
	if _, err := engine.objList(context.Background(), "mimir-distributor/"); err != nil {
		t.Fatalf("objList: %v", err)
	}
	normal := store.budget()
	if normal <= 0 {
		t.Fatalf("a request-path listing was given no bound (%v)", normal)
	}
	if normal > engine.opTimeout {
		t.Errorf("a request-path listing was given %v, above the per-operation bound %v",
			normal, engine.opTimeout)
	}

	// The recovery listing must be given materially more.
	store.reset()
	if _, err := engine.objectSegmentsByPartition(context.Background(), "mimir-distributor"); err != nil {
		t.Fatalf("objectSegmentsByPartition: %v", err)
	}
	recovery := store.budget()

	if recovery <= normal {
		t.Errorf("recovery listing got %v, no more than the request-path listing's %v; "+
			"a bucket larger than one request will time the inventory out and the "+
			"partition will never be recoverable again", recovery, normal)
	}
	if recovery <= engine.opTimeout {
		t.Errorf("recovery listing got %v, within the per-operation bound %v; "+
			"knode3 needed roughly a minute for 12,000 objects", recovery, engine.opTimeout)
	}
}

// TestRecoveryListing_OutlivesThePerOperationBudget is the same property
// end to end: an enumeration that takes longer than the per-operation bound must
// still succeed.
func TestRecoveryListing_OutlivesThePerOperationBudget(t *testing.T) {
	// The per-operation bound is shrunk so this does not have to spend 30s
	// proving a timeout does not fire. What matters is that recovery outlives
	// the bound it is given, not what that bound happens to be.
	const opTimeout = 500 * time.Millisecond
	work := opTimeout + time.Second
	store := &deadlineRecordingStore{pageCost: work}

	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOperationTimeout(opTimeout))
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	defer engine.Close()

	done := make(chan error, 1)
	go func() {
		_, err := engine.objectSegmentsByPartition(context.Background(), "mimir-distributor")
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("an enumeration of %v was cancelled: %v", work, err)
		}
	case <-time.After(recoveryListTimeout + 30*time.Second):
		t.Fatalf("the enumeration did not finish within %v", recoveryListTimeout)
	}
}

// TestRecoveryListing_StillCancellable keeps the longer budget from becoming an
// unbounded one. Recovery may take longer than a request; it may not ignore
// shutdown or cancellation.
func TestRecoveryListing_StillCancellable(t *testing.T) {
	store := &deadlineRecordingStore{pageCost: time.Hour}

	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOperationTimeout(time.Second))
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	defer engine.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := engine.objectSegmentsByPartition(ctx, "mimir-distributor")
		done <- err
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the enumeration ignored cancellation")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cancellation did not stop the enumeration")
	}
}
