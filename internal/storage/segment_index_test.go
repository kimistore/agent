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
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
)

// SegmentFor must pick the last segment starting at or below the offset, over
// an inventory far larger than a page of object storage could return.

// A read at the tail resolves from the open window without any object-store
// call. This is where essentially every read lands: consumers sit one offset
// behind the log end.
func TestSegmentKeyFor_HotReadIsFree(t *testing.T) {
	store := &listFailStore{inner: newCASStore(), failGet: true}
	engine := newEngine(t, store)
	defer engine.Close()

	engine.metadataCache.AddSegment("orders", 0, SegmentRef{Base: 100, Epoch: 1})
	engine.metadataCache.AddSegment("orders", 0, SegmentRef{Base: 200, Epoch: 1})

	// Every object-store operation fails, so a key that comes back at all is
	// proof that none was attempted. A counter would be racy: the engine's own
	// background loops make calls too.
	key, err := engine.segmentKeyFor(context.Background(), "orders", 0, 150)
	if err != nil {
		t.Fatalf("segmentKeyFor: %v", err)
	}
	if key != "orders/0/00000000000000000100-e1.log" {
		t.Errorf("got %q, want the base-100 segment", key)
	}
}

// The open window seals into an immutable bucket exactly once it fills, and
// sealing is what stops the checkpoint from growing with history.
func TestSealWindow_WritesOneBucketAndBoundsCheckpoint(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newEngine(t, store)
	defer engine.Close()

	for i := 0; i < RecentWindow; i++ {
		engine.recordSegmentUploaded(context.Background(), "orders", 0, int64(i)*100, int64(i)*100+100,
			fmt.Sprintf("orders/0/%020d-e1.log", i*100))
	}
	ps := engine.metadataCache.Topics["orders"].Partitions[0]
	if len(ps.Recent) != 0 {
		t.Errorf("window still holds %d segments after filling", len(ps.Recent))
	}
	if len(ps.Sealed) != 1 {
		t.Fatalf("got %d sealed buckets, want 1", len(ps.Sealed))
	}

	raw, err := engine.metadataCache.ToJSON()
	if err != nil {
		t.Fatalf("serialise: %v", err)
	}
	// The checkpoint now holds a directory entry, not the segments themselves.
	if n := len(raw); n > 2048 {
		t.Errorf("checkpoint is %d bytes for %d segments; it must not carry them", n, RecentWindow)
	}
}

// A cold read fetches the one bucket that covers the offset. It must not list.
func TestSegmentKeyFor_ColdReadFetchesOneBucket(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newEngine(t, store)
	defer engine.Close()

	// A sealed bucket covering 100..200, with the window empty.
	engine.metadataCache.AddSegment("orders", 0, SegmentRef{Base: 100, Epoch: 1})
	body, _ := json.Marshal([]SegmentRef{{Base: 100, Epoch: 1}, {Base: 200, Epoch: 1}})
	engine.metadataCache.SeededBucket("orders", 0, BucketRef{
		First: 100, Last: 200, Key: "orders/0/00000000000000000100-00000000000000000200.idx",
	}, body)

	// LIST fails throughout, so a resolved key can only have come from the
	// bucket that was fetched.
	key, err := engine.segmentKeyFor(context.Background(), "orders", 0, 150)
	if err != nil {
		t.Fatalf("segmentKeyFor: %v", err)
	}
	if key != "orders/0/00000000000000000100-e1.log" {
		t.Errorf("got %q, want the base-100 segment from the bucket", key)
	}
}

// A re-claimed partition writes the same base offset under a newer epoch, and
// both objects exist. The index must serve the newer one; otherwise a consumer
// is served a segment the current owner has already replaced.
func TestAddSegment_NewerEpochSupersedes(t *testing.T) {
	mc := NewMetadataCache()
	mc.AddSegment("orders", 0, SegmentRef{Base: 100, Epoch: 1})
	mc.AddSegment("orders", 0, SegmentRef{Base: 100, Epoch: 2})
	ps := mc.Topics["orders"].Partitions[0]
	if len(ps.Recent) != 1 {
		t.Fatalf("got %d entries at base 100, want 1", len(ps.Recent))
	}
	if got := ps.Recent[0].Epoch; got != 2 {
		t.Errorf("epoch = %d, want 2", got)
	}
}

// Retention drops whole buckets rather than editing them, so reclaiming
// history is a few DELETEs and not a scan of the index.
func TestDropSegmentsBefore_RemovesWholeBuckets(t *testing.T) {
	mc := NewMetadataCache()
	mc.SeededBucket("orders", 0, BucketRef{First: 0, Last: 99, Key: "a.idx"}, nil)
	mc.SeededBucket("orders", 0, BucketRef{First: 100, Last: 199, Key: "b.idx"}, nil)
	mc.SeededBucket("orders", 0, BucketRef{First: 200, Last: 299, Key: "c.idx"}, nil)
	mc.AddSegment("orders", 0, SegmentRef{Base: 300, Epoch: 1})

	dropped := mc.DropSegmentsBefore("orders", 0, 200)
	if len(dropped) != 2 {
		t.Fatalf("dropped %d buckets, want 2", len(dropped))
	}
	ps := mc.Topics["orders"].Partitions[0]
	if len(ps.Sealed) != 1 || ps.Sealed[0].Key != "c.idx" {
		t.Errorf("remaining sealed buckets = %v, want only c.idx", ps.Sealed)
	}
	if len(ps.Recent) != 1 || ps.Recent[0].Base != 300 {
		t.Errorf("window = %v, want only base 300", ps.Recent)
	}
}

// listFailStore fails every LIST, so a read that still reaches for one cannot
// pass. That is the whole point: the index replaced the listing.
type listFailStore struct {
	inner   ObjectStore
	lists   atomic.Int64
	gets    atomic.Int64
	failGet bool
}

func (f *listFailStore) Put(ctx context.Context, key string, r io.Reader) error {
	return f.inner.Put(ctx, key, r)
}

func (f *listFailStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	f.gets.Add(1)
	if f.failGet {
		return nil, fmt.Errorf("GET is not available")
	}
	return f.inner.Get(ctx, key)
}

func (f *listFailStore) Delete(ctx context.Context, key string) error {
	return f.inner.Delete(ctx, key)
}

func (f *listFailStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return f.inner.GetRange(ctx, key, start, length)
}

func (f *listFailStore) GetVersion(ctx context.Context, key string) ([]byte, string, bool, error) {
	if c, ok := f.inner.(ConditionalObjectStore); ok {
		return c.GetVersion(ctx, key)
	}
	return nil, "", false, ErrUnsupported
}

func (f *listFailStore) PutVersion(ctx context.Context, key string, data []byte, version string) (string, error) {
	if c, ok := f.inner.(ConditionalObjectStore); ok {
		return c.PutVersion(ctx, key, data, version)
	}
	return "", ErrUnsupported
}

func (f *listFailStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	f.lists.Add(1)
	return nil, fmt.Errorf("LIST is not available")
}

// The checkpoint must stay bounded as a partition accumulates segments. This is
// the guard for the regression where the new index was added but recovery kept
// repopulating the old flat one, so the checkpoint carried every segment ever
// written and grew exactly as before -- with a test suite that still passed.
//
// The bound is the open window: a checkpoint holds at most RecentWindow
// entries plus one directory entry per sealed bucket, so its size is set by
// RecentWindow and not by history. It therefore oscillates within the window
// cycle rather than growing monotonically, which is why this samples across a
// whole cycle and takes the largest, not the last.
func TestCheckpoint_StaysBoundedAsSegmentsAccumulate(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newEngine(t, store)
	defer engine.Close()

	sizeAt := func(n int) int {
		for i := 0; i < n; i++ {
			base := int64(i) * 100
			engine.recordSegmentUploaded(context.Background(), "orders", 0, base, base+100,
				fmt.Sprintf("orders/0/%020d-e1.log", base))
		}
		raw, err := engine.metadataCache.ToJSON()
		if err != nil {
			t.Fatalf("serialise: %v", err)
		}
		return len(raw)
	}

	// Sample across a full window cycle so the peak is seen, not a phase.
	peak := 0
	segments := 0
	for i := 0; i < 24; i++ {
		segments += RecentWindow / 3
		if n := sizeAt(RecentWindow / 3); n > peak {
			peak = n
		}
	}

	// A flat index would be ~106 bytes per segment; at this many segments that
	// is several megabytes. Bounded by the window it is a few tens of KB.
	const perEntryBudget = 24
	budget := RecentWindow*perEntryBudget + 4096
	if peak > budget {
		t.Errorf("checkpoint peaked at %d bytes after %d segments, budget %d; "+
			"it is carrying the whole index again", peak, segments, budget)
	}
	t.Logf("peak checkpoint %d bytes across %d segments (was ~%d with a flat index)",
		peak, segments, segments*106)
}
