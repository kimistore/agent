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
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kimistore/internal/storage/wal"
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
			fmt.Sprintf("orders/0/%020d-e1.log", i*100), 0)
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
	putBucket(t, store, "orders/0/00000000000000000100-00000000000000000200.idx",
		[]SegmentRef{{Base: 100, Epoch: 1}, {Base: 200, Epoch: 1}})
	engine.metadataCache.SeededBucket("orders", 0, BucketRef{
		First: 100, Last: 200, Key: "orders/0/00000000000000000100-00000000000000000200.idx",
	})

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
	mc.SeededBucket("orders", 0, BucketRef{First: 0, Last: 99, Key: "a.idx"})
	mc.SeededBucket("orders", 0, BucketRef{First: 100, Last: 199, Key: "b.idx"})
	mc.SeededBucket("orders", 0, BucketRef{First: 200, Last: 299, Key: "c.idx"})
	mc.AddSegment("orders", 0, SegmentRef{Base: 300, Epoch: 1})

	dropped, _, reclaimed := mc.DropSegmentsBefore("orders", 0, 200)
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
	// Base 300 is above the boundary, so it stays in the window; only entries
	// below it would come back for the caller to delete.
	if len(reclaimed) != 0 {
		t.Errorf("reclaimed = %v, want nothing: every window entry is above the boundary", reclaimed)
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
				fmt.Sprintf("orders/0/%020d-e1.log", base), 0)
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

// A restart must not shrink the checkpoint. It used to: a claim that failed for
// a transient reason made claimKnownPartitions log and continue, and
// dropUnownedPartitions then removed the partition, so the next checkpoint
// persisted an empty topic set over the good one. The agent following that
// found no index, no log position, and no idea the partition existed -- seen on
// knode3 as a checkpoint collapsing from 35 KB to 259 bytes.
//
// This drives the path directly: the partition is unowned, startup runs, and
// the durable state must survive it.
func TestRestart_DoesNotErasePartitionStateWhenUnowned(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")
	engine.CreateTopic("orders", 1)
	engine.Append("orders", 0, batchOfN(40), 40, true)
	engine.walMgr.FlushPartition("orders", 0)
	waitForClaimedSegment(t, store, "orders/0/"+walSegmentName(0, 1))
	for i := 0; i < 40; i++ {
		engine.recordSegmentUploaded(context.Background(), "orders", 0, int64(i)*10, int64(i)*10+10,
			fmt.Sprintf("orders/0/%s", walSegmentName(int64(i)*10, 1)), 0)
	}
	before, err := engine.metadataCache.ToJSON()
	if err != nil {
		t.Fatalf("serialise: %v", err)
	}
	leo := engine.metadataCache.LogEndOffset("orders", 0)
	engine.Close()

	// Another agent now holds the lease, so agent-a restarts fenced but unowned.
	store.expireClaim(t, "orders", 0)
	next := newOwningEngine(t, store, "agent-b")
	defer next.Close()
	_ = leo

	if !next.metadataCache.Has("orders", 0) {
		t.Fatal("a restart under a different agent erased the partition from the " +
			"durable state; the index and log position are gone with it")
	}
	if got := next.metadataCache.LogEndOffset("orders", 0); got == 0 {
		t.Error("log end offset was erased by a restart under another agent")
	}
	after, err := next.metadataCache.ToJSON()
	if err != nil {
		t.Fatalf("serialise after: %v", err)
	}
	if len(after) < len(before)/2 {
		t.Errorf("checkpoint shrank from %d to %d bytes across a restart", len(before), len(after))
	}
}

// A graceful shutdown must not erase the durable index. It used to, and this
// is the shape of what happened on knode3:
//
//	Draining 1 owned partition(s) before shutdown, budget 25s
//	Ownership: released mimir-distributor/0 at epoch 21405
//	Drain: drained 1 of 1 partition(s); 0 kept their claim
//	Saved metadata checkpoint to _agents/knode3/checkpoint.json (259 bytes)
//
// The drain released the claim and then dropped the partition from the view,
// which removed its position and index, and the next checkpoint persisted that
// empty state over the good one. The segments were still in object storage; only
// the index describing them was gone, and rebuilding it needs the listing the
// index exists to avoid.
func TestShutdownDrain_KeepsIndexAndPosition(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")
	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(40), 40, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	engine.walMgr.FlushPartition("orders", 0)
	waitForClaimedSegment(t, store, "orders/0/"+walSegmentName(0, 1))
	for i := 0; i < 40; i++ {
		engine.recordSegmentUploaded(context.Background(), "orders", 0, int64(i)*10, int64(i)*10+10,
			fmt.Sprintf("orders/0/%s", walSegmentName(int64(i)*10, 1)), 0)
	}
	before, err := engine.metadataCache.ToJSON()
	if err != nil {
		t.Fatalf("serialise: %v", err)
	}
	if len(before) < 300 {
		t.Fatalf("test setup: checkpoint is only %d bytes", len(before))
	}

	// The shutdown drain: everything released, nothing handed to a peer.
	if _, err := engine.DrainOwned(t.Context(), DefaultHandoverBudget); err != nil {
		t.Logf("drain reported: %v", err)
	}

	after, err := engine.metadataCache.ToJSON()
	if err != nil {
		t.Fatalf("serialise after drain: %v", err)
	}
	if len(after) < len(before)/2 {
		t.Errorf("checkpoint shrank from %d to %d bytes across a shutdown drain; "+
			"the index and position were erased", len(before), len(after))
	}
	if !engine.metadataCache.Has("orders", 0) {
		t.Error("a shutdown drain removed the partition entirely; nothing takes " +
			"over on shutdown, so this agent is the next reader")
	}
	if got := engine.metadataCache.LogEndOffset("orders", 0); got == 0 {
		t.Error("a shutdown drain erased the log end offset")
	}
}

// Reclaiming through the index removes whole buckets and their segments, and
// keeps the surviving window entries. This is the path that has to work without
// a listing, since a listing of a real partition cannot complete.
func TestReclaimIndex_RemovesBucketAndItsSegments(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newEngine(t, store)
	defer engine.Close()

	bucketKey := "orders/0/00000000000000000000-00000000000000000200.idx"
	refs := []SegmentRef{{Base: 0, Epoch: 1}, {Base: 100, Epoch: 1}, {Base: 200, Epoch: 1}}
	putBucket(t, store, bucketKey, refs)
	engine.metadataCache.SeededBucket("orders", 0, BucketRef{First: 0, Last: 200, Key: bucketKey})
	for _, r := range refs {
		key := fmt.Sprintf("orders/0/%s", wal.SegmentName(r.Base, r.Epoch))
		if err := store.inner.Put(context.Background(), key, strings.NewReader("x")); err != nil {
			t.Fatalf("seed segment: %v", err)
		}
	}
	engine.metadataCache.AddSegment("orders", 0, SegmentRef{Base: 300, Epoch: 1})
	if err := store.inner.Put(context.Background(), "orders/0/"+wal.SegmentName(300, 1), strings.NewReader("x")); err != nil {
		t.Fatalf("seed tail segment: %v", err)
	}

	deleted := engine.reclaimIndex(context.Background(), "orders", 0, 250)

	if deleted != 3 {
		t.Errorf("deleted %d segments, want the 3 the bucket covered", deleted)
	}
	// No LIST is consulted on this path. The store's List always fails, so a
	// reclaim that depended on one would have deleted nothing -- the three
	// deletions above are the proof. A counter would be racy here, because the
	// engine's background loops list independently.
	ps := engine.metadataCache.Topics["orders"].Partitions[0]
	if len(ps.Sealed) != 0 {
		t.Errorf("bucket still in the index: %v", ps.Sealed)
	}
	if len(ps.Recent) != 1 || ps.Recent[0].Base != 300 {
		t.Errorf("window = %v, want only the segment above the boundary", ps.Recent)
	}
	// The bucket and its segments must actually be gone from the store.
	if rc, err := store.inner.Get(context.Background(), bucketKey); err == nil {
		_ = rc.Close()
		t.Error("the index bucket object was not deleted")
	}
	for _, r := range refs {
		key := fmt.Sprintf("orders/0/%s", wal.SegmentName(r.Base, r.Epoch))
		if rc, err := store.inner.Get(context.Background(), key); err == nil {
			_ = rc.Close()
			t.Errorf("segment %s was not deleted", key)
		}
	}
}

// A bucket the boundary passes through is rewritten rather than dropped. Leaving
// it alone would leave the index naming segments that no longer exist, so a read
// of a reclaimed offset would resolve to a missing object instead of falling
// back.
func TestReclaimIndex_RewritesStraddlingBucket(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newEngine(t, store)
	defer engine.Close()

	bucketKey := "orders/0/00000000000000000000-00000000000000000300.idx"
	refs := []SegmentRef{{Base: 0, Epoch: 1}, {Base: 100, Epoch: 1}, {Base: 200, Epoch: 1}, {Base: 300, Epoch: 1}}
	putBucket(t, store, bucketKey, refs)
	engine.metadataCache.SeededBucket("orders", 0, BucketRef{First: 0, Last: 300, Key: bucketKey})
	for _, r := range refs {
		_ = store.inner.Put(context.Background(), fmt.Sprintf("orders/0/%s", wal.SegmentName(r.Base, r.Epoch)), strings.NewReader("x"))
	}

	deleted := engine.reclaimIndex(context.Background(), "orders", 0, 250)

	// 0 and 100 are provably consumed (their successors start below 250); 200's
	// successor starts at 300, above the bound, so it is kept.
	if deleted != 2 {
		t.Errorf("deleted %d, want the 2 provably consumed segments", deleted)
	}

	ps := engine.metadataCache.Topics["orders"].Partitions[0]
	if len(ps.Sealed) != 1 || ps.Sealed[0].Key != bucketKey {
		t.Fatalf("the straddling bucket was dropped instead of rewritten: %v", ps.Sealed)
	}
	rc, err := store.inner.Get(context.Background(), bucketKey)
	if err != nil {
		t.Fatalf("rewritten bucket is missing: %v", err)
	}
	defer func() { _ = rc.Close() }()
	raw, _ := io.ReadAll(rc)
	var kept []SegmentRef
	if err := json.Unmarshal(raw, &kept); err != nil {
		t.Fatalf("rewritten bucket is not readable: %v", err)
	}
	if len(kept) != 2 || kept[0].Base != 200 || kept[1].Base != 300 {
		t.Errorf("bucket now holds %v, want the segments from base 200 up", kept)
	}
}

// putBucket writes a sealed bucket's body where reclaim will read it back from.
func putBucket(t *testing.T, store *listFailStore, key string, refs []SegmentRef) {
	t.Helper()
	body, err := json.Marshal(refs)
	if err != nil {
		t.Fatalf("marshal bucket: %v", err)
	}
	if err := store.inner.Put(context.Background(), key, bytes.NewReader(body)); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
}

// Reclaiming must never delete the segment that contains the boundary offset.
// A consumer resuming at exactly the boundary still has to read from it, and
// the entry whose base sits below the boundary can be the one spanning it --
// a trim keyed purely on "base >= start" deletes it and the resume finds
// nothing.
//
// This is the live shape from knode3: bases 777763, 777764, [777765], 777766,
// with the committed offset landing between two entries.
func TestReclaimIndex_KeepsSegmentSpanningTheBoundary(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newEngine(t, store)
	defer engine.Close()

	bases := []int64{777763, 777764, 777766, 777767}
	for _, b := range bases {
		engine.metadataCache.AddSegment("orders", 0, SegmentRef{Base: b, Epoch: 1})
		if err := store.inner.Put(context.Background(),
			fmt.Sprintf("orders/0/%s", wal.SegmentName(b, 1)), strings.NewReader("x")); err != nil {
			t.Fatalf("seed segment %d: %v", b, err)
		}
	}

	const boundary = 777765
	deleted := engine.reclaimIndex(context.Background(), "orders", 0, boundary)

	// Only 777763 is provably consumed: 777764 spans the boundary, so it has to
	// stay for a consumer resuming at 777765.
	if deleted != 1 {
		t.Errorf("deleted %d segment(s), want 1 (777763 only)", deleted)
	}

	key, err := engine.segmentKeyFor(context.Background(), "orders", 0, boundary)
	if err != nil {
		t.Fatalf("a consumer resuming at the boundary could not resolve a segment: %v", err)
	}
	if want := fmt.Sprintf("orders/0/%s", wal.SegmentName(777764, 1)); key != want {
		t.Errorf("offset %d resolved to %q, want %q -- the segment spanning the boundary was reclaimed", boundary, key, want)
	}
}

// Retention configured but not yet exceeded must delete nothing.
//
// The consumer bound alone used to be the trigger, which meant switching
// retention on deleted everything the consumer had passed -- on knode3 that is
// ~11,000 objects on the first sweep, because the consumer sits near the log
// end. A healthy log inside its budget is the normal state and must be left
// alone.
func TestReclaimByIndex_InsideBudgetDeletesNothing(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{
		RetentionBytes: 1 << 30, // 1 GiB
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	engine.committedMu.Lock()
	engine.committedAuthoritative = true
	engine.committedMu.Unlock()

	now := time.Now().Unix()
	for _, b := range []int64{0, 100, 200, 300} {
		engine.metadataCache.AddSegment("orders", 0, SegmentRef{Base: b, Size: 1000, Modified: now})
		if err := store.inner.Put(context.Background(),
			fmt.Sprintf("orders/0/%s", wal.SegmentName(b, 0)), strings.NewReader("x")); err != nil {
			t.Fatalf("seed segment %d: %v", b, err)
		}
	}

	// The consumer is caught up, so every segment is below the bound.
	engine.SaveOffset("mimir-trial-ingesters", "orders", 0, 300)
	engine.applyRetention()

	if got := engine.reclaimByIndex(context.Background(), "orders", 0); got != 0 {
		t.Errorf("reclaimed %d segment(s) while inside the byte budget; want 0", got)
	}
	ps := engine.metadataCache.Topics["orders"].Partitions[0]
	if len(ps.Recent) != 4 {
		t.Errorf("window holds %d entries, want all 4 kept", len(ps.Recent))
	}
}

// An age policy that has not elapsed must also delete nothing, and one that has
// must delete only what is genuinely old.
func TestReclaimByIndex_AgePolicy(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{
		RetentionTime: time.Hour,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	engine.committedMu.Lock()
	engine.committedAuthoritative = true
	engine.committedMu.Unlock()

	now := time.Now()
	// Segments are written in offset order, so age increases with base offset:
	// the old ones are a prefix. An out-of-order timestamp cannot occur, and the
	// scan relies on that -- it stops at the first segment inside the limit, so a
	// re-uploaded segment with a newer time holds back everything after it.
	// Erring that way keeps data rather than deleting it.
	engine.metadataCache.AddSegment("orders", 0, SegmentRef{Base: 300, Size: 10, Modified: now.Unix()})
	engine.metadataCache.AddSegment("orders", 0, SegmentRef{Base: 200, Size: 10, Modified: now.Add(-2 * time.Hour).Unix()})
	engine.metadataCache.AddSegment("orders", 0, SegmentRef{Base: 100, Size: 10, Modified: now.Add(-3 * time.Hour).Unix()})

	if got := engine.policyBoundary("orders", 0); got != 200 {
		t.Errorf("policy boundary = %d, want 200 (the last segment past the age limit)", got)
	}
}
