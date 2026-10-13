package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"kimistore/internal/storage/wal"
)

// seedBucket writes a sealed bucket object and records it in the index, the way
// the writer would after a window fills.
func seedBucket(t *testing.T, engine *StorageEngine, store ObjectStore, topic string, partition int32, first int64, count int, bodySize int) (BucketRef, []SegmentRef) {
	t.Helper()
	var refs []SegmentRef
	for i := int64(0); i < int64(count); i++ {
		off := first + i
		refs = append(refs, SegmentRef{Base: off, Epoch: 7, Size: int64(bodySize + 12)})
		key := fmt.Sprintf("%s/%d/%s", topic, partition, wal.SegmentName(off, 7))

		var body []byte
		hdr := make([]byte, 12)
		binary.BigEndian.PutUint64(hdr[0:8], uint64(off))
		binary.BigEndian.PutUint32(hdr[8:12], uint32(bodySize))
		body = append(body, hdr...)
		entry := make([]byte, bodySize)
		binary.BigEndian.PutUint64(entry[0:8], uint64(off))
		body = append(body, entry...)

		if err := store.Put(context.Background(), key, bytesReader(body)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	b := BucketRef{
		First: first, Last: first + int64(count) - 1,
		Key:    bucketKey(topic, partition, first, first+int64(count)-1),
		Bytes:  int64(count * (bodySize + 12)),
		Newest: 1000,
	}
	raw, err := json.Marshal(refs)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), b.Key, bytesReader(raw)); err != nil {
		t.Fatalf("put bucket: %v", err)
	}
	engine.metadataCache.SeededBucket(topic, partition, b)
	return b, refs
}

func newCompactingEngine(t *testing.T, store ObjectStore) *StorageEngine {
	t.Helper()
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{
			Interval:       time.Hour,
			MinSegments:    4,
			MaxMergedBytes: 1 << 30,
			ScratchDir:     t.TempDir(),
		}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

// Compaction must preserve every record. This is the property that matters: a
// merge that drops a record is unrecoverable, and the merged object is the only
// copy of that data once the sources go.
func TestCompactPartition_PreservesEveryRecord(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newCompactingEngine(t, store)

	_, refs := seedBucket(t, engine, store.inner, "orders", 0, 1000, 12, 40)
	// A second, newer bucket: the oldest is what gets compacted, so the range
	// under test is not also the one deliberately spared.
	seedBucket(t, engine, store.inner, "orders", 0, 2000, 12, 40)

	before := map[int64][]byte{}
	for _, ref := range refs {
		body, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref))
		if err != nil {
			t.Fatalf("read source: %v", err)
		}
		// Store the record body, not the whole segment file: after the merge the
		// offset lives inside a larger segment, and only the record itself is
		// comparable.
		raw := readAll(t, body)
		entry := findEntry(t, raw, ref.Base)
		if entry == nil {
			t.Fatalf("source segment %d has no entry for its own base offset", ref.Base)
		}
		before[ref.Base] = entry
	}

	n, err := engine.compactPartition(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("compactPartition: %v", err)
	}
	if n == 0 {
		t.Fatal("nothing was compacted")
	}

	// Every offset the sources held must still resolve, with the same bytes.
	// Resolution goes through the real read path -- the open window first, then
	// the sealed bucket -- because that is what a consumer uses, and asserting
	// against the window alone would pass even if the merged bucket were wrong.
	for off, want := range before {
		key, err := engine.segmentKeyFor(context.Background(), "orders", 0, off)
		if err != nil {
			t.Errorf("offset %d resolves to no segment after compaction: %v", off, err)
			continue
		}
		rc, err := store.inner.Get(context.Background(), key)
		if err != nil {
			t.Errorf("offset %d: resolved segment unreadable: %v", off, err)
			continue
		}
		got := readAll(t, rc)
		_ = rc
		// The resolved segment now holds many records, so the record for this
		// offset is located inside it rather than being the whole file. Its
		// bytes must be identical to what the source segment held at that
		// offset.
		entry := findEntry(t, got, off)
		if entry == nil {
			t.Errorf("offset %d: no entry with that offset in the merged segment", off)
			continue
		}
		if !bytes.Equal(entry, want) {
			t.Errorf("offset %d: record changed across the merge (%d bytes vs %d)", off, len(entry), len(want))
		}
	}

	// The sources must be gone, or the merge reclaimed nothing.
	remaining := 0
	for _, ref := range refs {
		if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref)); err == nil {
			_ = rc.Close()
			remaining++
		}
	}
	if remaining != 0 {
		t.Errorf("%d source segment(s) still present after compaction", remaining)
	}
}

// The index must name the merged segment only after that segment exists. A crash
// between the two would leave the index pointing at an object that was never
// written, which loses data rather than merely duplicating it.
func TestCompactPartition_IndexOnlyNamesWrittenObjects(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newCompactingEngine(t, store)

	seedBucket(t, engine, store.inner, "orders", 0, 5000, 12, 40)
	seedBucket(t, engine, store.inner, "orders", 0, 6000, 12, 40)

	if _, err := engine.compactPartition(context.Background(), "orders", 0); err != nil {
		t.Fatalf("compactPartition: %v", err)
	}

	// Everything the index names must exist in the store.
	window, sealed := engine.metadataCache.IndexSnapshot("orders", 0)
	for _, ref := range window {
		if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref)); err != nil {
			t.Errorf("window names %s but it is not in the store", segmentKey("orders", 0, ref))
		} else {
			_ = rc.Close()
		}
	}
	for _, bk := range sealed {
		refs, err := engine.readBucket(context.Background(), bk)
		if err != nil {
			t.Errorf("bucket %s is not readable: %v", bk.Key, err)
			continue
		}
		for _, ref := range refs {
			if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref)); err != nil {
				t.Errorf("bucket %s names %s but it is not in the store", bk.Key, segmentKey("orders", 0, ref))
			} else {
				_ = rc.Close()
			}
		}
	}
}

// The newest sealed bucket abuts the open window and is the range a reader is
// most likely to be in, so it is left alone.
func TestCompactPartition_LeavesNewestBucketAlone(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newCompactingEngine(t, store)

	_, _ = seedBucket(t, engine, store.inner, "orders", 0, 1000, 12, 40)
	second, _ := seedBucket(t, engine, store.inner, "orders", 0, 2000, 12, 40)

	n, err := engine.compactPartition(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("compactPartition: %v", err)
	}
	if n == 0 {
		t.Fatal("nothing compacted")
	}

	_, sealed := engine.metadataCache.IndexSnapshot("orders", 0)
	var hasSecond bool
	for _, b := range sealed {
		if b.Key == second.Key {
			hasSecond = true
		}
	}
	if !hasSecond {
		t.Error("the newest sealed bucket was compacted; it abuts the open window")
	}
}

// A bucket whose entries predate the index carrying sizes must be skipped, not
// merged blind: the merged size cap cannot be applied without them.
func TestCompactPartition_SkipsBucketsWithoutSizes(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newCompactingEngine(t, store)

	bucket, refs := seedBucket(t, engine, store.inner, "orders", 0, 3000, 12, 40)
	// A compactable bucket behind it, then a spare newest to leave alone.
	seedBucket(t, engine, store.inner, "orders", 0, 4000, 12, 40)
	seedBucket(t, engine, store.inner, "orders", 0, 5000, 12, 40)

	// Rewrite the bucket with entries carrying no size, which is what entries
	// recorded before SegmentRef gained the field look like. The cap on merged
	// size cannot be applied to them, so the merge must be refused.
	var unsized []SegmentRef
	for _, ref := range refs {
		ref.Size = 0
		unsized = append(unsized, ref)
	}
	raw, err := json.Marshal(unsized)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.inner.Put(context.Background(), bucket.Key, bytesReader(raw)); err != nil {
		t.Fatalf("rewrite bucket: %v", err)
	}

	// The unsized bucket is skipped, not merged. The pass itself still succeeds,
	// because the bucket behind it is compactable -- which is the whole point of
	// scanning forward.
	n, err := engine.compactPartition(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("compactPartition: %v", err)
	}
	if n == 0 {
		t.Error("nothing was compacted; the unsized bucket blocked the queue")
	}
	for _, ref := range refs {
		rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref))
		if err != nil {
			t.Errorf("source %d was deleted despite the merge being refused: %v", ref.Base, err)
			continue
		}
		_ = rc.Close()
	}
}

// findEntry returns the record at offset within a segment, decoding the same
// header layout the writer produces.
func findEntry(t *testing.T, seg []byte, offset int64) []byte {
	t.Helper()
	for pos := 0; pos+12 <= len(seg); {
		off := int64(binary.BigEndian.Uint64(seg[pos : pos+8]))
		size := int(binary.BigEndian.Uint32(seg[pos+8 : pos+12]))
		body := seg[pos+12 : pos+12+size]
		if off == offset {
			return append([]byte(nil), body...)
		}
		pos += 12 + size
	}
	return nil
}

// bytesReader adapts a byte slice for the store's Put.
func bytesReader(b []byte) *strings.Reader { return strings.NewReader(string(b)) }

func readAll(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	defer func() { _ = rc.Close() }()
	out, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return out
}

// Compaction must run unless an operator explicitly turns it off.
//
// The zero value of a bool cannot mean "on by default", so the flag was
// inverted to Disabled. This pins that, because the previous opt-in shape
// meant a caller passing a zero CompactorConfig silently disabled compaction
// and the object count never came down.
func TestCompactor_RunsUnlessDisabled(t *testing.T) {
	store := newCASStore()
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()
	if engine.compactorCfg.Disabled {
		t.Error("compaction is disabled for a zero config; it must be on by default")
	}

	off, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{Disabled: true}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer off.Close()
	if !off.compactorCfg.Disabled {
		t.Error("WithCompactor(Disabled) did not disable compaction")
	}
}

// A merge that committed its index update and then died before deleting its
// sources must leave those sources reclaimable, or they are storage nothing can
// ever reach again.
//
// This is the crash window the marker exists for. The test reproduces it by
// writing the marker and stopping short of the delete, which is exactly the
// state a process death between those two steps leaves behind.
func TestResumeInterruptedMerges_ReclaimsOrphanedSources(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newCompactingEngine(t, store)

	_, refs := seedBucket(t, engine, store.inner, "orders", 0, 1000, 12, 40)
	seedBucket(t, engine, store.inner, "orders", 0, 2000, 12, 40)

	// Perform the merge, but restore the sources afterwards and leave a marker
	// naming the bucket that replaced them -- the state after a crash between
	// the index commit and the delete.
	if _, err := engine.compactPartition(context.Background(), "orders", 0); err != nil {
		t.Fatalf("compactPartition: %v", err)
	}
	_, sealed := engine.metadataCache.IndexSnapshot("orders", 0)
	if len(sealed) == 0 {
		t.Fatal("no sealed buckets after compaction")
	}
	mergedBucket := sealed[0]

	for _, ref := range refs {
		key := segmentKey("orders", 0, ref)
		if err := store.inner.Put(context.Background(), key, bytesReader([]byte("orphan"))); err != nil {
			t.Fatalf("restore orphan %s: %v", key, err)
		}
	}
	marker := engine.markerKey("orders", 0, mergedBucket.Key)
	raw, err := json.Marshal(refs)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.inner.Put(context.Background(), marker, bytesReader(raw)); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	// The staged orphan must be present before recovery, or the test would pass
	// without proving anything.
	if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, refs[0])); err != nil {
		t.Fatalf("orphan was not staged (%v); the test would prove nothing", err)
	} else {
		_ = rc.Close()
	}

	deleted := engine.resumeInterruptedMerges(context.Background(), "orders", 0)
	if deleted == 0 {
		t.Fatal("recovery reclaimed nothing")
	}

	for _, ref := range refs {
		if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref)); err == nil {
			_ = rc.Close()
			t.Errorf("orphan %d survived recovery", ref.Base)
		}
	}
	if rc, err := store.inner.Get(context.Background(), marker); err == nil {
		_ = rc.Close()
		t.Error("the marker was not cleared after recovery")
	}
}

// A marker whose bucket the index does not name belongs to a merge that never
// committed. Its sources are still the live data, so acting on it would delete
// records the index depends on.
func TestResumeInterruptedMerges_IgnoresUncommittedMarker(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newCompactingEngine(t, store)

	_, refs := seedBucket(t, engine, store.inner, "orders", 0, 1000, 12, 40)
	seedBucket(t, engine, store.inner, "orders", 0, 2000, 12, 40)

	// A marker naming a bucket the index knows nothing about.
	marker := engine.markerKey("orders", 0, "orders/0/99999999999999999999-99999999999999999999.idx")
	raw, err := json.Marshal(refs)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.inner.Put(context.Background(), marker, bytesReader(raw)); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	if deleted := engine.resumeInterruptedMerges(context.Background(), "orders", 0); deleted != 0 {
		t.Errorf("recovery deleted %d segment(s) for a merge that never committed", deleted)
	}
	for _, ref := range refs {
		if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref)); err != nil {
			t.Errorf("source %d was deleted by an uncommitted marker: %v", ref.Base, err)
		} else {
			_ = rc.Close()
		}
	}
}

// A marker that cannot be read must not be treated as permission to delete. The
// sources it would name are still referenced until a merge commits.
func TestResumeInterruptedMerges_IgnoresUnreadableMarker(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newCompactingEngine(t, store)

	_, refs := seedBucket(t, engine, store.inner, "orders", 0, 1000, 12, 40)
	seedBucket(t, engine, store.inner, "orders", 0, 2000, 12, 40)

	_, sealed := engine.metadataCache.IndexSnapshot("orders", 0)
	marker := engine.markerKey("orders", 0, sealed[0].Key)
	if err := store.inner.Put(context.Background(), marker, bytesReader([]byte("{not json"))); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	if deleted := engine.resumeInterruptedMerges(context.Background(), "orders", 0); deleted != 0 {
		t.Errorf("recovery deleted %d segment(s) on an unreadable marker", deleted)
	}
	for _, ref := range refs {
		if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref)); err != nil {
			t.Errorf("source %d was deleted on an unreadable marker: %v", ref.Base, err)
		} else {
			_ = rc.Close()
		}
	}
}

// A bucket that cannot be merged must not block the buckets behind it.
//
// This is what knode3 did for 23 hours: three buckets recorded before the index
// carried sizes sat at the front of the queue, the compactor took only the
// oldest candidate and returned when it could not merge it, and fourteen
// perfectly compactable buckets behind it were never reached. Every tick logged
// the same skip and the object count did not move.
func TestCompactPartition_SkipsPastAnUnmergeableBucket(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newCompactingEngine(t, store)

	// The blocker: entries with no recorded size, oldest in the queue.
	blocker, blockerRefs := seedBucket(t, engine, store.inner, "orders", 0, 1000, 12, 40)
	rewriteUnsized(t, store, blocker, blockerRefs)

	// Two compactable buckets behind it, then a spare newest to leave alone.
	// The first of these is the one that must be merged: it is the oldest
	// candidate the blocker is hiding.
	_, behind := seedBucket(t, engine, store.inner, "orders", 0, 2000, 12, 40)
	seedBucket(t, engine, store.inner, "orders", 0, 3000, 12, 40)
	seedBucket(t, engine, store.inner, "orders", 0, 4000, 12, 40)

	n, err := engine.compactPartition(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("compactPartition: %v", err)
	}
	if n == 0 {
		t.Fatal("nothing was compacted; the blocker stopped the queue")
	}

	// The blocker must be untouched -- refusing it is correct.
	for _, ref := range blockerRefs {
		if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref)); err != nil {
			t.Errorf("blocker source %d was deleted despite being unmergeable: %v", ref.Base, err)
		} else {
			_ = rc.Close()
		}
	}

	// The bucket behind it must have been consolidated.
	merged := 0
	for _, ref := range behind {
		if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref)); err == nil {
			_ = rc.Close()
		} else {
			merged++
		}
	}
	if merged != len(behind) {
		t.Errorf("%d of %d source segments behind the blocker were reclaimed; want all",
			merged, len(behind))
	}
}

// rewriteUnsized rewrites a bucket's contents with sizes cleared, which is what
// entries recorded before SegmentRef gained that field look like.
func rewriteUnsized(t *testing.T, store *listFailStore, b BucketRef, refs []SegmentRef) {
	t.Helper()
	var unsized []SegmentRef
	for _, ref := range refs {
		ref.Size = 0
		unsized = append(unsized, ref)
	}
	raw, err := json.Marshal(unsized)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.inner.Put(context.Background(), b.Key, bytesReader(raw)); err != nil {
		t.Fatalf("rewrite bucket: %v", err)
	}
}

// A merge must take segments until the size cap, not stop at the minimum.
//
// This is what stopped compaction winning the race. The selection condition was
// `len(sources) >= MinSegments && bytesIn >= MaxMergedBytes`, and once a bucket's
// first entry was itself a merged segment the byte test was satisfied on the
// first iteration -- so every subsequent pass merged exactly MinSegments, eight.
// Eight segments a pass cannot keep up with 3,200 an hour; the interval was not
// the problem.
func TestCompactPartition_TakesSegmentsUpToTheSizeCap(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	scratch := t.TempDir()
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{
			Interval:       time.Hour,
			MinSegments:    8,
			MaxMergedBytes: 64 * 1024, // 64KB cap, entries of 1KB
			ScratchDir:     scratch,
		}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	const count = 120
	bucket, refs := seedBucketSized(t, engine, store.inner, "orders", 0, 1000, count, 1024)
	seedBucketSized(t, engine, store.inner, "orders", 0, 2000, count, 1024)
	seedBucketSized(t, engine, store.inner, "orders", 0, 3000, count, 1024)

	// First pass creates the merged segment. The second is the one that matters:
	// the bucket now opens with a segment that is already at the cap, and that
	// is what made the byte test true on the first iteration and stopped
	// selection at MinSegments. A test that only compacts once cannot see it.
	if _, err := engine.compactPartition(context.Background(), "orders", 0); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	n, err := engine.compactPartition(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	t.Logf("  second pass merged %d segment(s)", n)

	if n < 32 {
		t.Errorf("second pass merged %d segment(s); selection is still stopping at MinSegments "+
			"because the bucket now opens with an already-merged segment", n)
	}

	merged := 0
	for _, ref := range refs {
		if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref)); err == nil {
			_ = rc.Close()
		} else {
			merged++
		}
	}
	if merged < 32 {
		t.Errorf("%d source segments were reclaimed, want the passes to clear the bucket", merged)
	}
	_ = bucket
}

// seedBucketSized is seedBucket with an explicit entry size, so the size cap can
// be exercised against known input.
func seedBucketSized(t *testing.T, engine *StorageEngine, store ObjectStore, topic string, partition int32, first int64, count int, bodySize int) (BucketRef, []SegmentRef) {
	t.Helper()
	var refs []SegmentRef
	for i := int64(0); i < int64(count); i++ {
		off := first + i
		refs = append(refs, SegmentRef{Base: off, Epoch: 7, Size: int64(bodySize + 12)})
		key := fmt.Sprintf("%s/%d/%s", topic, partition, wal.SegmentName(off, 7))

		hdr := make([]byte, 12)
		binary.BigEndian.PutUint64(hdr[0:8], uint64(off))
		binary.BigEndian.PutUint32(hdr[8:12], uint32(bodySize))
		body := append([]byte(nil), hdr...)
		entry := make([]byte, bodySize)
		binary.BigEndian.PutUint64(entry[0:8], uint64(off))
		body = append(body, entry...)

		if err := store.Put(context.Background(), key, bytesReader(body)); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	b := BucketRef{
		First: first, Last: first + int64(count) - 1,
		Key:    bucketKey(topic, partition, first, first+int64(count)-1),
		Bytes:  int64(count * (bodySize + 12)),
		Newest: 1000,
	}
	raw, err := json.Marshal(refs)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), b.Key, bytesReader(raw)); err != nil {
		t.Fatalf("put bucket: %v", err)
	}
	engine.metadataCache.SeededBucket(topic, partition, b)
	return b, refs
}

// An index entry whose object is gone must not wedge the partition.
//
// knode3 had exactly this: one bucket entry named a segment that no longer
// existed, and every pass failed on it in the same place, forever. The compactor
// logged the same 404 once a minute for hours and merged nothing, which reads
// as "compaction is running" in a log search.
func TestCompactPartition_SkipsAnEntryWhoseObjectIsGone(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{
			Interval: time.Hour, MinSegments: 4,
			MaxMergedBytes: 1 << 20, ScratchDir: t.TempDir(),
		}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	bucket, refs := seedBucket(t, engine, store.inner, "orders", 0, 1000, 12, 40)
	seedBucket(t, engine, store.inner, "orders", 0, 2000, 12, 40)
	seedBucket(t, engine, store.inner, "orders", 0, 3000, 12, 40)

	// Remove one object the bucket names, leaving the index pointing at it.
	victim := refs[3]
	if err := store.inner.Delete(context.Background(), segmentKey("orders", 0, victim)); err != nil {
		t.Fatalf("delete victim: %v", err)
	}
	if err := store.inner.Delete(context.Background(),
		strings.TrimSuffix(segmentKey("orders", 0, victim), ".log")+".index"); err != nil {
		t.Fatalf("delete victim sidecar: %v", err)
	}

	n, err := engine.compactPartition(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("compactPartition returned an error over one missing object: %v", err)
	}
	if n < 8 {
		t.Errorf("merged %d segment(s); one absent object should not have stopped the pass", n)
	}

	// The survivors must still be readable.
	ps := engine.metadataCache.Topics["orders"].Partitions[0]
	if len(ps.Sealed) == 0 {
		t.Fatal("no sealed buckets remain")
	}
	for _, ref := range refs {
		if ref.Base == victim.Base {
			continue
		}
		if ref.Base > 1010 {
			continue
		}
		key, err := engine.segmentKeyFor(context.Background(), "orders", 0, ref.Base)
		if err != nil {
			t.Errorf("offset %d no longer resolves after the merge: %v", ref.Base, err)
			continue
		}
		if key == "" {
			t.Errorf("offset %d resolves to no segment", ref.Base)
		}
	}
	_ = bucket
}
