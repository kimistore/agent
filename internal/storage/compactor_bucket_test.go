package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"kimistore/internal/storage/wal"
)

// seedBucketWithLeadingMerge lays down a bucket whose first entry is already a
// merged segment, with the small segments behind it.
//
// That shape is what a partially compacted bucket actually looks like on the
// node, and it is the only shape that exercises the selection path which skips
// an entry. A bucket of uniformly small segments never skips, so a test built
// from those passes against the defect.
func seedBucketWithLeadingMerge(t *testing.T, engine *StorageEngine, store ObjectStore, topic string, partition int32, first int64, small, size int, mergedBytes int64) (BucketRef, []SegmentRef) {
	t.Helper()
	var refs []SegmentRef

	// The head: one segment already at the cap, from an earlier pass.
	head := SegmentRef{Base: first, Epoch: 9, Size: mergedBytes}
	writeSeg(t, store, topic, partition, head, size)
	refs = append(refs, head)

	for i := 1; i <= small; i++ {
		off := first + int64(i)*10
		ref := SegmentRef{Base: off, Epoch: 7, Size: int64(size + 12)}
		writeSeg(t, store, topic, partition, ref, size)
		refs = append(refs, ref)
	}

	b := BucketRef{
		First:  refs[0].Base,
		Last:   refs[len(refs)-1].Base,
		Key:    bucketKey(topic, partition, refs[0].Base, refs[len(refs)-1].Base),
		Bytes:  sumSizes(refs),
		Newest: 1000,
	}
	raw, err := json.Marshal(refs)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), b.Key, bytesReader(raw)); err != nil {
		t.Fatal(err)
	}
	engine.metadataCache.SeededBucket(topic, partition, b)
	return b, refs
}

func writeSeg(t *testing.T, store ObjectStore, topic string, partition int32, ref SegmentRef, bodySize int) {
	t.Helper()
	key := fmt.Sprintf("%s/%d/%s", topic, partition, wal.SegmentName(ref.Base, ref.Epoch))
	hdr := make([]byte, 12)
	putU64(hdr[0:8], uint64(ref.Base))
	putU32(hdr[8:12], uint32(bodySize))
	body := append([]byte(nil), hdr...)
	entry := make([]byte, bodySize)
	putU64(entry[0:8], uint64(ref.Base))
	body = append(body, entry...)
	if err := store.Put(context.Background(), key, bytesReader(body)); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func sumSizes(refs []SegmentRef) int64 {
	var t int64
	for _, r := range refs {
		t += r.Size
	}
	return t
}

// A merge must not make the index name a segment that was deleted, and must not
// drop a segment that was not merged.
//
// This is the defect: selection skips an entry that is already a merged segment,
// which makes the chosen sources a non-contiguous subset of the bucket. The
// rewritten bucket was then derived by position -- refs[len(sources):] -- rather
// than by membership, so it listed entries that had been merged and deleted
// while dropping entries that had been left alone.
//
// On knode3 that destroyed seven offsets, each the tail of a compacted bucket.
func TestCompactPartition_RewrittenBucketIsConsistentWithItsObjects(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{
			Interval: time.Hour, MinSegments: 4,
			MaxMergedBytes: 32 * 1024, ScratchDir: t.TempDir(),
		}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	// Head entry is 24KB against a 32KB cap, so it is stepped over as a previous
	// pass's product and the sources become non-contiguous.
	bucket, refs := seedBucketWithLeadingMerge(t, engine, store.inner, "orders", 0,
		1000, 24, 1024, 24*1024)
	seedBucket(t, engine, store.inner, "orders", 0, 2000, 12, 40)
	seedBucket(t, engine, store.inner, "orders", 0, 3000, 12, 40)

	if _, err := engine.compactPartition(context.Background(), "orders", 0); err != nil {
		t.Fatalf("compactPartition: %v", err)
	}

	// The rewritten bucket must name only segments that exist. It is read back
	// from the index rather than by its original key, because a merge rewrites
	// the bucket under a new key when its range changes.
	raw := readBucketObject(t, store.inner, currentBucketKey(t, engine, 0))
	var after []SegmentRef
	if err := json.Unmarshal(raw, &after); err != nil {
		t.Fatalf("rewritten bucket unreadable: %v", err)
	}
	for _, ref := range after {
		if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref)); err != nil {
			t.Errorf("index names %s but the object is gone -- a merged segment was "+
				"listed in the rewritten bucket and then deleted", segmentKey("orders", 0, ref))
		} else {
			_ = rc.Close()
		}
	}

	// Nothing that was not merged may have lost its index entry.
	kept := map[int64]bool{}
	for _, r := range after {
		kept[r.Base] = true
	}
	// The head segment was stepped over, so it is not merged and must remain
	// named.
	if !kept[refs[0].Base] {
		t.Errorf("segment at %d was not merged but lost its index entry", refs[0].Base)
	}
	// Every offset must still resolve through the real read path.
	for _, ref := range refs {
		if _, err := engine.segmentKeyFor(context.Background(), "orders", 0, ref.Base); err != nil {
			t.Errorf("offset %d no longer resolves: %v", ref.Base, err)
		}
	}

	_ = bucket
	// And the bucket must stay sorted, because BucketFor binary-searches it.
	if !sort.SliceIsSorted(after, func(i, j int) bool { return after[i].Base < after[j].Base }) {
		t.Error("rewritten bucket is not sorted by base offset; BucketFor would miss entries")
	}
	if after[0].Base != refs[0].Base {
		t.Errorf("rewritten bucket starts at %d, want %d: it no longer covers its original range",
			after[0].Base, refs[0].Base)
	}
	// The bucket must still describe the same range. That is the BucketRef's
	// Last, not the base of its last entry: a merged segment spans every offset
	// its sources covered, so the last entry can sit well below the end of the
	// range. BucketFor searches on Last, so it is the field that decides whether
	// an offset resolves at all.
	_, sealed := engine.metadataCache.IndexSnapshot("orders", 0)
	var rewritten *BucketRef
	for i := range sealed {
		if sealed[i].Key == currentBucketKey(t, engine, 0) {
			rewritten = &sealed[i]
		}
	}
	if rewritten == nil {
		t.Fatal("rewritten bucket is not in the index")
	}
	if rewritten.First != refs[0].Base || rewritten.Last != refs[len(refs)-1].Base {
		t.Errorf("rewritten bucket spans %d..%d, want %d..%d",
			rewritten.First, rewritten.Last, refs[0].Base, refs[len(refs)-1].Base)
	}
}

// Every offset the bucket covered before a merge must still resolve after it.
// Losing an index entry is as much a failure as naming a deleted object: the
// read finds nothing either way.
func TestCompactPartition_NoOffsetIsLostAcrossRepeatedPasses(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{
			Interval: time.Hour, MinSegments: 4,
			MaxMergedBytes: 32 * 1024, ScratchDir: t.TempDir(),
		}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	_, refs := seedBucketWithLeadingMerge(t, engine, store.inner, "orders", 0,
		1000, 40, 1024, 24*1024)
	seedBucket(t, engine, store.inner, "orders", 0, 2000, 12, 40)
	seedBucket(t, engine, store.inner, "orders", 0, 3000, 12, 40)

	for pass := 1; pass <= 4; pass++ {
		if _, err := engine.compactPartition(context.Background(), "orders", 0); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		raw := readBucketObject(t, store.inner, currentBucketKey(t, engine, 0))
		var after []SegmentRef
		if err := json.Unmarshal(raw, &after); err != nil {
			t.Fatalf("pass %d: bucket unreadable: %v", pass, err)
		}
		seen := map[int64]bool{}
		for _, ref := range after {
			seen[ref.Base] = true
		}
		for _, ref := range refs {
			if seen[ref.Base] {
				continue
			}
			// Only a merged segment may stop being named individually; it is
			// then covered by the merged segment that starts at its own base.
			if _, err := engine.segmentKeyFor(context.Background(), "orders", 0, ref.Base); err != nil {
				t.Errorf("pass %d: offset %d is no longer resolvable: %v", pass, ref.Base, err)
			}
		}
	}
}

// currentBucketKey is the oldest sealed bucket's key as the index now holds it.
func currentBucketKey(t *testing.T, engine *StorageEngine, partition int32) string {
	t.Helper()
	_, sealed := engine.metadataCache.IndexSnapshot("orders", partition)
	if len(sealed) == 0 {
		t.Fatal("no sealed buckets")
	}
	oldest := sealed[0]
	for _, b := range sealed {
		if b.First < oldest.First {
			oldest = b
		}
	}
	return oldest.Key
}

func readBucketObject(t *testing.T, store ObjectStore, key string) []byte {
	t.Helper()
	rc, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("read bucket %s: %v", key, err)
	}
	defer func() { _ = rc.Close() }()
	buf := make([]byte, 0)
	tmp := make([]byte, 32*1024)
	for {
		n, err := rc.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			break
		}
	}
	return buf
}

func putU64(b []byte, v uint64) {
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (56 - 8*i))
	}
}

func putU32(b []byte, v uint32) {
	for i := 0; i < 4; i++ {
		b[i] = byte(v >> (24 - 8*i))
	}
}
