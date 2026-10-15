package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"kimistore/internal/metrics"
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

// seedAllMerged lays down a bucket whose every entry is an already-merged
// product: each is at or above half the cap, so selectSources skips them all.
func seedAllMerged(t *testing.T, engine *StorageEngine, store ObjectStore, topic string, partition int32, first int64, count int, size int64) BucketRef {
	t.Helper()
	var refs []SegmentRef
	for i := int64(0); i < int64(count); i++ {
		ref := SegmentRef{Base: first + i*10, Epoch: 9, Size: size}
		writeSeg(t, store, topic, partition, ref, 1024)
		refs = append(refs, ref)
	}
	b := BucketRef{
		First: refs[0].Base, Last: refs[len(refs)-1].Base,
		Key:   bucketKey(topic, partition, refs[0].Base, refs[len(refs)-1].Base),
		Bytes: sumSizes(refs), Newest: 1000,
	}
	raw, err := json.Marshal(refs)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), b.Key, bytesReader(raw)); err != nil {
		t.Fatal(err)
	}
	engine.metadataCache.SeededBucket(topic, partition, b)
	return b
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

// A bucket that is already fully merged must not stop the queue.
//
// This is what knode3 did for eight hours: compaction logged nothing at all
// while the object count rose by 48,000. Once the backlog was compacted the
// oldest bucket held nothing but merged products, every one of them at least
// half the merged-size cap, so the selection skipped every entry and the pass had
// nothing to do. Candidate selection accepted that bucket anyway, because it only
// asked whether the entries carried sizes -- never whether a pass would actually
// pick any -- so it won the queue and returned zero every minute.
func TestCompactPartition_FullyMergedBucketDoesNotBlockTheQueue(t *testing.T) {
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

	// Oldest bucket: nothing but merged products, every entry at or above half
	// the 32KB cap, so the selection has nothing it can take.
	seedAllMerged(t, engine, store.inner, "orders", 0, 1000, 12, 24*1024)

	// Behind it: a bucket of ordinary small segments that must be merged.
	_, behind := seedBucketSized(t, engine, store.inner, "orders", 0, 2000, 120, 1024)
	seedBucketSized(t, engine, store.inner, "orders", 0, 3000, 120, 1024)

	n, err := engine.compactPartition(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("compactPartition: %v", err)
	}
	if n == 0 {
		t.Fatal("nothing was compacted; a fully merged oldest bucket blocked the queue")
	}

	merged := 0
	for _, ref := range behind {
		if rc, err := store.inner.Get(context.Background(), segmentKey("orders", 0, ref)); err == nil {
			_ = rc.Close()
		} else {
			merged++
		}
	}
	// A 32KB cap over ~1KB entries takes about 31 segments, so anything near
	// that proves the pass reached past the blocking bucket.
	if merged < 20 {
		t.Errorf("%d of %d segments behind the fully merged bucket were reclaimed, want the pass to reach them",
			merged, len(behind))
	}
}

// A compactor that has stopped must be distinguishable from one with nothing to
// do.
//
// This is the failure the metric exists for. Compaction went silent for eight
// hours on knode3 -- no log line, no error, return value ignored -- while the
// object count rose by 48,000. "No work available" and "not running" looked
// identical from outside, and the only reason it was eventually caught was
// noticing the object count had gone the wrong way.
func TestCompactor_ReportsLivenessThroughMetrics(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{
			Interval: time.Hour, MinSegments: 4,
			MaxMergedBytes: 32 * 1024, ScratchDir: t.TempDir(),
			MaxPassDuration: time.Minute,
		}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	if got := testutil.ToFloat64(metrics.CompactorLastMergeTimestamp); got != 0 {
		t.Errorf("last-merge timestamp is %v before anything has merged; want 0 so "+
			"'never merged' is distinguishable from 'merged long ago'", got)
	}
	passesBefore := testutil.ToFloat64(metrics.CompactorPasses)
	mergesBefore := testutil.ToFloat64(metrics.CompactorMerges)
	segmentsBefore := testutil.ToFloat64(metrics.CompactorSegmentsMerged)

	seedBucketSized(t, engine, store.inner, "orders", 0, 1000, 120, 1024)
	seedBucketSized(t, engine, store.inner, "orders", 0, 2000, 120, 1024)
	seedBucketSized(t, engine, store.inner, "orders", 0, 3000, 120, 1024)

	// One pass, which now consolidates repeatedly until the queue is empty.
	engine.compactorPass("orders", 0)

	if got := testutil.ToFloat64(metrics.CompactorPasses); got <= passesBefore {
		t.Error("a pass did not increment the pass counter")
	}
	if got := testutil.ToFloat64(metrics.CompactorMerges); got <= mergesBefore {
		t.Error("a pass that merged did not increment the merge counter")
	}
	if got := testutil.ToFloat64(metrics.CompactorSegmentsMerged); got <= segmentsBefore {
		t.Error("a pass that merged did not count the segments it reclaimed")
	}
	if got := testutil.ToFloat64(metrics.CompactorLastMergeTimestamp); got == 0 {
		t.Error("a successful merge did not set the last-merge timestamp")
	}

	// The queue is empty now. A pass that finds nothing must still count as a
	// pass and must not move the merge counters or the last-merge time. That
	// difference is the signal: passes climbing with merges flat means the
	// compactor is running and idle; passes flat means it has stopped.
	passesBefore = testutil.ToFloat64(metrics.CompactorPasses)
	mergesBefore = testutil.ToFloat64(metrics.CompactorMerges)
	segmentsBefore = testutil.ToFloat64(metrics.CompactorSegmentsMerged)
	lastBefore := testutil.ToFloat64(metrics.CompactorLastMergeTimestamp)

	engine.compactorPass("orders", 0)

	if testutil.ToFloat64(metrics.CompactorPasses) <= passesBefore {
		t.Error("an idle pass did not increment the pass counter")
	}
	if testutil.ToFloat64(metrics.CompactorMerges) != mergesBefore {
		t.Error("an idle pass was counted as a merge")
	}
	if testutil.ToFloat64(metrics.CompactorSegmentsMerged) != segmentsBefore {
		t.Error("an idle pass counted segments")
	}
	if testutil.ToFloat64(metrics.CompactorLastMergeTimestamp) != lastBefore {
		t.Error("an idle pass moved the last-merge timestamp")
	}

	// A failing pass is counted as an error, and is not counted as a merge. The
	// distinction matters because a compactor that fails every pass looks alive
	// from the pass counter alone -- the object count never falls and the last
	// merge goes stale, but neither of those is an error counter until the errors
	// are counted somewhere.
	seedBucketSized(t, engine, store.inner, "orders", 0, 4000, 120, 1024)
	store.failGet = true
	errsBefore := testutil.ToFloat64(metrics.CompactorPassErrors)
	mergesBefore = testutil.ToFloat64(metrics.CompactorMerges)
	lastBefore = testutil.ToFloat64(metrics.CompactorLastMergeTimestamp)

	engine.compactorPass("orders", 0)

	if testutil.ToFloat64(metrics.CompactorPassErrors) <= errsBefore {
		t.Error("a failing pass did not increment the error counter")
	}
	if testutil.ToFloat64(metrics.CompactorMerges) != mergesBefore {
		t.Error("a failing pass was counted as a merge")
	}
	if testutil.ToFloat64(metrics.CompactorLastMergeTimestamp) != lastBefore {
		t.Error("a failing pass moved the last-merge timestamp")
	}
}

// A pass consolidates repeatedly, not once.
//
// One merge per tick capped the compactor at one 16MB merge a minute against a
// workload adding a segment a second -- a surplus, but spent so slowly that a
// twelve-bucket backlog took hours. The loop is what turns that into minutes.
func TestCompactorPass_ConsolidatesRepeatedlyUntilTheQueueIsEmpty(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{
			Interval: time.Hour, MinSegments: 4,
			MaxMergedBytes: 32 * 1024, ScratchDir: t.TempDir(),
			MaxPassDuration: time.Minute,
		}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	for _, base := range []int64{1000, 2000, 3000, 4000, 5000} {
		seedBucketSized(t, engine, store.inner, "orders", 0, base, 120, 1024)
	}

	mergesBefore := testutil.ToFloat64(metrics.CompactorMerges)
	segmentsBefore := testutil.ToFloat64(metrics.CompactorSegmentsMerged)

	engine.compactorPass("orders", 0)

	merges := testutil.ToFloat64(metrics.CompactorMerges) - mergesBefore
	segments := testutil.ToFloat64(metrics.CompactorSegmentsMerged) - segmentsBefore

	// Five buckets of 120 one-KB entries against a 32KB cap is about 31 entries
	// per merge, so roughly nineteen merges. One merge per pass would have done
	// one; the loop must do nearly all of them in a single pass.
	if merges < 15 {
		t.Errorf("one pass performed %.0f merges, want it to consolidate most of the backlog in one pass", merges)
	}
	if segments < 400 {
		t.Errorf("one pass reclaimed %.0f of %d seeded segments", segments, 5*120)
	}

	// A second pass has nothing left to do.
	mergesBefore = testutil.ToFloat64(metrics.CompactorMerges)
	engine.compactorPass("orders", 0)
	if got := testutil.ToFloat64(metrics.CompactorMerges) - mergesBefore; got != 0 {
		t.Errorf("a second pass merged %.0f more; the first pass left work behind", got)
	}
}

// The pass stops on its time budget, with the work still queued and the merges
// already done still recorded.
//
// The budget is what stops a backlog from turning into an unbounded burst of I/O.
// A pass that runs out must not discard what it finished, and must say that work
// remains rather than looking identical to one that found the queue empty.
func TestCompactorPass_StopsOnItsBudgetAndKeepsWhatItMerged(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{
			Interval: time.Hour, MinSegments: 4,
			MaxMergedBytes: 32 * 1024, ScratchDir: t.TempDir(),
			// Already spent, so the first merge exhausts it.
			MaxPassDuration: time.Nanosecond,
		}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	for _, base := range []int64{1000, 2000, 3000} {
		seedBucketSized(t, engine, store.inner, "orders", 0, base, 120, 1024)
	}

	mergesBefore := testutil.ToFloat64(metrics.CompactorMerges)
	budgetBefore := testutil.ToFloat64(metrics.CompactorPassBudgetExhausted)

	engine.compactorPass("orders", 0)

	if got := testutil.ToFloat64(metrics.CompactorMerges) - mergesBefore; got != 1 {
		t.Errorf("a spent budget performed %.0f merges, want the one it started", got)
	}
	if got := testutil.ToFloat64(metrics.CompactorPassBudgetExhausted) - budgetBefore; got != 1 {
		t.Errorf("budget-exhausted counter moved by %.0f, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.CompactorLastMergeTimestamp); got == 0 {
		t.Error("the merge the budget allowed was not recorded")
	}

	// The queue is untouched behind it, so the next pass continues where this
	// one stopped.
	mergesBefore = testutil.ToFloat64(metrics.CompactorMerges)
	engine.compactorPass("orders", 0)
	if got := testutil.ToFloat64(metrics.CompactorMerges) - mergesBefore; got != 1 {
		t.Errorf("the next pass merged %.0f, want it to carry on from where the budget stopped", got)
	}
}

// bucketWriteFailStore fails writes of sealed bucket objects while allowing
// segment writes.
//
// This isolates the failure that strands a merged segment: the segment goes up,
// the bucket that would name it does not. Everything else keeps working, so the
// only thing distinguishing this from a healthy run is an object nothing
// references.
type bucketWriteFailStore struct {
	// The concrete store, not the interface: the engine also uses the optional
	// ConditionalObjectStore methods, and a wrapper that hides them is not
	// equivalent to the store it wraps.
	inner *casStore
	fail  atomic.Bool
}

var bucketKeyPattern = regexp.MustCompile(`\d{20}-\d{20}\.idx$`)

func (f *bucketWriteFailStore) Put(ctx context.Context, key string, r io.Reader) error {
	if f.fail.Load() && bucketKeyPattern.MatchString(key) {
		return fmt.Errorf("bucket write is unavailable")
	}
	return f.inner.Put(ctx, key, r)
}

func (f *bucketWriteFailStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return f.inner.Get(ctx, key)
}

func (f *bucketWriteFailStore) Delete(ctx context.Context, key string) error {
	return f.inner.Delete(ctx, key)
}

func (f *bucketWriteFailStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return f.inner.GetRange(ctx, key, start, length)
}

func (f *bucketWriteFailStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	return f.inner.List(ctx, prefix)
}

func (f *bucketWriteFailStore) GetVersion(ctx context.Context, key string) ([]byte, string, bool, error) {
	return f.inner.GetVersion(ctx, key)
}

func (f *bucketWriteFailStore) PutVersion(ctx context.Context, key string, data []byte, version string) (string, error) {
	if f.fail.Load() && bucketKeyPattern.MatchString(key) {
		return "", fmt.Errorf("bucket write is unavailable")
	}
	return f.inner.PutVersion(ctx, key, data, version)
}

// A merge that fails after writing its segment must not leave the segment
// behind.
//
// The order is segment, then bucket, then the swap that commits it. A failure in
// between used to return with the segment still in object storage and named by
// nothing -- and since the swap is what replaces the sources, the segment's range
// is covered by nothing at that moment either. There was no path to delete it
// afterwards: no index named it, so no future merge or retention pass would ever
// look at it.
//
// knode3 had 218 of them, about 18MB each, roughly 4GB, accumulated over the
// life of the system.
func TestCompactPartition_FailedMergeDiscardsItsSegmentAndKeepsSources(t *testing.T) {
	inner := newCASStore()
	store := &bucketWriteFailStore{inner: inner}
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{
			Interval: time.Hour, MinSegments: 4,
			MaxMergedBytes: 32 * 1024, ScratchDir: t.TempDir(),
			MaxPassDuration: time.Minute,
		}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	pendingBefore := testutil.ToFloat64(metrics.CompactorUncommittedMergedSegments)
	writtenBefore := testutil.ToFloat64(metrics.CompactorMergedSegmentsWritten)
	commitsBefore := testutil.ToFloat64(metrics.CompactorMergeCommits)

	// Two buckets: the newest is left alone by design, so a lone bucket would
	// be skipped rather than merged and the failure would never be reached.
	_, sources := seedBucketSized(t, engine, inner, "orders", 0, 1000, 120, 1024)
	seedBucketSized(t, engine, inner, "orders", 0, 2000, 120, 1024)

	store.fail.Store(true)
	if _, err = engine.compactPartition(context.Background(), "orders", 0); err == nil {
		t.Fatal("expected the merge to fail when the bucket write fails")
	}

	// The segment was written, and must not still be there.
	if got := testutil.ToFloat64(metrics.CompactorMergedSegmentsWritten) - writtenBefore; got != 1 {
		t.Fatalf("expected the merged segment to have been written, got %v writes", got)
	}
	merged := 0
	for _, ref := range sources {
		rc, err := inner.Get(context.Background(), segmentKey("orders", 0, ref))
		if err == nil {
			_ = rc.Close()
		} else {
			merged++
		}
	}
	if merged > 0 {
		t.Errorf("%d source segments were deleted by a merge that never committed; "+
			"the index still names them, so their offsets are now unreadable", merged)
	}
	for _, ref := range sources[:4] {
		key := segmentKey("orders", 0, ref)
		rc, err := inner.Get(context.Background(), key)
		if err != nil {
			t.Errorf("source %s is gone after a failed merge: %v", key, err)
			continue
		}
		_ = rc.Close()
	}

	// Nothing may be left pending: the write and the commit do not balance.
	if got := testutil.ToFloat64(metrics.CompactorUncommittedMergedSegments); got != pendingBefore {
		t.Errorf("uncommitted merged segments is %v, want %v -- a discarded "+
			"segment must not stay on the gauge", got, pendingBefore)
	}
	if got := testutil.ToFloat64(metrics.CompactorMergeCommits); got != commitsBefore {
		t.Errorf("a merge that failed was counted as a commit")
	}

	// And the merged segment must not be on disk under any name.
	entries, err := inner.List(context.Background(), "orders/0/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	stray := 0
	for _, e := range entries {
		if e.Size > 20*1024 { // a merged product, larger than any source here
			stray++
		}
	}
	if stray != 0 {
		t.Errorf("%d large segment(s) left behind by a merge that never committed", stray)
	}
}

// A rewritten bucket must record the sum of what it holds, not the size of the
// segment the pass happened to produce.
//
// Bytes is what the byte retention policy accumulates against the budget. Setting
// it to the merged segment's own size records one entry's worth for a bucket
// holding all of them, so the index under-reports the partition by the number of
// entries it is carrying.
//
// On knode3 this had been true for as long as the compactor had been running: the
// index claimed 1.12GB of sealed data where 18.58GB sat on disk, a ratio of 0.06,
// in 38 of 46 buckets. Reads were unaffected, because they resolve offsets through
// the entries rather than through this field -- which is why nothing noticed. A
// KIMISTORE_RETENTION_BYTES set from real disk usage would have accounted for a
// sixteenth of what is stored and kept roughly sixteen times too much, while
// looking correctly configured.
func TestCompactPartition_RewrittenBucketRecordsTheSumOfItsEntries(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{
			Interval: time.Hour, MinSegments: 4,
			MaxMergedBytes: 32 * 1024, ScratchDir: t.TempDir(),
			MaxPassDuration: time.Minute,
		}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	// A bucket far larger than one merge's cap, so the pass rewrites it while
	// most of its entries are kept rather than folded in. A bucket that compacts
	// to a single segment would record the right total by accident.
	_, refs := seedBucketSized(t, engine, store.inner, "orders", 0, 1000, 400, 1024)
	seedBucketSized(t, engine, store.inner, "orders", 0, 5000, 400, 1024)

	if _, err := engine.compactPartition(context.Background(), "orders", 0); err != nil {
		t.Fatalf("compactPartition: %v", err)
	}

	window, buckets := engine.metadataCache.IndexSnapshot("orders", 0)
	var rewritten *BucketRef
	_ = window
	for i := range buckets {
		if buckets[i].First == 1000 {
			rewritten = &buckets[i]
		}
	}
	if rewritten == nil {
		t.Fatal("the merged bucket is not in the index")
	}

	body := readBucketObject(t, store.inner, rewritten.Key)
	var contents []SegmentRef
	if err := json.Unmarshal(body, &contents); err != nil {
		t.Fatalf("bucket body: %v", err)
	}
	var want int64
	for _, r := range contents {
		want += r.Size
	}
	if len(contents) < 2 {
		t.Fatalf("bucket holds %d entries; this test needs a partially merged bucket "+
			"where recording only the merged segment would under-report", len(contents))
	}
	if rewritten.Bytes != want {
		t.Errorf("bucket records Bytes=%d but its %d entries sum to %d (%.1fx low)",
			rewritten.Bytes, len(contents), want,
			float64(want)/float64(max(rewritten.Bytes, 1)))
	}

	// The sources it kept are still in the bucket, and the sources it merged are
	// not, so the entries are not simply the originals.
	if len(contents) == len(refs) {
		t.Error("bucket lists every original entry; the merge replaced none of them")
	}
}

// A bucket whose recorded byte total is wrong is corrected when the index loads,
// because nothing else will ever correct it.
//
// Until 1.0.20 the compactor wrote a rewritten bucket's Bytes from the single
// segment it had just produced. Those buckets persist: one whose entries are all
// already-merged products has nothing left to merge, so the compactor skips it
// forever and its total stays wrong for the life of the partition. Only a load
// that recomputes from the entries can fix it, which is what this covers.
func TestRestore_RepairsBucketByteTotalsThatDisagreeWithTheirEntries(t *testing.T) {
	inner := newCASStore()
	engine, err := NewStorageEngine(t.TempDir(), inner, "bucket", RetentionConfig{},
		WithCompactor(CompactorConfig{
			Interval: time.Hour, MinSegments: 4,
			MaxMergedBytes: 32 * 1024, ScratchDir: t.TempDir(),
		}))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	// Two buckets, so the seeded one is not the newest and is therefore not
	// skipped by compaction on a later pass.
	seedBucketSized(t, engine, inner, "orders", 0, 1000, 120, 1024)
	seedBucketSized(t, engine, inner, "orders", 0, 2000, 120, 1024)

	buckets := engine.metadataCache.BucketRefs("orders", 0)
	if len(buckets) != 2 {
		t.Fatalf("expected 2 sealed buckets, got %d", len(buckets))
	}
	target := buckets[0]

	refs, err := engine.readBucket(context.Background(), target)
	if err != nil {
		t.Fatalf("readBucket: %v", err)
	}
	var want int64
	for _, r := range refs {
		want += r.Size
	}
	if want == 0 {
		t.Fatal("fixture has no sized entries")
	}

	// The damage the old compactor wrote: the total of the last entry alone.
	// SeededBucket derives the total correctly, so it is injected afterwards --
	// which is also how it reached the real index, from a writer that believed it
	// knew the total.
	damage := refs[len(refs)-1].Size
	engine.metadataCache.RepairBucketBytes("orders", 0, target.Key, damage)
	if got := engine.metadataCache.BucketRefs("orders", 0)[0].Bytes; got != damage {
		t.Fatalf("fixture did not take: recorded %d, want %d", got, damage)
	}

	engine.repairBucketBytes(context.Background())

	var fixed *BucketRef
	for i, b := range engine.metadataCache.BucketRefs("orders", 0) {
		if b.Key == target.Key {
			fixed = &engine.metadataCache.BucketRefs("orders", 0)[i]
		}
	}
	if fixed == nil {
		t.Fatal("bucket vanished from the index")
	}
	if fixed.Bytes != want {
		t.Errorf("recorded Bytes=%d, entries sum to %d; a bucket the compactor will never "+
			"rewrite keeps its wrong total forever unless it is repaired at load", fixed.Bytes, want)
	}
}
