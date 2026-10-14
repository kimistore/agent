package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"kimistore/internal/metrics"
	"kimistore/internal/storage/index"
	"kimistore/internal/storage/wal"
)

// CompactorConfig configures background segment consolidation.
type CompactorConfig struct {
	// Enabled turns the compactor on. It is on by default, which is a change
	// from the first version: leaving it off meant the object count a partition
	// accumulated was governed by the flush interval and nothing else, and no
	// amount of waiting brought it down.
	//
	// Compaction is conservative by construction, which is what makes the
	// default defensible. It only ever merges sealed buckets, never the open
	// window and never the newest sealed bucket, so the range a reader is
	// working in is untouched. It refuses entries recorded before the index
	// carried sizes rather than merging them blind. And it commits the index
	// before deleting anything, so an interrupted merge leaves duplicate data
	// rather than a hole.
	Enabled bool
	// Disabled inverts Enabled, for a caller that wants compaction off without
	// having to distinguish "unset" from "explicitly false".
	//
	// The zero value of a bool cannot mean "on by default", so the two cannot
	// both be had. Compaction being opt-out is the deliberate choice: leaving it
	// opt-in meant the object count was governed by the flush interval and
	// nothing reduced it, and an operator who wants it off can say so
	// unambiguously.
	Disabled bool
	// Interval is how often a partition is considered for compaction.
	//
	// The default is a minute because the interval decides how often a partition
	// is reconsidered, and at fifteen minutes against this workload a backlog
	// took hours to clear. A pass now consolidates repeatedly until its own
	// MaxPassDuration runs out or the queue empties, so the interval only sets
	// the floor between sweeps, not the rate of work.
	Interval time.Duration
	// MinSegments is how many segments must be covered before a merge is worth
	// doing. Merging two segments into one halves the object count and costs a
	// full read and rewrite, so a floor keeps the compactor off buckets that are
	// already reasonably sized.
	MinSegments int
	// MaxMergedBytes bounds the size of a merged segment. It should match the
	// writer's roll threshold: a merged segment that is larger than what the
	// writer would produce is not what anything downstream expects.
	MaxMergedBytes int64
	// MaxPassDuration bounds how long one pass keeps consolidating before
	// yielding to the next tick.
	//
	// It is wall-clock rather than a merge count because the useful bound is the
	// one that stops the compactor monopolising the disk, and how many merges fit
	// in that depends on the object store. Thirty seconds against a one minute
	// interval leaves room for ingest between passes while still clearing a
	// backlog in minutes rather than hours; once the queue is empty the loop exits
	// on its own and the budget is never reached.
	MaxPassDuration time.Duration
	// ScratchDir is where segments are staged during a merge.
	ScratchDir string
}

func (c CompactorConfig) withDefaults() CompactorConfig {
	if c.Interval <= 0 {
		c.Interval = time.Minute
	}
	if c.MinSegments <= 0 {
		c.MinSegments = 8
	}
	if c.MaxMergedBytes <= 0 {
		c.MaxMergedBytes = wal.MaxSegmentSize
	}
	if c.MaxPassDuration <= 0 {
		c.MaxPassDuration = 30 * time.Second
	}
	return c
}

// WithCompactor configures background segment consolidation. It is an Option so
// the configuration is in place before the compactor goroutine starts.
func WithCompactor(cfg CompactorConfig) Option {
	return func(s *StorageEngine) { s.compactorCfg = cfg }
}

func (s *StorageEngine) compactorLoop() {
	defer s.wg.Done()
	cfg := s.compactorCfg.withDefaults()
	if cfg.Disabled {
		// Nothing to do. The goroutine was counted in the WaitGroup when it was
		// started, so returning here is correct -- what is not correct is
		// starting it at all, which is what the guard in NewStorageEngine does.
		return
	}
	if cfg.ScratchDir == "" {
		cfg.ScratchDir = s.walDir
	}
	if err := os.MkdirAll(cfg.ScratchDir, 0o755); err != nil {
		log.Printf("Compactor: cannot use scratch dir %s: %v", cfg.ScratchDir, err)
		return
	}

	// Finish any merge a previous process committed but did not finish deleting
	// before this one ran. It runs once, before the first compaction pass, and
	// only after the index has been restored -- the marker is found through the
	// bucket directory, so an empty index would find nothing and the sources
	// would stay orphaned.
	go func() {
		ctx := context.Background()
		for _, topic := range s.metadataCache.GetTopics() {
			for _, partition := range s.metadataCache.GetPartitions(topic) {
				if n := s.resumeInterruptedMerges(ctx, topic, partition); n > 0 {
					log.Printf("Compactor: recovered %d orphaned source segment(s) in %s/%d", n, topic, partition)
				}
			}
		}
	}()

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			for _, topic := range s.metadataCache.GetTopics() {
				for _, partition := range s.metadataCache.GetPartitions(topic) {
					s.compactorPass(topic, partition)
				}
			}
		}
	}
}

// compactorPass runs one compaction pass over a partition and records what it
// did.
//
// The recording is a method rather than inlined in the ticker loop so that a
// test can reach it. While it was inlined, the only way to observe it was a live
// ticker, and a compactor that had stopped -- the failure these metrics exist to
// detect -- was precisely the case a test could not produce.
//
// Every pass counts, whether or not it found work. That is what separates
// "running with nothing to do" from "stopped": the pass counter climbs in the
// first case and is flat in the second, and only the second is a fault.
func (s *StorageEngine) compactorPass(topic string, partition int32) {
	metrics.CompactorPasses.Inc()

	// Keep merging until the queue is empty or the budget runs out.
	//
	// One merge per tick was an arbitrary cap, and an expensive one: each merge
	// consolidates roughly 200 segments, and on knode3 a minute of ingest adds
	// only about 60, so a single merge per interval is already a surplus. It just
	// spends it slowly. Against a backlog of a dozen full buckets the pass had
	// 49,000 segments to reclaim and could reach 198 of them a minute, which is
	// hours of queueing to reclaim data that was never in doubt, while the
	// compactor sat idle through 95% of every interval.
	//
	// Each merge is its own transaction -- merged segment, then bucket, then the
	// index swap that commits it, then the marker, then the sources -- so
	// repeating them changes throughput and nothing about what survives a crash
	// mid-pass. The loop re-reads the index through compactPartition on every
	// iteration rather than carrying state across merges, so it cannot merge
	// against a view that a previous merge invalidated.
	//
	// The bound is wall-clock, not a count. A count would mean the same thing on
	// a fast object store and a slow one; the time budget is what keeps the
	// compactor from monopolising the disk it is compacting onto. Progress is
	// guaranteed independently of it: a merge replaces its sources with one
	// segment, and a segment at least half the cap is never itself a source, so
	// each iteration strictly reduces the number of mergeable entries.
	cfg := s.compactorCfg.withDefaults()
	deadline := time.Now().Add(cfg.MaxPassDuration)
	ctx := context.Background()

	var merges, segments int
	for {
		n, err := s.compactPartition(ctx, topic, partition)
		switch {
		case err != nil:
			// Stop rather than continue. A failure here is a store problem, and
			// retrying it in a tight loop would turn one error into a spin.
			metrics.CompactorPassErrors.Inc()
			log.Printf("Compactor: %s/%d: %v", topic, partition, err)
			if merges > 0 {
				recordPass(work{merges: merges, segments: segments})
				log.Printf("Compactor: %s/%d merged %d run(s), %d segment(s) before failing",
					topic, partition, merges, segments)
			}
			return
		case n == 0:
			if merges == 0 {
				return
			}
			recordPass(work{merges: merges, segments: segments})
			log.Printf("Compactor: %s/%d merged %d run(s), %d segment(s)",
				topic, partition, merges, segments)
			return
		}

		merges++
		segments += n

		// Checked after the merge, so the budget is never spent on nothing: a
		// pass always completes the merge it started.
		if time.Now().After(deadline) {
			metrics.CompactorPassBudgetExhausted.Inc()
			log.Printf("Compactor: %s/%d merged %d run(s), %d segment(s) before its %s budget",
				topic, partition, merges, segments, cfg.MaxPassDuration)
			recordPass(work{merges: merges, segments: segments})
			return
		}
	}
}

// work is one pass's tally of what it consolidated.
type work struct {
	merges   int
	segments int
}

// recordPass publishes a pass's tally.
//
// The metrics count merges, not passes that merged. While a pass was a single
// merge the two were the same number and the distinction was invisible; with a
// pass doing many, counting the pass would flatten merges against passes and
// destroy the difference the liveness alert reads -- a pass that did thirty
// merges and a pass that did one would look identical.
func recordPass(w work) {
	if w.merges == 0 {
		return
	}
	metrics.CompactorMerges.Add(float64(w.merges))
	metrics.CompactorSegmentsMerged.Add(float64(w.segments))
	// Set last, and only on a real merge. An idle pass leaves the value alone, so
	// a stale timestamp means "no merge in a long time" -- the condition worth
	// alerting on.
	metrics.CompactorLastMergeTimestamp.SetToCurrentTime()
}

// compactionMarker names the object that records which sources a merge replaced.
//
// It exists so a restart can finish a deletion that was interrupted. Without
// it, a crash between writing the merged segment and deleting the sources leaves
// them orphaned: unreachable, so they cost storage and nothing else, and no
// listing could ever find them again.
const compactionMarkerSuffix = ".compacting"

// compactPartition merges the oldest eligible run of sealed buckets into one
// segment each and returns how many source segments it consolidated.
//
// The ordering is the whole safety argument, and it is not negotiable:
//
//  1. read the sources and merge them locally, verifying as it goes
//  2. write the merged segment
//  3. write the rewritten bucket naming it
//  4. swap the bucket reference in the index -- the commit point
//  5. write the marker naming the sources, so an interrupted step 6 can resume
//  6. delete the sources and the marker
//
// A crash before step 4 leaves the index naming segments that exist, and the
// merged object is orphaned garbage that a later pass overwrites. A crash after
// step 4 leaves the index naming a segment that exists, with the sources still
// present as duplicates. Neither loses data, and both are recoverable, which is
// why the index is committed before anything is deleted.
func (s *StorageEngine) compactPartition(ctx context.Context, topic string, partition int32) (int, error) {
	cfg := s.compactorCfg.withDefaults()

	buckets, err := s.eligibleBuckets(topic, partition, cfg)
	if err != nil {
		return 0, err
	}
	if len(buckets) == 0 {
		return 0, nil
	}

	// Scan forward to the first bucket that can actually be merged, rather than
	// taking the oldest and giving up on it.
	//
	// Taking only the oldest meant one unmergeable bucket -- here, one recorded
	// before the index carried sizes -- blocked every later one indefinitely. The
	// compactor reported the same skip every 15 minutes for 23 hours while
	// fourteen compactable buckets sat behind it.
	//
	// The candidate list is oldest-first and this stops at the first workable
	// bucket, so precedence is unchanged: the oldest mergeable range is still
	// compacted first, and a skipped bucket no longer starves the ones behind it.
	var (
		b       BucketRef
		refs    []SegmentRef
		picked  []int
		found   bool
		readErr error
	)
	for _, cand := range buckets {
		candRefs, err := s.readBucket(ctx, cand)
		if err != nil {
			readErr = fmt.Errorf("read bucket %s: %w", cand.Key, err)
			continue
		}
		if !allSized(candRefs) {
			continue
		}
		// The candidate is accepted only if a pass over it would actually pick
		// something. A bucket whose entries are all already merged products is
		// skipped by the selection, so accepting it here made it win the queue
		// and then yield nothing -- which is what stopped compaction entirely
		// once the backlog was compacted: the first bucket was fully merged, so
		// every entry was skipped, and no bucket behind it was ever reached.
		candPicked := selectSources(candRefs, cfg)
		if len(candPicked) < cfg.MinSegments {
			continue
		}
		b, refs, picked, found = cand, candRefs, candPicked, true
		break
	}
	if !found {
		if readErr != nil {
			return 0, readErr
		}
		return 0, nil
	}

	// Merge only a whole-bucket prefix, capped at the merged size the writer
	// would produce. Merging all 4096 entries of a bucket would produce a single
	// object far larger than any segment the writer emits.
	// picked holds the position in refs of every entry this pass merges, and is
	// the single source of truth for what the rewritten bucket must stop naming.
	//
	// It exists because the sources are not a prefix of the bucket. The loop
	// below steps over entries that are already merged products, so the sources
	// are a subset. Deriving the rest of the bucket by slicing on the length of
	// the sources listed merged segments -- whose objects this pass then deletes
	// -- and dropped entries that were left alone. On knode3 that destroyed seven
	// offsets.
	// picked was chosen above, where the candidate was accepted on the same
	// test, so it is not recomputed here: the two would have to agree for the
	// pass to merge what the selection promised.

	merged, picked, err := s.mergeRefs(ctx, topic, partition, refs, picked, cfg)
	if err != nil {
		return 0, err
	}
	sources := refsAt(refs, picked)
	// The merged segment needs its own key. Giving it the first source's base
	// and epoch would write over that source's object, and the delete that
	// follows would then remove the only copy of the range.
	//
	// The epoch is bumped instead: it is already the agent's per-partition
	// claim, it is what distinguishes two segments at the same base offset, and
	// a value above every source's keeps the merged object from colliding with
	// any of them. This is the same rule AddSegment uses when a re-claimed
	// partition writes an offset again under a newer epoch.
	mergedRef := SegmentRef{
		Base:     sources[0].Base,
		Epoch:    mergedEpoch(sources),
		Size:     merged.BytesOut,
		Modified: time.Now().Unix(),
	}

	// Steps 2 and 3: the merged segment and the bucket that names it.
	if err := s.writeMergedSegment(ctx, topic, partition, mergedRef, merged.Path, merged.BytesOut); err != nil {
		return 0, fmt.Errorf("write merged segment: %w", err)
	}
	// Everything this pass did not merge, selected by membership. Slicing
	// refs[len(sources):] assumes the sources are a prefix, and they are not --
	// the selection steps over entries that are already merged products.
	takenSet := make([]bool, len(refs))
	for _, i := range picked {
		takenSet[i] = true
	}
	kept := make([]SegmentRef, 0, len(refs))
	for i, ref := range refs {
		if !takenSet[i] {
			kept = append(kept, ref)
		}
	}
	// Last is the base offset of the last source entry, not base plus a count.
	// Offsets do not advance one per entry -- a segment's base is the offset its
	// first record carries, and entries can skip -- so the two differ on any
	// real partition. SealWindow derives it the same way, and a bucket whose
	// range overstated by even one would make BucketFor resolve an offset to the
	// wrong bucket.
	// The rewritten bucket is described by its contents, not derived from the
	// sources, so its range and key are whatever it actually holds.
	contents := make([]SegmentRef, 0, len(kept)+1)
	contents = append(contents, kept...)
	contents = append(contents, mergedRef)
	// Sorted explicitly: a stepped-over entry at the head leaves the merged
	// segment's base above the bucket's first, and BucketFor binary-searches this
	// list -- one that is out of order is silently missed.
	sort.Slice(contents, func(i, j int) bool { return contents[i].Base < contents[j].Base })

	// The bucket's range is unchanged by a merge: it still describes the same
	// offsets it did before, with the same entries folded into fewer segments.
	//
	// Taking it from the contents instead is wrong whenever the pass merged the
	// whole bucket -- the single merged entry sits at the first source's base,
	// so the range would collapse onto that base and every offset above it would
	// fall outside every bucket. SealWindow derives it the same way: the last
	// entry's base offset, not the last entry's reach.
	first, last := refs[0].Base, refs[len(refs)-1].Base
	next := BucketRef{
		First:  first,
		Last:   last,
		Key:    bucketKey(topic, partition, first, last),
		Bytes:  merged.BytesOut,
		Newest: mergedRef.Modified,
	}
	// The entries this pass did not merge stay in the bucket. Leaving them out
	// drops them from the index: they would remain in object storage,
	// unreachable, and a read for their offsets would fail.
	body, err := json.Marshal(contents)
	if err != nil {
		return 0, err
	}
	if err := s.PutObject(ctx, next.Key, body); err != nil {
		return 0, fmt.Errorf("write merged bucket: %w", err)
	}

	// Step 4: commit.
	if !s.metadataCache.SwapBucket(topic, partition, b.Key, next) {
		// Retention rewrote or dropped this bucket while the merge ran. The
		// merged object and the new bucket are now unreferenced, so leave the
		// sources alone and let a later pass retry.
		log.Printf("Compactor: %s/%d bucket %s changed under the merge; leaving sources in place",
			topic, partition, b.Key)
		return 0, fmt.Errorf("bucket %s changed during merge", b.Key)
	}
	s.markManifestDirty(topic, partition)

	// Step 5: record the sources so an interrupted delete can be finished.
	marker := s.markerKey(topic, partition, next.Key)
	if err := s.PutObject(ctx, marker, marshalRefs(sources)); err != nil {
		log.Printf("Compactor: cannot record merge marker %s: %v", marker, err)
		return len(sources), nil
	}

	// Step 6: the sources are no longer named by the index.
	deleted := 0
	for _, ref := range sources {
		if s.deleteSegmentObjects(ctx, topic, partition, ref) {
			deleted++
		}
	}
	// The old bucket is deleted only when the merge produced a different one. A
	// merge that consumed the whole bucket produces the same key -- same first
	// and last offset -- so deleting it would remove the very bucket the index
	// now names, and every read in the range would fail to resolve.
	if b.Key != next.Key {
		if err := s.objDelete(ctx, b.Key); err != nil {
			log.Printf("Compactor: cannot delete old bucket %s: %v", b.Key, err)
		}
	}
	if err := s.objDelete(ctx, marker); err != nil {
		log.Printf("Compactor: cannot remove merge marker %s: %v", marker, err)
	}
	metrics.RetentionSegmentsDeleted.Add(float64(deleted))

	return len(sources), nil
}

// eligibleBuckets returns sealed buckets worth merging, oldest first.
//
// Only sealed buckets are considered. The open window is still being appended
// to, and a segment in it may be the one a consumer is reading right now.
func (s *StorageEngine) eligibleBuckets(topic string, partition int32, cfg CompactorConfig) ([]BucketRef, error) {
	window, sealed := s.metadataCache.IndexSnapshot(topic, partition)
	if len(sealed) == 0 {
		return nil, nil
	}

	// Never merge the newest sealed bucket: it abuts the open window, and
	// merging it would rewrite the range a reader is most likely to be in.
	candidates := sealed
	if len(candidates) > 1 {
		candidates = candidates[:len(candidates)-1]
	} else {
		return nil, nil
	}

	// Anything too small to be worth merging is skipped without being read.
	out := make([]BucketRef, 0, len(candidates))
	for _, b := range candidates {
		if b.Bytes == 0 {
			// Recorded before the index carried sizes; the cap cannot be
			// applied, so leave it until it has been through a rewrite.
			continue
		}
		out = append(out, b)
	}
	_ = window
	return out, nil
}

// mergeRefs downloads the sources, merges them, and returns the result.
func (s *StorageEngine) mergeRefs(ctx context.Context, topic string, partition int32, refs []SegmentRef, picked []int, cfg CompactorConfig) (*stagedMerge, []int, error) {
	dir, err := os.MkdirTemp(cfg.ScratchDir, "compact-*")
	if err != nil {
		return nil, nil, fmt.Errorf("scratch dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	paths := make([]string, 0, len(picked))
	have := make([]SegmentRef, 0, len(picked))
	havePicked := make([]int, 0, len(picked))
	for k, idx := range picked {
		ref := refs[idx]
		dst := filepath.Join(dir, fmt.Sprintf("src-%06d", k))
		if err := s.downloadSegment(ctx, topic, partition, ref, dst); err != nil {
			// An index entry whose object is gone cannot be merged, and
			// aborting on it would retry the same entry on every pass forever --
			// which is exactly what knode3 did, silently, for hours.
			//
			// It is also not recoverable: nothing else names those offsets. The
			// entry is dropped from the merge rather than blocking it, and the
			// log start moves past it, because a gap that no object can fill is
			// better reported by ListOffsets than by a read that never resolves.
			if isNotFound(err) {
				log.Printf("Compactor: %s/%d index names %s but the object is gone; dropping it from the merge",
					topic, partition, segmentKey(topic, partition, ref))
				s.markManifestDirty(topic, partition)
				continue
			}
			return nil, nil, fmt.Errorf("download %s: %w", segmentKey(topic, partition, ref), err)
		}
		paths = append(paths, dst)
		have = append(have, ref)
		havePicked = append(havePicked, idx)
	}
	if len(have) < cfg.MinSegments {
		return nil, nil, fmt.Errorf("fewer than %d of this bucket's segments exist in object storage", cfg.MinSegments)
	}
	sources := have
	// The byte total is taken from what was actually taken. An entry dropped for
	// being absent is no longer part of the merge, and counting it would fail the
	// consistency check below for a reason that has nothing to do with the
	// objects on hand.
	var bytesIn int64
	for _, ref := range sources {
		bytesIn += ref.Size
	}

	out := filepath.Join(dir, "merged.log")
	res, err := wal.MergeSegments(paths, out, sources[0].Base)
	if err != nil {
		return nil, nil, err
	}
	if res.BytesIn != bytesIn {
		// The index and the objects disagree about how much data this range
		// holds. Merging anyway would write a bucket whose recorded size is
		// wrong, and the byte policy would then be reasoning from fiction.
		return nil, nil, fmt.Errorf("index records %d bytes for this range, objects hold %d", bytesIn, res.BytesIn)
	}

	// The scratch directory is removed by the defer above, so the merged file is
	// staged outside it and left for the caller to upload.
	merged := filepath.Join(cfg.ScratchDir, fmt.Sprintf("merged-%s-%d-%020d-%020d.log",
		filepath.Base(dir), partition, sources[0].Base, sources[len(sources)-1].Base))
	if err := moveFile(out, merged); err != nil {
		return nil, nil, fmt.Errorf("stage merged segment: %w", err)
	}
	return &stagedMerge{MergeResult: res, Path: merged}, havePicked, nil
}

// refsAt materialises the entries at the given positions in refs.
func refsAt(refs []SegmentRef, idx []int) []SegmentRef {
	out := make([]SegmentRef, 0, len(idx))
	for _, i := range idx {
		out = append(out, refs[i])
	}
	return out
}

// mergedEpoch picks an epoch above every source's, so the merged object cannot
// overwrite any segment it was built from.
//
// A bucket's entries share an epoch in the common case, so this is normally
// one more than a single number. Taking the maximum across the sources rather
// than the first matters when a re-claimed partition left entries from two
// epochs in one bucket.
// allSized reports whether every reference carries a recorded size.
//
// A zero size means the entry predates the index carrying sizes, and the cap on
// merged size cannot be applied to it. Merging such an entry blind would produce
// an object of unknown size, which is how a single merge ends up costing more
// than it saves.
func allSized(refs []SegmentRef) bool {
	for _, ref := range refs {
		if ref.Size == 0 {
			return false
		}
	}
	return true
}

func mergedEpoch(sources []SegmentRef) int64 {
	var max int64
	for _, ref := range sources {
		if ref.Epoch > max {
			max = ref.Epoch
		}
	}
	return max + 1
}

func (s *StorageEngine) markerKey(topic string, partition int32, bucket string) string {
	return fmt.Sprintf("%s/%d/%s%s", topic, partition, filepath.Base(bucket), compactionMarkerSuffix)
}

func marshalRefs(refs []SegmentRef) []byte {
	b, err := json.Marshal(refs)
	if err != nil {
		return nil
	}
	return b
}

// downloadSegment copies one segment object out of the store.
func (s *StorageEngine) downloadSegment(ctx context.Context, topic string, partition int32, ref SegmentRef, dst string) error {
	rc, err := s.objGet(ctx, segmentKey(topic, partition, ref))
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, rc); err != nil {
		_ = f.Close()
		return err
	}
	return f.Sync()
}

// writeMergedSegment writes the merged object and its index sidecar.
//
// The sidecar is regenerated from the merged file rather than copied from the
// sources. It records byte offsets of entries within a segment, so a copy of a
// source sidecar would point at positions that no longer exist -- the merged
// file concatenates them. Generating it costs one pass over the merged file,
// which was written locally and so is still cheap to read.
func (s *StorageEngine) writeMergedSegment(ctx context.Context, topic string, partition int32, ref SegmentRef, path string, size int64) error {
	if size <= 0 {
		return fmt.Errorf("merged segment has no size")
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	putCtx, cancel := s.objCtx(ctx)
	defer cancel()
	key := segmentKey(topic, partition, ref)
	if err := s.objStore.Put(putCtx, key, f); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()

	indexData, err := index.GenerateIndex(path, ref.Base)
	if err != nil {
		// Without a sidecar the reader falls back to scanning entry headers,
		// which is correct but slower. Failing the merge here would be worse
		// than the object having no sidecar, so this is logged, not fatal.
		log.Printf("Compactor: cannot generate index for merged segment %s: %v", key, err)
		return nil
	}
	if len(indexData) == 0 {
		return nil
	}
	return s.objPut(ctx, strings.TrimSuffix(key, ".log")+".index", bytes.NewReader(indexData))
}

// stagedMerge is a merged segment still on local disk, awaiting upload.
type stagedMerge struct {
	*wal.MergeResult
	Path string
}

// moveFile renames src to dst, falling back to a copy when the two are on
// different filesystems.
func moveFile(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	_ = out.Close()
	return os.Remove(src)
}

// resumeInterruptedMerges finishes deletions left half-done by a crash.
//
// A merge writes its marker after committing the index and deletes the sources
// after that. A crash in between leaves the sources unreferenced: the index
// names the merged segment, so nothing will read them again, and no listing
// could find them either. They are storage that is never reclaimed and never
// visible again.
//
// The marker is found through the checkpoint rather than by listing, which is
// what makes recovery possible on this bucket at all. Markers are named after
// the bucket that replaced them, so every marker belongs to a bucket the index
// now names -- one probe per sealed bucket, and a missing marker is the normal
// case. A partition with a few hundred sealed buckets costs a few hundred GETs
// at startup, against a listing that cannot complete here at all.
//
// A marker whose bucket the index does not name belongs to a merge that never
// committed. Its sources are still live, so it is left alone rather than acted
// on: deleting them would remove data nothing else refers to.
func (s *StorageEngine) resumeInterruptedMerges(ctx context.Context, topic string, partition int32) int {
	_, sealed := s.metadataCache.IndexSnapshot(topic, partition)

	deleted := 0
	for _, b := range sealed {
		marker := s.markerKey(topic, partition, b.Key)
		body, err := s.objGetWhole(ctx, marker)
		if err != nil {
			// No marker: this bucket is not the result of an interrupted merge.
			continue
		}

		var sources []SegmentRef
		if err := json.Unmarshal(body, &sources); err != nil || len(sources) == 0 {
			// An unreadable marker is not permission to delete anything. The
			// sources it would name are still referenced by the index until the
			// merge commits, so guessing here is how recovery becomes loss.
			log.Printf("Compactor: marker %s is not readable (%v); leaving its sources in place", marker, err)
			continue
		}

		// The index names this bucket, so the merge committed and these sources
		// are the ones it replaced.
		removed := 0
		for _, ref := range sources {
			if s.deleteSegmentObjects(ctx, topic, partition, ref) {
				removed++
			}
		}
		deleted += removed
		metrics.RetentionSegmentsDeleted.Add(float64(removed))
		if err := s.objDelete(ctx, marker); err != nil {
			log.Printf("Compactor: cannot remove recovered marker %s: %v", marker, err)
		}
		log.Printf("Compactor: resumed an interrupted merge for %s/%d (%d source segment(s) reclaimed)",
			topic, partition, removed)
	}
	return deleted
}

// isNotFound reports whether an object-store error means the object is absent,
// rather than the store being unreachable or timing out.
//
// The distinction decides whether a merge should drop an index entry or give up.
// A missing object cannot be fetched by retrying, so retrying is what turns one
// bad entry into a permanently stuck partition.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "nosuchkey") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "statuscode: 404")
}

// selectSources returns the positions in refs that a pass would merge.
//
// It steps over entries at least half the merged-size cap, which are earlier
// passes' products, and stops before an entry would carry the result past the
// cap. Both halves matter: taking an already-merged entry spends the whole
// budget on it and leaves the pass merging nothing, and taking one that
// overshoots produces an object larger than anything the writer emits.
//
// Candidate selection and the pass itself both call this, so a bucket is only
// taken when a pass over it would actually have something to do.
func selectSources(refs []SegmentRef, cfg CompactorConfig) []int {
	var picked []int
	var bytesIn int64
	for i, ref := range refs {
		if ref.Size >= cfg.MaxMergedBytes/2 {
			continue
		}
		if bytesIn+ref.Size > cfg.MaxMergedBytes {
			break
		}
		picked = append(picked, i)
		bytesIn += ref.Size
	}
	return picked
}
