package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
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
	// ScratchDir is where segments are staged during a merge.
	ScratchDir string
}

func (c CompactorConfig) withDefaults() CompactorConfig {
	if c.Interval <= 0 {
		c.Interval = 15 * time.Minute
	}
	if c.MinSegments <= 0 {
		c.MinSegments = 8
	}
	if c.MaxMergedBytes <= 0 {
		c.MaxMergedBytes = wal.MaxSegmentSize
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
					if n, err := s.compactPartition(context.Background(), topic, partition); err != nil {
						log.Printf("Compactor: %s/%d: %v", topic, partition, err)
					} else if n > 0 {
						log.Printf("Compactor: %s/%d merged %d segment(s)", topic, partition, n)
					}
				}
			}
		}
	}
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

	b := buckets[0]
	refs, err := s.readBucket(ctx, b)
	if err != nil {
		return 0, fmt.Errorf("read bucket %s: %w", b.Key, err)
	}
	if len(refs) < cfg.MinSegments {
		return 0, nil
	}

	// Merge only a whole-bucket prefix, capped at the merged size the writer
	// would produce. Merging all 4096 entries of a bucket would produce a single
	// object far larger than any segment the writer emits.
	var sources []SegmentRef
	var bytesIn int64
	for _, ref := range refs {
		if len(sources) >= cfg.MinSegments && bytesIn >= cfg.MaxMergedBytes {
			break
		}
		sources = append(sources, ref)
		bytesIn += ref.Size
	}
	if len(sources) < cfg.MinSegments {
		return 0, nil
	}

	// An entry with no recorded size predates the index carrying sizes, so the
	// cap cannot be applied. Merging it would produce an object of unknown size,
	// which is how a single merge ends up costing more than it saves.
	for _, ref := range sources {
		if ref.Size == 0 {
			return 0, fmt.Errorf("bucket %s holds entries recorded before sizes were indexed; skipping", b.Key)
		}
	}

	merged, err := s.mergeRefs(ctx, topic, partition, sources, bytesIn, cfg)
	if err != nil {
		return 0, err
	}
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
	kept := append([]SegmentRef(nil), refs[len(sources):]...)
	// Last is the base offset of the last source entry, not base plus a count.
	// Offsets do not advance one per entry -- a segment's base is the offset its
	// first record carries, and entries can skip -- so the two differ on any
	// real partition. SealWindow derives it the same way, and a bucket whose
	// range overstated by even one would make BucketFor resolve an offset to the
	// wrong bucket.
	mergedLast := sources[len(sources)-1].Base
	next := BucketRef{
		First:  mergedRef.Base,
		Last:   mergedLast,
		Key:    bucketKey(topic, partition, mergedRef.Base, mergedLast),
		Bytes:  merged.BytesOut,
		Newest: mergedRef.Modified,
	}
	body, err := json.Marshal(append([]SegmentRef{mergedRef}, kept...))
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
func (s *StorageEngine) mergeRefs(ctx context.Context, topic string, partition int32, sources []SegmentRef, bytesIn int64, cfg CompactorConfig) (*stagedMerge, error) {
	dir, err := os.MkdirTemp(cfg.ScratchDir, "compact-*")
	if err != nil {
		return nil, fmt.Errorf("scratch dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	paths := make([]string, 0, len(sources))
	for i, ref := range sources {
		dst := filepath.Join(dir, fmt.Sprintf("src-%06d", i))
		if err := s.downloadSegment(ctx, topic, partition, ref, dst); err != nil {
			return nil, fmt.Errorf("download %s: %w", segmentKey(topic, partition, ref), err)
		}
		paths = append(paths, dst)
	}

	out := filepath.Join(dir, "merged.log")
	res, err := wal.MergeSegments(paths, out, sources[0].Base)
	if err != nil {
		return nil, err
	}
	if res.BytesIn != bytesIn {
		// The index and the objects disagree about how much data this range
		// holds. Merging anyway would write a bucket whose recorded size is
		// wrong, and the byte policy would then be reasoning from fiction.
		return nil, fmt.Errorf("index records %d bytes for this range, objects hold %d", bytesIn, res.BytesIn)
	}

	// The scratch directory is removed by the defer above, so the merged file is
	// staged outside it and left for the caller to upload.
	merged := filepath.Join(cfg.ScratchDir, fmt.Sprintf("merged-%s-%d-%020d-%020d.log",
		filepath.Base(dir), partition, sources[0].Base, sources[len(sources)-1].Base))
	if err := moveFile(out, merged); err != nil {
		return nil, fmt.Errorf("stage merged segment: %w", err)
	}
	return &stagedMerge{MergeResult: res, Path: merged}, nil
}

// mergedEpoch picks an epoch above every source's, so the merged object cannot
// overwrite any segment it was built from.
//
// A bucket's entries share an epoch in the common case, so this is normally
// one more than a single number. Taking the maximum across the sources rather
// than the first matters when a re-claimed partition left entries from two
// epochs in one bucket.
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
