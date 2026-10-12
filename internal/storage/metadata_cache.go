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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"kimistore/internal/storage/wal"
)

// MetadataCache is the broker's authoritative view of what exists: which
// topics and partitions are known, and where each partition's offsets stand.
//
// It is the only durable record of a partition's log end offset. The in-memory
// WAL can only report offsets it can still see on local disk, and local
// segments are deleted once they reach object storage, so a restart that
// trusted the WAL alone would rewind the log to zero and start handing out
// offsets that are already taken.
type MetadataCache struct {
	Topics map[string]*TopicState `json:"topics"`
	// Committed carries per-group consumer offsets so retention's log start
	// survives a restart without re-reading every offset object. Object
	// storage remains authoritative; this is a fast path on top of it.
	Committed map[string]map[string]int64 `json:"committed_offsets,omitempty"`

	// Consumer group state is deliberately absent, and used to be here.
	//
	// Group state cannot be persisted across a coordinator change: the members
	// in a restored group belonged to connections on a process that has exited,
	// and their assignments were computed against a member set that no longer
	// exists. Replaying it produces a group that looks alive and rebalances
	// wrongly. It is rebuilt from JoinGroup instead, which is the same cost as a
	// full rebalance -- the cost D-4 already accepts.
	//
	// Offsets are the exception and are still carried here, because they are the
	// one part of a group that is genuinely durable and genuinely needed.

	// WriterEpoch is the writer-lease epoch of the agent that wrote this
	// checkpoint. A restart that reads a checkpoint stamped with a higher
	// epoch than its own knows another writer has taken the log over, and
	// refuses to serve rather than overwriting a log it no longer owns.
	WriterEpoch int64 `json:"writer_epoch,omitempty"`
	// Writer identifies that agent, purely for whoever reads the object by hand.
	Writer string `json:"writer,omitempty"`

	mu sync.RWMutex
}

type TopicState struct {
	Partitions map[int32]*PartitionState `json:"partitions"`
}

// PartitionState is the durable position of one partition.
type PartitionState struct {
	// LogEndOffset is the offset the next record will be written at. It is
	// the value that must survive a restart, and the one ListOffsets reports
	// as the latest offset.
	LogEndOffset int64 `json:"log_end_offset"`
	// HighWatermark is the last offset replicated to consumers. On a
	// single-node agent it tracks LogEndOffset.
	HighWatermark int64 `json:"high_watermark"`
	// LogStartOffset is the oldest offset still retrievable. Retention
	// advances it as it reclaims segments; it is what ListOffsets must
	// report as the earliest offset.
	LogStartOffset int64 `json:"log_start_offset"`

	// Segments is the ordered segment inventory, oldest first.
	Segments []*SegmentMetadata `json:"segments"`
	Recent   []SegmentRef       `json:"recent,omitempty"`
	Sealed   []BucketRef        `json:"sealed,omitempty"`
}

// SegmentRef names one uploaded segment by its base offset and ownership
// epoch. The object key is derived from those two numbers and the partition
// prefix, so storing the key itself spends ~90 bytes to say what 16 say.
type SegmentRef struct {
	Base  int64 `json:"b"`
	Epoch int64 `json:"e"`
}

// BucketRef names a sealed run of segments. A sealed bucket is immutable, so
// the run it covers cannot change after it is written.
type BucketRef struct {
	First int64  `json:"first"`
	Last  int64  `json:"last"`
	Key   string `json:"key"`
}

// RecentWindow is how many segments stay inline in the checkpoint before they
// are sealed into a bucket.
//
// Reads are hot at this workload -- the consumer sits one offset behind the log
// end -- so the window absorbs that. It is sized to keep the checkpoint small:
// at 4096 the inline window is ~64 KB and the directory grows at history/4096.
const RecentWindow = 4096

type SegmentMetadata struct {
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	S3Key       string `json:"s3_key"`
}

func NewMetadataCache() *MetadataCache {
	return &MetadataCache{
		Topics: make(map[string]*TopicState),
	}
}

// Load scans the WAL directory for topics and partitions that already exist on
// local disk. It seeds structure only: the log end offset comes from the
// checkpoint or object storage, because local disk understates it.
func (mc *MetadataCache) Load(walDir string) error {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.Topics == nil {
		mc.Topics = make(map[string]*TopicState)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		partitionEntries, err := os.ReadDir(filepath.Join(walDir, entry.Name()))
		if err != nil {
			continue
		}
		for _, pe := range partitionEntries {
			if !pe.IsDir() {
				continue
			}
			pID, err := strconv.Atoi(pe.Name())
			if err != nil {
				continue
			}
			mc.touchLocked(entry.Name(), int32(pID))
		}
	}
	return nil
}

// touchLocked ensures a topic/partition entry exists. Callers must hold mu.
func (mc *MetadataCache) touchLocked(topic string, partition int32) *PartitionState {
	if mc.Topics[topic] == nil {
		mc.Topics[topic] = &TopicState{Partitions: make(map[int32]*PartitionState)}
	}
	if mc.Topics[topic].Partitions == nil {
		mc.Topics[topic].Partitions = make(map[int32]*PartitionState)
	}
	ps := mc.Topics[topic].Partitions[partition]
	if ps == nil {
		ps = &PartitionState{}
		mc.Topics[topic].Partitions[partition] = ps
	}
	if ps.Segments == nil {
		ps.Segments = make([]*SegmentMetadata, 0)
	}
	return ps
}

// ---- accessors ----

func (mc *MetadataCache) GetTopics() []string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	topics := make([]string, 0, len(mc.Topics))
	for t := range mc.Topics {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	return topics
}

func (mc *MetadataCache) GetTopicCount() int {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return len(mc.Topics)
}

func (mc *MetadataCache) GetPartitionCount() int {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	count := 0
	for _, t := range mc.Topics {
		count += len(t.Partitions)
	}
	return count
}

func (mc *MetadataCache) AddTopic(topic string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.Topics[topic] == nil {
		mc.Topics[topic] = &TopicState{Partitions: make(map[int32]*PartitionState)}
	}
}

// Has reports whether a topic/partition is already known. The append path
// calls this before taking the write lock, so a steady stream of writes to
// already-known partitions never serialises on it.
func (mc *MetadataCache) Has(topic string, partition int32) bool {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	ts, ok := mc.Topics[topic]
	if !ok {
		return false
	}
	_, ok = ts.Partitions[partition]
	return ok
}

func (mc *MetadataCache) AddPartition(topic string, partition int32) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.touchLocked(topic, partition)
}

// AdvancePartition is the hot-path update after a successful append. It only
// ever moves the log end offset forward, so a lost update can never rewind it.
func (mc *MetadataCache) AdvancePartition(topic string, partition int32, newEnd int64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	ps := mc.touchLocked(topic, partition)
	if newEnd > ps.LogEndOffset {
		ps.LogEndOffset = newEnd
		ps.HighWatermark = newEnd
	}
}

// SetPartitionState overwrites the durable position of a partition, used when
// restoring from the checkpoint or object storage.
func (mc *MetadataCache) SetPartitionState(topic string, partition int32, leo, lso int64, segments []*SegmentMetadata) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	ps := mc.touchLocked(topic, partition)
	ps.LogEndOffset = leo
	ps.HighWatermark = leo
	ps.LogStartOffset = lso
	if segments != nil {
		ps.Segments = segments
	}
}

// GetPartitions returns the known partitions for a topic. Retention and
// Metadata both use this rather than scanning the local WAL directory, so a
// partition whose segments have all been offloaded is still known to exist.
func (mc *MetadataCache) GetPartitions(topic string) []int32 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	ts, ok := mc.Topics[topic]
	if !ok {
		return nil
	}
	parts := make([]int32, 0, len(ts.Partitions))
	for p := range ts.Partitions {
		parts = append(parts, p)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	return parts
}

// LogEndOffset reports the next offset a write to the partition will take.
func (mc *MetadataCache) LogEndOffset(topic string, partition int32) int64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	ts, ok := mc.Topics[topic]
	if !ok {
		return 0
	}
	ps, ok := ts.Partitions[partition]
	if !ok {
		return 0
	}
	return ps.LogEndOffset
}

// LogStartOffset reports the oldest offset still retrievable.
func (mc *MetadataCache) LogStartOffset(topic string, partition int32) int64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	ts, ok := mc.Topics[topic]
	if !ok {
		return 0
	}
	ps, ok := ts.Partitions[partition]
	if !ok {
		return 0
	}
	return ps.LogStartOffset
}

// SegmentFor returns the key of the uploaded segment covering offset, found by
// binary search over the ordered inventory the manifest carries.
//
// It exists so a read never has to list object storage. The inventory is
// ordered oldest first, so the segment covering an offset is the last one
// whose start is at or below it. Returning "" means the inventory does not
// cover the offset, and the caller has to fall back to a listing.
//
// EndOffset is deliberately not consulted. It is -1 for any segment this
// agent cannot prove an end for, and treating that as the end of the range
// would make a reader conclude the offset falls past it and look in the wrong
// place. The start offsets alone already order the inventory unambiguously.
func (mc *MetadataCache) SegmentFor(topic string, partition int32, offset int64) string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	ts, ok := mc.Topics[topic]
	if !ok {
		return ""
	}
	ps, ok := ts.Partitions[partition]
	if !ok || len(ps.Segments) == 0 {
		return ""
	}

	segs := ps.Segments
	i := sort.Search(len(segs), func(i int) bool { return segs[i].StartOffset > offset })
	if i == 0 {
		// Every segment starts after the requested offset, so the offset
		// predates the oldest segment still on record.
		return ""
	}
	seg := segs[i-1]
	if seg == nil || seg.S3Key == "" {
		return ""
	}
	return seg.S3Key
}

// AddSegment records an uploaded segment in a partition's index.
//
// It goes into the open window, which is inline in the checkpoint. The window
// is sealed into an immutable bucket only when it fills, so the total bytes
// written for the index is O(total segments) rather than
// O(total segments x checkpoints) -- which is the whole point of the split.
// A sealed bucket is never rewritten, because the run it covers is finished by
// construction.
//
// There is exactly one entry per base offset. A re-claimed partition writes the
// same base offset again under a newer ownership epoch, and both objects exist
// in the bucket; keeping both would leave the lookup choosing between them by
// position rather than by epoch, and it would pick the superseded one.
//
// A full window is dropped rather than sealed here: sealing writes an object,
// and this holds no object store. The caller seals through SealWindow.
func (mc *MetadataCache) AddSegment(topic string, partition int32, ref SegmentRef) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	ps := mc.touchLocked(topic, partition)
	i := sort.Search(len(ps.Recent), func(i int) bool { return ps.Recent[i].Base >= ref.Base })
	if i < len(ps.Recent) && ps.Recent[i].Base == ref.Base {
		if ref.Epoch >= ps.Recent[i].Epoch {
			ps.Recent[i] = ref
		}
		return
	}
	ps.Recent = append(ps.Recent, SegmentRef{})
	copy(ps.Recent[i+1:], ps.Recent[i:])
	ps.Recent[i] = ref
}

// SealWindow moves a full open window into an immutable bucket and returns the
// bucket with the segments it covers, or nil when the window is not yet full.
// The caller persists the returned segments under the returned key.
//
// Oldest first, and the window holds a contiguous run, so the bucket's range is
// simply its first and last entry.
func (mc *MetadataCache) SealWindow(topic string, partition int32) (*BucketRef, []SegmentRef) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	ps := mc.touchLocked(topic, partition)
	if len(ps.Recent) < RecentWindow {
		return nil, nil
	}
	refs := append([]SegmentRef(nil), ps.Recent...)
	bucket := BucketRef{
		First: refs[0].Base,
		Last:  refs[len(refs)-1].Base,
		Key:   bucketKey(topic, partition, refs[0].Base, refs[len(refs)-1].Base),
	}
	ps.Sealed = append(ps.Sealed, bucket)
	ps.Recent = nil
	return &bucket, refs
}

// BucketFor returns the sealed bucket covering offset, or nil when the offset
// falls in no bucket. It is the lookup a cold read performs before fetching.
func (mc *MetadataCache) BucketFor(topic string, partition int32, offset int64) *BucketRef {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	ts, ok := mc.Topics[topic]
	if !ok {
		return nil
	}
	ps, ok := ts.Partitions[partition]
	if !ok {
		return nil
	}
	i := sort.Search(len(ps.Sealed), func(i int) bool { return ps.Sealed[i].Last >= offset })
	if i >= len(ps.Sealed) || ps.Sealed[i].First > offset {
		return nil
	}
	b := ps.Sealed[i]
	return &b
}

// RecentFor returns the segment in the open window covering offset, or nil.
// A hit here costs nothing: the window is already in memory.
func (mc *MetadataCache) RecentFor(topic string, partition int32, offset int64) *SegmentRef {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	ts, ok := mc.Topics[topic]
	if !ok {
		return nil
	}
	ps, ok := ts.Partitions[partition]
	if !ok || len(ps.Recent) == 0 {
		return nil
	}
	i := sort.Search(len(ps.Recent), func(i int) bool { return ps.Recent[i].Base > offset })
	if i == 0 {
		return nil
	}
	ref := ps.Recent[i-1]
	return &ref
}

// LoadBucket installs a fetched bucket's segments into the open window's
// sibling lookup, by merging them into Recent.
//
// They are merged rather than kept apart because the read path does a single
// binary search, and because a bucket covering the tail is contiguous with the
// window anyway. Segments the window already holds win, so a bucket fetched
// twice does not undo newer entries.
func (mc *MetadataCache) LoadBucket(topic string, partition int32, refs []SegmentRef) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	ps := mc.touchLocked(topic, partition)
	merged := make([]SegmentRef, 0, len(ps.Recent)+len(refs))
	merged = append(merged, ps.Recent...)
	merged = append(merged, refs...)
	sort.Slice(merged, func(i, j int) bool { return merged[i].Base < merged[j].Base })

	out := merged[:0]
	for i, r := range merged {
		if i > 0 && merged[i-1].Base == r.Base {
			// The window entry is newer, so it is the one already present.
			continue
		}
		out = append(out, r)
	}
	ps.Recent = out
}

// DropSegmentsBefore removes index entries covering offsets below start, and
// reports the sealed buckets that became entirely reclaimable so the caller
// can delete them.
//
// Retention deletes whole buckets rather than editing them, so this is O(buckets
// dropped) plus O(window trimmed) rather than a scan of the whole index.
func (mc *MetadataCache) DropSegmentsBefore(topic string, partition int32, start int64) []BucketRef {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	ts, ok := mc.Topics[topic]
	if !ok {
		return nil
	}
	ps, ok := ts.Partitions[partition]
	if !ok {
		return nil
	}

	var dropped []BucketRef
	keep := ps.Sealed[:0]
	for _, b := range ps.Sealed {
		if b.Last < start {
			dropped = append(dropped, b)
			continue
		}
		keep = append(keep, b)
	}
	ps.Sealed = keep

	trim := sort.Search(len(ps.Recent), func(i int) bool { return ps.Recent[i].Base >= start })
	ps.Recent = append([]SegmentRef(nil), ps.Recent[trim:]...)
	return dropped
}

// segmentEpoch reads the ownership epoch out of a segment key, reporting -1
// when the key does not carry one.
func segmentEpoch(key string) int64 {
	if _, epoch, ok := parseSegmentKey(key); ok {
		return epoch
	}
	return -1
}

// segmentKey renders the object key a segment reference names.
//
// The name is built with wal.SegmentName rather than formatted here, because
// that is what the writer used and the two have to agree exactly: an epoch of
// zero or less omits the suffix entirely, so rendering "-e0" would point at an
// object that does not exist for every pre-ownership segment. Deriving the key
// is what keeps an index entry at 16 bytes, and it is only safe because the
// name is a pure function of these two numbers.
func segmentKey(topic string, partition int32, ref SegmentRef) string {
	return fmt.Sprintf("%s/%d/%s", topic, partition, wal.SegmentName(ref.Base, ref.Epoch))
}

// bucketKey renders a sealed bucket's key from the range it covers.
func bucketKey(topic string, partition int32, first, last int64) string {
	return fmt.Sprintf("%s/%d/%020d-%020d.idx", topic, partition, first, last)
}

// TopicsSnapshot returns a deep copy of the durable state, suitable for
// serialising without holding the lock across I/O.
func (mc *MetadataCache) TopicsSnapshot() map[string]map[int32]*PartitionState {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	out := make(map[string]map[int32]*PartitionState, len(mc.Topics))
	for name, ts := range mc.Topics {
		parts := make(map[int32]*PartitionState, len(ts.Partitions))
		for pid, ps := range ts.Partitions {
			clone := *ps
			clone.Segments = make([]*SegmentMetadata, len(ps.Segments))
			for i, seg := range ps.Segments {
				c := *seg
				clone.Segments[i] = &c
			}
			parts[pid] = &clone
		}
		out[name] = parts
	}
	return out
}

func (mc *MetadataCache) RemoveTopic(topic string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.Topics, topic)
}

func (mc *MetadataCache) RemovePartition(topic string, partition int32) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	ts, ok := mc.Topics[topic]
	if !ok {
		return
	}
	delete(ts.Partitions, partition)
	if len(ts.Partitions) == 0 {
		delete(mc.Topics, topic)
	}
}

func (mc *MetadataCache) ToJSON() ([]byte, error) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return json.Marshal(mc)
}

func (mc *MetadataCache) FromJSON(data []byte) error {
	// Decode into the cache's own type rather than a hand-written subset.
	// A subset silently drops whatever it forgot to declare, which is how
	// committed offsets came to be written on every checkpoint and read back
	// on none.
	var state MetadataCache
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	if state.Topics == nil {
		state.Topics = make(map[string]*TopicState)
	}
	for _, ts := range state.Topics {
		if ts.Partitions == nil {
			ts.Partitions = make(map[int32]*PartitionState)
		}
		for _, ps := range ts.Partitions {
			if ps.Segments == nil {
				ps.Segments = make([]*SegmentMetadata, 0)
			}
		}
	}
	mc.Topics = state.Topics
	mc.Committed = state.Committed
	mc.WriterEpoch = state.WriterEpoch
	mc.Writer = state.Writer
	return nil
}

// SeededBucket installs a bucket into the index as if it had been sealed and
// written. It exists for tests and for recovery, which reads bucket objects
// back rather than reconstructing them.
func (mc *MetadataCache) SeededBucket(topic string, partition int32, b BucketRef, body []byte) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	ps := mc.touchLocked(topic, partition)
	ps.Sealed = append(ps.Sealed, b)
	sort.Slice(ps.Sealed, func(i, j int) bool { return ps.Sealed[i].Last < ps.Sealed[j].Last })
	if len(body) > 0 {
		var refs []SegmentRef
		if err := json.Unmarshal(body, &refs); err == nil {
			ps.Recent = append(ps.Recent, refs...)
			sort.Slice(ps.Recent, func(i, j int) bool { return ps.Recent[i].Base < ps.Recent[j].Base })
		}
	}
}
