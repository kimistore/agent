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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
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
}

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
