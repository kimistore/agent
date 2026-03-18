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
	"strconv"
	"sync"

	"kimistore/internal/coordinator"
)

type MetadataCache struct {
	Topics      map[string]*TopicState          `json:"topics"`
	Coordinator *coordinator.CoordinatorState `json:"coordinator,omitempty"`
	mu          sync.RWMutex
}

type TopicState struct {
	Partitions map[int32]*PartitionState `json:"partitions"`
}

type PartitionState struct {
	// Current HW, LE (Log End Offset)
	HighWatermark int64 `json:"high_watermark"`
	LogEndOffset  int64 `json:"log_end_offset"`

	// Sorted list of segments for fast binary search
	// We will populate this later in Phase 1 or 2
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

func (mc *MetadataCache) Load(walDir string) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Scan WAL directory
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		topicName := entry.Name()
		if topicName == "tmp-wal-test" || topicName == "tmp-storage-test-wal" {
			continue
		}

		// Add Topic
		if mc.Topics[topicName] == nil {
			mc.Topics[topicName] = &TopicState{
				Partitions: make(map[int32]*PartitionState),
			}
		}

		// Scan Partitions
		partitionEntries, err := os.ReadDir(filepath.Join(walDir, topicName))
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
			partitionID := int32(pID)

			if mc.Topics[topicName].Partitions[partitionID] == nil {
				mc.Topics[topicName].Partitions[partitionID] = &PartitionState{
					HighWatermark: 0,
					LogEndOffset:  0,
					Segments:      make([]*SegmentMetadata, 0),
				}
			}
		}
	}
	return nil
}

// Accessors

func (mc *MetadataCache) GetTopics() []string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	topics := make([]string, 0, len(mc.Topics))
	for t := range mc.Topics {
		topics = append(topics, t)
	}
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
	if _, ok := mc.Topics[topic]; !ok {
		mc.Topics[topic] = &TopicState{
			Partitions: make(map[int32]*PartitionState),
		}
	}
}

func (mc *MetadataCache) AddPartition(topic string, partition int32) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	// Ensure Topics map is initialized
	if mc.Topics == nil {
		mc.Topics = make(map[string]*TopicState)
	}
	if _, ok := mc.Topics[topic]; !ok {
		mc.Topics[topic] = &TopicState{
			Partitions: make(map[int32]*PartitionState),
		}
	}
	// Ensure Partitions map is initialized (may be nil after JSON unmarshal)
	if mc.Topics[topic].Partitions == nil {
		mc.Topics[topic].Partitions = make(map[int32]*PartitionState)
	}
	if _, ok := mc.Topics[topic].Partitions[partition]; !ok {
		mc.Topics[topic].Partitions[partition] = &PartitionState{
			Segments: make([]*SegmentMetadata, 0),
		}
	}
}

func (mc *MetadataCache) RemoveTopic(topic string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	delete(mc.Topics, topic)
}

func (mc *MetadataCache) ToJSON() ([]byte, error) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return json.Marshal(mc)
}

func (mc *MetadataCache) FromJSON(data []byte) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	var state struct {
		Topics      map[string]*TopicState          `json:"topics"`
		Coordinator *coordinator.CoordinatorState `json:"coordinator"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	// Ensure Topics map is never nil
	if state.Topics == nil {
		state.Topics = make(map[string]*TopicState)
	}
	mc.Topics = state.Topics
	mc.Coordinator = state.Coordinator
	return nil
}
