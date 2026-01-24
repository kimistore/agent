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
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

type RetentionConfig struct {
	RetentionBytes int64         // Max size in bytes per partition. -1 for infinite.
	RetentionTime  time.Duration // Max age of segments. 0 for infinite.
}

func (s *StorageEngine) retentionLoop() {
	defer s.wg.Done()
	// Kafka defaults to 5 minutes check interval
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			s.applyRetention()
		}
	}
}

func (s *StorageEngine) applyRetention() {
	log.Println("Starting retention check...")
	ctx := context.Background()

	// 1. List all objects (MVP: simple scan of entire bucket)
	// Optimization for later: Use Delimiter to list partitions or only scan known partitions
	objects, err := s.objStore.List(ctx, "")
	if err != nil {
		log.Printf("Retention: failed to list objects: %v", err)
		return
	}

	// 2. Group by partition: "topic/partition"
	grouped := make(map[string][]ObjectMetadata)
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".log") {
			continue
		}
		// Key format: topic/partition/offset.log
		parts := strings.Split(obj.Key, "/")
		if len(parts) < 3 {
			continue
		}
		// Extract "topic/partition"
		// Handle nested paths if any? Assuming standard topic/partition/segment
		// topic is parts[0], partition is parts[1]
		// key = topic + "/" + partition
		groupKey := parts[0] + "/" + parts[1]
		grouped[groupKey] = append(grouped[groupKey], obj)
	}

	// 3. Process each partition
	for groupKey, segments := range grouped {
		s.processPartitionRetention(ctx, groupKey, segments)
	}

	log.Println("Retention check completed.")
}

func (s *StorageEngine) processPartitionRetention(ctx context.Context, groupKey string, segments []ObjectMetadata) {
	// Sort segments by offset (filename)
	sort.Slice(segments, func(i, j int) bool {
		return parseOffsetFromKey(segments[i].Key) < parseOffsetFromKey(segments[j].Key)
	})

	var toDelete []string

	// --- Time Based Retention ---
	if s.retentionCfg.RetentionTime > 0 {
		now := time.Now().Unix()
		cutoff := now - int64(s.retentionCfg.RetentionTime.Seconds())

		for _, seg := range segments {
			// If LastModified < cutoff, delete
			if seg.LastModified < cutoff {
				toDelete = append(toDelete, seg.Key)
			}
		}
	}

	// Filter out already marked for deletion to calculate size
	var remaining []ObjectMetadata
	deletedSet := make(map[string]bool)
	for _, key := range toDelete {
		deletedSet[key] = true
	}
	for _, seg := range segments {
		if !deletedSet[seg.Key] {
			remaining = append(remaining, seg)
		}
	}

	// --- Size Based Retention ---
	if s.retentionCfg.RetentionBytes > 0 {
		var totalSize int64
		for _, seg := range remaining {
			totalSize += seg.Size
		}

		// Delete from oldest (start of slice) until totalSize <= Limit
		for i := 0; i < len(remaining); i++ {
			if totalSize <= s.retentionCfg.RetentionBytes {
				break
			}
			seg := remaining[i]
			toDelete = append(toDelete, seg.Key)
			totalSize -= seg.Size
		}
	}

	// Execute Deletion
	if len(toDelete) > 0 {
		log.Printf("Retention: deleting %d segments for %s", len(toDelete), groupKey)
		for _, key := range toDelete {
			if err := s.objStore.Delete(ctx, key); err != nil {
				log.Printf("Retention: failed to delete %s: %v", key, err)
			}
		}

		// Invalidate Cache for this partition
		s.cacheMu.Lock()
		delete(s.segmentCache, groupKey)
		s.cacheMu.Unlock()
	}
}

func parseOffsetFromKey(key string) int64 {
	parts := strings.Split(key, "/")
	filename := parts[len(parts)-1]
	baseName := strings.TrimSuffix(filename, ".log")
	off, _ := strconv.ParseInt(baseName, 10, 64)
	return off
}
