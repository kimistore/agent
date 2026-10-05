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

	"kimistore/internal/metrics"
)

// RetentionConfig controls how long segments are kept. A zero value disables
// both policies, which is why retention does nothing until configured.
type RetentionConfig struct {
	RetentionBytes int64         // Max size in bytes per partition. -1 for infinite.
	RetentionTime  time.Duration // Max age of segments. 0 for infinite.
	// CheckInterval is how often retention runs. Defaults to 5 minutes,
	// matching Kafka's retention.check.interval.ms.
	CheckInterval time.Duration
}

func (s *StorageEngine) retentionLoop() {
	defer s.wg.Done()

	interval := s.retentionCfg.CheckInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}

	ticker := time.NewTicker(interval)
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

// applyRetention enforces time and size policies.
//
// It walks the known topic/partition pairs and lists each one under its own
// prefix. The previous implementation listed the entire bucket every interval,
// which on a large or shared bucket is an expensive, throttle-prone operation
// that grows with total data rather than with the data being reclaimed.
func (s *StorageEngine) applyRetention() {
	if s.retentionCfg.RetentionTime <= 0 && s.retentionCfg.RetentionBytes <= 0 {
		return
	}

	log.Printf("Retention: starting sweep (time=%s bytes=%d)",
		s.retentionCfg.RetentionTime, s.retentionCfg.RetentionBytes)

	// Fail closed. Until consumer offsets have been read from object storage
	// we cannot tell an empty log from a log whose readers we have lost track
	// of, and deleting on that assumption loses data.
	if !s.offsetsAuthoritative() {
		log.Println("Retention: skipping sweep; consumer offsets have not been loaded from object storage yet")
		return
	}

	ctx := context.Background()
	var topicsChecked, segmentsDeleted int

	for _, topic := range s.metadataCache.GetTopics() {
		for _, partition := range s.metadataCache.GetPartitions(topic) {
			prefix := partitionPrefix(topic, partition)

			objects, err := s.objStore.List(ctx, prefix)
			if err != nil {
				log.Printf("Retention: failed to list %s: %v", prefix, err)
				continue
			}
			topicsChecked++

			segments := filterSegments(objects)
			if len(segments) == 0 {
				continue
			}

			deleted := s.processPartitionRetention(ctx, topic, partition, segments)
			segmentsDeleted += deleted
		}
	}

	log.Printf("Retention: sweep complete (%d partition(s) checked, %d segment(s) deleted)",
		topicsChecked, segmentsDeleted)
}

func partitionPrefix(topic string, partition int32) string {
	return topic + "/" + strconv.Itoa(int(partition)) + "/"
}

// filterSegments keeps only log segments under this partition, dropping the
// index sidecars that live alongside them.
func filterSegments(objects []ObjectMetadata) []ObjectMetadata {
	out := make([]ObjectMetadata, 0, len(objects))
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".log") {
			out = append(out, obj)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return parseOffsetFromKey(out[i].Key) < parseOffsetFromKey(out[j].Key)
	})
	return out
}

// processPartitionRetention deletes segments for one partition, oldest first,
// and returns how many were removed.
func (s *StorageEngine) processPartitionRetention(ctx context.Context, topic string, partition int32, segments []ObjectMetadata) int {
	// Segments are sorted oldest-first by start offset. The lowest offset any
	// consumer still needs bounds what may be reclaimed: anything at or above
	// it is still live data.
	firstAllowed := s.FirstAllowedOffset(topic, partition)

	// nextStart[i] is the start offset of the segment after segments[i], or
	// -1 when segments[i] is the newest. A segment is only safe to delete if
	// every offset inside it falls below firstAllowed, which we can only prove
	// when we know where the following segment begins.
	nextStart := make([]int64, len(segments))
	for i := range segments {
		if i+1 < len(segments) {
			nextStart[i] = parseOffsetFromKey(segments[i+1].Key)
		} else {
			nextStart[i] = -1 // newest segment: never proven fully consumed
		}
	}

	consumable := func(i int) bool {
		ns := nextStart[i]
		if ns < 0 {
			// The newest segment: we have no next start offset, so we cannot
			// prove every offset in it is below firstAllowed. Kept, matching
			// Kafka's treatment of the active segment.
			return false
		}
		if firstAllowed < 0 {
			// No consumer has committed against this partition, so nothing is
			// being actively read and the normal policies apply unconstrained.
			return true
		}
		// Every offset in this segment must sit below the lowest offset any
		// consumer still needs.
		return ns <= firstAllowed
	}

	var candidates []ObjectMetadata
	for i := range segments {
		if consumable(i) {
			candidates = append(candidates, segments[i])
		}
	}

	toDelete := make(map[string]bool)

	// --- Time based retention ---
	if s.retentionCfg.RetentionTime > 0 {
		cutoff := time.Now().Add(-s.retentionCfg.RetentionTime).Unix()
		for _, seg := range candidates {
			if seg.LastModified < cutoff {
				toDelete[seg.Key] = true
			}
		}
	}

	// --- Size based retention ---
	// Deletion is driven by the candidates that survive the time policy, so a
	// segment held back for live consumers does not eat into the budget.
	if s.retentionCfg.RetentionBytes > 0 {
		var total int64
		remaining := make([]ObjectMetadata, 0, len(candidates))
		for _, seg := range segments {
			total += seg.Size
		}
		for _, seg := range candidates {
			if toDelete[seg.Key] {
				continue
			}
			remaining = append(remaining, seg)
		}
		for _, seg := range remaining {
			if total <= s.retentionCfg.RetentionBytes {
				break
			}
			toDelete[seg.Key] = true
			total -= seg.Size
		}
	}

	if len(toDelete) == 0 {
		return 0
	}

	cacheKey := topic + "/" + strconv.Itoa(int(partition))
	deleted := 0
	for key := range toDelete {
		if err := s.objStore.Delete(ctx, key); err != nil {
			log.Printf("Retention: failed to delete %s: %v", key, err)
			continue
		}
		deleted++
		metrics.RetentionSegmentsDeleted.Inc()
	}

	if deleted > 0 {
		// The cached key list is now stale, and the index sidecars for the
		// deleted segments are gone with them.
		s.cacheMu.Lock()
		delete(s.segmentCache, cacheKey)
		s.cacheMu.Unlock()

		// The log start has moved. ListOffsets has to report it, otherwise a
		// consumer that resets to "earliest" is sent to an offset that no
		// longer exists and loops on OffsetOutOfRange forever.
		remaining := s.oldestRetainedOffset(ctx, topic, partition)
		s.metadataCache.SetPartitionState(topic, partition,
			s.metadataCache.LogEndOffset(topic, partition), remaining, s.currentSegments(ctx, topic, partition))
		log.Printf("Retention: deleted %d segment(s) from %s; log start now %d", deleted, cacheKey, remaining)
	}
	return deleted
}

// oldestRetainedOffset reports the start offset of the oldest segment still
// present, or the log end offset if the partition has been fully reclaimed.
func (s *StorageEngine) oldestRetainedOffset(ctx context.Context, topic string, partition int32) int64 {
	segments := s.currentSegments(ctx, topic, partition)
	if len(segments) == 0 {
		return s.metadataCache.LogEndOffset(topic, partition)
	}
	return segments[0].StartOffset
}

func (s *StorageEngine) currentSegments(ctx context.Context, topic string, partition int32) []*SegmentMetadata {
	segs, err := s.objectSegments(ctx, topic, partition)
	if err != nil {
		return nil
	}
	return segs
}

// FirstAllowedOffset is the lowest offset still needed by any consumer group,
// or -1 when no group has committed against the partition. Retention must not
// reclaim data at or above it.
//
// The minimum is computed across groups on each call rather than cached, so it
// rises as the slowest consumer advances instead of pinning retention to
// whatever offset it held when it first committed.
func (s *StorageEngine) FirstAllowedOffset(topic string, partition int32) int64 {
	tp := topic + "/" + strconv.Itoa(int(partition))

	s.committedMu.Lock()
	defer s.committedMu.Unlock()

	lowest := int64(-1)
	for _, byPartition := range s.committedByGroup {
		off, ok := byPartition[tp]
		if !ok {
			continue
		}
		if lowest < 0 || off < lowest {
			lowest = off
		}
	}
	return lowest
}

// offsetsAuthoritative reports whether consumer offsets have been loaded, which
// is the precondition for reclaiming anything.
func (s *StorageEngine) offsetsAuthoritative() bool {
	s.committedMu.Lock()
	defer s.committedMu.Unlock()
	return s.committedAuthoritative
}

// parseOffsetFromKey extracts the start offset encoded in "..../<offset>.log".
func parseOffsetFromKey(key string) int64 {
	parts := strings.Split(key, "/")
	filename := parts[len(parts)-1]
	baseName := strings.TrimSuffix(filename, ".log")
	off, _ := strconv.ParseInt(baseName, 10, 64)
	return off
}
