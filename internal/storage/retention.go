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
	"kimistore/internal/storage/wal"
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
			topicsChecked++

			// The consumer bound comes from committed offsets and costs
			// nothing, so it is reclaimed straight off the index. The size and
			// time policies still need a listing: they select on segment size
			// and last-modified time, and the index carries neither. That is a
			// separate change -- putting them in SegmentRef -- not something to
			// smuggle in here, so this path keeps its listing and reports when
			// it cannot complete.
			if s.retentionCfg.RetentionTime > 0 || s.retentionCfg.RetentionBytes > 0 {
				segmentsDeleted += s.reclaimByPolicy(ctx, topic, partition)
			}
			segmentsDeleted += s.reclaimByIndex(ctx, topic, partition)
		}
	}

	log.Printf("Retention: sweep complete (%d partition(s) checked, %d segment(s) deleted)",
		topicsChecked, segmentsDeleted)
}

func partitionPrefix(topic string, partition int32) string {
	return topic + "/" + strconv.Itoa(int(partition)) + "/"
}

// filterSegments keeps only log segments under this partition, dropping the
// index sidecars that live alongside them, and orders them oldest first.
//
// The order is by base offset and then by descending epoch, so that at a base
// offset a superseded owner's segment sorts before the one that replaced it and
// the retention sweep treats the newer epoch as the one that follows.
func filterSegments(objects []ObjectMetadata) []ObjectMetadata {
	out := make([]ObjectMetadata, 0, len(objects))
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".log") {
			out = append(out, obj)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		oi, ei, _ := parseSegmentKey(out[i].Key)
		oj, ej, _ := parseSegmentKey(out[j].Key)
		if oi != oj {
			return oi < oj
		}
		return ei > ej
	})
	return out
}

// filterLiveSegments drops the segments a superseded owner left behind.
//
// Putting the ownership epoch in the segment key is what stops a stale writer's
// upload from overwriting its successor's, but it also means both uploads can
// exist. Only the highest epoch at a given base offset is part of the log: the
// stale one is unreachable, because reads resolve to the highest epoch and the
// manifest records the position the current owner advanced to. Keeping the stale
// copies would make the segment inventory describe a log that never existed, and
// would let retention reason about offsets twice.
func filterLiveSegments(segments []ObjectMetadata) []ObjectMetadata {
	live := make([]ObjectMetadata, 0, len(segments))
	for i := 0; i < len(segments); {
		j := i + 1
		for j < len(segments) && parseOffsetFromKey(segments[j].Key) == parseOffsetFromKey(segments[i].Key) {
			j++
		}
		// filterSegments puts the highest epoch first within one offset.
		live = append(live, segments[i])
		i = j
	}
	return live
}

// cacheKeyFor is the segment-cache key for a partition.
func cacheKeyFor(topic string, partition int32) string {
	return topic + "/" + strconv.Itoa(int(partition))
}

// staleEpochSegments returns the segments at a base offset that a higher epoch
// has superseded. They are never the answer to a read and never appear in a
// manifest, so holding them costs storage and nothing else.
func staleEpochSegments(segments []ObjectMetadata) []ObjectMetadata {
	live := make(map[string]bool)
	for _, seg := range filterLiveSegments(segments) {
		live[seg.Key] = true
	}
	var stale []ObjectMetadata
	for _, seg := range segments {
		if !live[seg.Key] {
			stale = append(stale, seg)
		}
	}
	return stale
}

// deleteStaleEpochSegments removes a superseded segment and its index sidecar,
// and reports how many segment objects went.
func (s *StorageEngine) deleteStaleEpochSegments(ctx context.Context, cacheKey string, stale []ObjectMetadata) int {
	deleted := 0
	for _, seg := range stale {
		keys := []string{seg.Key, strings.TrimSuffix(seg.Key, ".log") + ".index"}
		failed := false
		for _, key := range keys {
			if err := s.objDelete(ctx, key); err != nil {
				log.Printf("Retention: failed to delete superseded segment object %s: %v", key, err)
				failed = true
			}
		}
		if failed {
			continue
		}
		deleted++
		metrics.RetentionSegmentsDeleted.Inc()
		log.Printf("Retention: deleted %s, superseded by a newer ownership epoch", seg.Key)
	}
	if deleted > 0 {
		s.invalidateCache(cacheKey)
	}
	return deleted
}

// reclaimPartition reclaims everything a sweep may remove from one partition,
// and returns how many segment objects went.
//
// It has two parts. Superseded-epoch segments go first and unconditionally: a
// segment the current owner replaced is unreachable garbage rather than retained
// data, so there is no reason to hold it until a consumer advances past offsets
// it will never be asked for. Retention then applies its own policies to the
// live log only, where the offset arithmetic is sound.
func (s *StorageEngine) reclaimPartition(ctx context.Context, topic string, partition int32, segments []ObjectMetadata) int {
	deleted := 0
	if stale := staleEpochSegments(segments); len(stale) > 0 {
		deleted += s.deleteStaleEpochSegments(ctx, cacheKeyFor(topic, partition), stale)
	}
	return deleted + s.processPartitionRetention(ctx, topic, partition, filterLiveSegments(segments))
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

	cacheKey := cacheKeyFor(topic, partition)
	deleted := 0
	for key := range toDelete {
		// The index sidecar goes with the segment it describes; leaving it
		// behind would be an object nothing can ever read again.
		for _, k := range []string{key, strings.TrimSuffix(key, ".log") + ".index"} {
			if err := s.objDelete(ctx, k); err != nil {
				log.Printf("Retention: failed to delete %s: %v", k, err)
			}
		}
		deleted++
		metrics.RetentionSegmentsDeleted.Inc()
	}

	if deleted > 0 {
		// The cached key list is now stale, and the index sidecars for the
		// deleted segments are gone with them.
		s.invalidateCache(cacheKey)
		s.markManifestDirty(topic, partition)
	}
	return deleted
}

// oldestIndexedOffset reports the base offset of the oldest segment the index
// still holds, or -1 when it holds none. It replaces a listing to answer the
// same question about what remains.
func (s *StorageEngine) oldestIndexedOffset(topic string, partition int32) (int64, bool) {
	return s.metadataCache.OldestIndexed(topic, partition)
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

// parseOffsetFromKey extracts the start offset encoded in a segment name,
// ignoring the ownership epoch a segment written under partition ownership
// carries ("..../<offset>-e<epoch>.log"). Offsets are never negative, so an
// unparseable name is reported as no offset rather than a plausible one.
func parseOffsetFromKey(key string) int64 {
	off, _, _ := parseSegmentKey(key)
	return off
}

// parseSegmentKey splits a segment object key into its base offset and the
// ownership epoch that wrote it. A segment written before partition ownership
// has no epoch suffix and parses as epoch 0.
func parseSegmentKey(key string) (baseOffset, epoch int64, ok bool) {
	parts := strings.Split(key, "/")
	return wal.ParseSegmentName(parts[len(parts)-1])
}

// reclaimByIndex removes everything below the lowest offset any consumer still
// needs, driving from the index rather than from a listing.
//
// A listing here is the operation that does not scale: it grows with the
// partition's whole history, and on knode3 it could not complete inside the
// operation timeout, so the sweep logged a failure and reclaimed nothing on
// every pass.
func (s *StorageEngine) reclaimByIndex(ctx context.Context, topic string, partition int32) int {
	firstAllowed := s.FirstAllowedOffset(topic, partition)
	if firstAllowed < 0 {
		return 0 // nothing has committed against this partition yet
	}

	// An index that holds nothing may mean the partition is empty, or that its
	// segments predate the index. Those need different handling: reclaiming
	// nothing when segments are present leaks storage silently and forever, so
	// the listing is used as a fallback. It is bounded by how much unindexed
	// data there is, which is the legacy case and does not grow.
	if _, ok := s.oldestIndexedOffset(topic, partition); !ok {
		return s.reclaimUnindexed(ctx, topic, partition, firstAllowed)
	}

	deleted := s.reclaimIndex(ctx, topic, partition, firstAllowed)
	if deleted == 0 {
		return 0
	}

	// The log start has moved. ListOffsets has to report it, otherwise a
	// consumer that resets to "earliest" is sent to an offset that no longer
	// exists and loops on OffsetOutOfRange forever.
	remaining, _ := s.oldestIndexedOffset(topic, partition)
	if remaining < 0 {
		remaining = s.metadataCache.LogEndOffset(topic, partition)
	}
	s.metadataCache.SetPartitionState(topic, partition,
		s.metadataCache.LogEndOffset(topic, partition), remaining)
	s.markManifestDirty(topic, partition)
	log.Printf("Retention: reclaimed %d segment(s) from %s via the index; log start now %d",
		deleted, cacheKeyFor(topic, partition), remaining)
	return deleted
}

// reclaimUnindexed reclaims a partition the index knows nothing about, by
// listing it. This is the legacy path: segments written before the index
// existed, or before this agent saw them uploaded.
func (s *StorageEngine) reclaimUnindexed(ctx context.Context, topic string, partition int32, firstAllowed int64) int {
	segments, err := s.objectSegments(ctx, topic, partition)
	if err != nil {
		log.Printf("Retention: cannot list %s/%d to reclaim it: %v", topic, partition, err)
		return 0
	}
	if len(segments) == 0 {
		return 0
	}

	// Same rule the index path uses: a segment is reclaimable only when its
	// successor starts at or below the bound, which is what proves every offset
	// inside it is consumed. The newest has no successor and is kept.
	drop := make([]*SegmentMetadata, 0, len(segments))
	for i := 0; i+1 < len(segments); i++ {
		if segments[i+1].StartOffset <= firstAllowed {
			drop = append(drop, segments[i])
		}
	}
	if len(drop) == 0 {
		return 0
	}

	deleted := 0
	for _, seg := range drop {
		key := seg.S3Key
		ok := true
		for _, k := range []string{key, strings.TrimSuffix(key, ".log") + ".index"} {
			if err := s.objDelete(ctx, k); err != nil {
				log.Printf("Retention: failed to delete %s: %v", k, err)
				ok = false
			}
		}
		if ok {
			deleted++
			metrics.RetentionSegmentsDeleted.Inc()
		}
	}
	if deleted == 0 {
		return 0
	}

	remaining := segments[0].StartOffset
	if len(drop) == len(segments)-1 {
		remaining = s.metadataCache.LogEndOffset(topic, partition)
	} else if len(drop) < len(segments) {
		remaining = segments[len(drop)].StartOffset
	}
	s.metadataCache.SetPartitionState(topic, partition,
		s.metadataCache.LogEndOffset(topic, partition), remaining)
	s.markManifestDirty(topic, partition)
	log.Printf("Retention: reclaimed %d unindexed segment(s) from %s/%d; log start now %d",
		deleted, topic, partition, remaining)
	return deleted
}

// reclaimByPolicy applies the size and time retention policies, which select on
// segment size and last-modified time. The index carries neither, so this still
// lists the partition.
//
// It is kept separate from reclaimByIndex on purpose: the consumer bound is
// reclaimed listing-free on every sweep, so a partition whose listing cannot
// complete still loses the data no consumer needs. This path degrades on its
// own rather than taking the other down with it.
func (s *StorageEngine) reclaimByPolicy(ctx context.Context, topic string, partition int32) int {
	objects, err := s.objStore.List(ctx, partitionPrefix(topic, partition))
	if err != nil {
		log.Printf("Retention: size/time policy cannot list %s/%d: %v", topic, partition, err)
		return 0
	}
	segments := filterSegments(objects)
	if len(segments) == 0 {
		return 0
	}
	return s.reclaimPartition(ctx, topic, partition, segments)
}
