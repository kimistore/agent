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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"kimistore/internal/metrics"
)

// The durable log position used to live in one bucket-global object,
// _meta/manifest.json. That works for exactly one writer: every rewrite puts
// the whole topic inventory back, so a second agent -- or a future owner of
// only part of the log -- would replace another writer's partitions with its
// own view of them. It is also a single object rewritten in full on every
// checkpoint, which does not scale with the partition count.
//
// Phase 0 splits it into one object per partition under a reserved _topics/
// prefix. An owner writes only the partitions it is responsible for, recovery
// discovers partitions with one bounded LIST instead of inferring them from
// segment objects, and a failed write affects one partition rather than the
// whole log position.
const topicsMetadataPrefix = "_topics/"

// partitionManifestKey is where one partition's durable position lives.
func partitionManifestKey(topic string, partition int32) string {
	return fmt.Sprintf("%s%s/_manifest/%d", topicsMetadataPrefix, topic, partition)
}

// parsePartitionManifestKey splits "_topics/<topic>/_manifest/<partition>".
// A Kafka topic name cannot contain '/', so the shape is unambiguous.
func parsePartitionManifestKey(key string) (topic string, partition int32, ok bool) {
	rest := strings.TrimPrefix(key, topicsMetadataPrefix)
	if rest == key {
		return "", 0, false
	}
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) != 3 || parts[1] != "_manifest" {
		return "", 0, false
	}
	p, err := strconv.ParseInt(parts[2], 10, 32)
	if err != nil {
		return "", 0, false
	}
	return parts[0], int32(p), true
}

// perPartitionManifest is the durable position of one partition, including the
// epoch that makes it possible to tell a stale writer's record from a current
// one.
//
// With partition ownership that is the per-partition ownership epoch, stamped
// from the claim this agent holds rather than from the bucket-global lease.
// Epoch is the newer name for it; WriterEpoch is what pre-ownership agents
// wrote and is still read so an upgrade recovers the same position.
type perPartitionManifest struct {
	Epoch          int64              `json:"epoch,omitempty"`
	WriterEpoch    int64              `json:"writer_epoch,omitempty"`
	Writer         string             `json:"writer,omitempty"`
	Topic          string             `json:"topic"`
	Partition      int32              `json:"partition"`
	LogEndOffset   int64              `json:"log_end_offset"`
	LogStartOffset int64              `json:"log_start_offset"`
	Segments       []*SegmentMetadata `json:"segments,omitempty"`
}

// storedEpoch is the epoch this manifest was written under, accepting either
// field so a manifest from before per-partition ownership still fences.
func (m perPartitionManifest) storedEpoch() int64 {
	if m.Epoch > 0 {
		return m.Epoch
	}
	return m.WriterEpoch
}

// savePartitionManifests writes the durable position of every partition whose
// state has changed since the last save.
//
// It is deliberately incremental. The previous whole-log manifest was one PUT
// per checkpoint; writing every partition's object on every checkpoint would
// turn that into one PUT per partition, which is the opposite of the scaling
// this change exists to enable. A partition is marked dirty whenever its end
// offset, start offset or segment inventory moves, and the mark is cleared
// only after its object lands (and only if nothing marked it dirty again while
// the write was in flight).
//
// On the first save after a cold start every known partition is written once,
// so a partition recovered from segment inventory gains a manifest of its own.
func (s *StorageEngine) savePartitionManifests(ctx context.Context) error {
	snapshot := s.metadataCache.TopicsSnapshot()

	s.manifestMu.Lock()
	dirty := make(map[string]uint64, len(s.manifestDirty))
	for k, gen := range s.manifestDirty {
		dirty[k] = gen
	}
	firstSave := !s.manifestSaved.Load()
	s.manifestMu.Unlock()

	if firstSave {
		for topic, parts := range snapshot {
			for pid := range parts {
				key := topic + "/" + strconv.Itoa(int(pid))
				if _, ok := dirty[key]; !ok {
					dirty[key] = 0
				}
			}
		}
	}

	written := 0
	var firstErr error

	for topic, parts := range snapshot {
		for pid, ps := range parts {
			key := topic + "/" + strconv.Itoa(int(pid))
			gen, ok := dirty[key]
			if !ok {
				continue
			}

			// Never write a partition this agent does not own. Under ownership
			// the record would be stamped with an epoch it has no claim for, and
			// the real owner would then read a position it did not write.
			if s.ownership != nil && !s.ownership.Owns(topic, pid) {
				continue
			}

			// The check above is a local belief, and a local belief goes stale:
			// the claim can be taken the instant it ages out, and this agent
			// learns of it at its next renewal, up to a third of a TTL later.
			// The manifest is an unconditional PUT, so a stale writer that
			// skipped this would overwrite the new owner's record with its own
			// lower epoch and log end.
			//
			// Asking the store costs one read per dirty partition per save. The
			// save runs on a 30-second timer, not on the append path, so the
			// read is affordable and the alternative is two writers.
			if s.ownership != nil && s.ownership.fenced {
				if err := s.ownership.verifyClaim(ctx, topic, pid); err != nil {
					log.Printf("Manifest: not writing %s/%d: %v", topic, pid, err)
					metrics.ManifestWritesRejected.Inc()
					// Drop it from the dirty set too: it is no longer ours to
					// write, so retrying on the next tick would fail the same
					// way for as long as this agent runs.
					s.clearManifestDirty(topic, pid)
					continue
				}
			}

			segments := ps.Segments
			if segments == nil {
				segments = []*SegmentMetadata{}
			}
			data, err := json.MarshalIndent(perPartitionManifest{
				Epoch:          s.partitionEpoch(topic, pid),
				WriterEpoch:    s.lease.Epoch(),
				Writer:         s.partitionWriter(topic, pid),
				Topic:          topic,
				Partition:      pid,
				LogEndOffset:   ps.LogEndOffset,
				LogStartOffset: ps.LogStartOffset,
				Segments:       segments,
			}, "", "  ")
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}

			if err := s.objPut(ctx, partitionManifestKey(topic, pid), bytes.NewReader(data)); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}

			// Clear the mark only if this write is still the newest one. An
			// append during the PUT bumps the generation, and leaving the
			// partition dirty is what gets the newer position written next.
			if gen > 0 {
				s.manifestMu.Lock()
				if s.manifestDirty[key] == gen {
					delete(s.manifestDirty, key)
				}
				s.manifestMu.Unlock()
			}
			written++
		}
	}

	// A first save that did not finish must be retried in full, otherwise a
	// partition whose object failed to land would never be written again.
	if firstErr == nil {
		s.manifestSaved.Store(true)
	}
	if written > 0 {
		log.Printf("Saved %d per-partition manifest(s)", written)
	}
	return firstErr
}

// loadPartitionManifests restores log positions from the per-partition
// manifests. It returns how many partitions were restored and whether any
// manifest object was found at all, so recovery can fall back to the legacy
// global manifest on a bucket written by an older agent.
func (s *StorageEngine) loadPartitionManifests(ctx context.Context) (restored int, found bool, err error) {
	objects, err := s.objList(ctx, topicsMetadataPrefix)
	if err != nil {
		return 0, false, err
	}

	for _, obj := range objects {
		topic, pid, ok := parsePartitionManifestKey(obj.Key)
		if !ok {
			continue
		}
		found = true

		// A manifest found by LIST may name a partition the checkpoint did not
		// know about, so claim it here too. Claiming before comparing epochs is
		// what makes the comparison meaningful: our epoch is the newest one
		// anyone can have written under.
		if s.ownership != nil && !s.ownership.Owns(topic, pid) {
			if _, claimErr := s.claimPartition(ctx, topic, pid); claimErr != nil {
				log.Printf("Ownership: not claiming %s/%d: %v", topic, pid, claimErr)
				continue
			}
		}

		rc, getErr := s.objGet(ctx, obj.Key)
		if getErr != nil {
			log.Printf("Manifest %s is unreadable (%v); continuing with the others", obj.Key, getErr)
			continue
		}
		data, readErr := io.ReadAll(rc)
		_ = rc.Close()
		if readErr != nil {
			log.Printf("Manifest %s is unreadable (%v); continuing with the others", obj.Key, readErr)
			continue
		}

		var m perPartitionManifest
		if jsonErr := json.Unmarshal(data, &m); jsonErr != nil {
			log.Printf("Manifest %s does not parse (%v); continuing with the others", obj.Key, jsonErr)
			continue
		}
		// A manifest stamped with a higher epoch was written by an owner that
		// held this partition after us. Its position is authoritative and ours
		// is not, so serving from here would reissue offsets it already handed
		// out. This cannot normally happen -- claiming bumps our epoch above
		// anything already recorded -- but the check is what makes a corrupted
		// or hand-edited manifest fail closed instead of rewinding a log.
		if stored := m.storedEpoch(); stored > s.partitionEpoch(m.Topic, m.Partition) {
			return restored, found, fmt.Errorf("%w: manifest at %s was written at epoch %d by %q, this agent holds epoch %d",
				errSuperseded, obj.Key, stored, m.Writer, s.partitionEpoch(m.Topic, m.Partition))
		}

		s.metadataCache.SetPartitionState(m.Topic, m.Partition, m.LogEndOffset, m.LogStartOffset, m.Segments)
		restored++
	}
	return restored, found, nil
}

// loadLegacyManifest reads the bucket-global _meta/manifest.json an agent
// older than Phase 0 leaves behind. It exists so an upgrade recovers the same
// position the previous version would have, instead of falling all the way
// back to the segment inventory.
func (s *StorageEngine) loadLegacyManifest(ctx context.Context) (int, error) {
	rc, err := s.objGet(ctx, legacyManifestKey)
	if err != nil {
		return 0, nil // absent is the normal case for a fresh bucket
	}
	data, readErr := io.ReadAll(rc)
	_ = rc.Close()
	if readErr != nil {
		return 0, readErr
	}

	parsed, parseErr := parseManifest(data)
	if parseErr != nil {
		log.Printf("Legacy manifest at %s is unreadable (%v); falling back to segment inventory", legacyManifestKey, parseErr)
		return 0, nil
	}
	if parsed.WriterEpoch > s.lease.Epoch() {
		return 0, fmt.Errorf("%w: legacy manifest at %s was written at epoch %d by %q, this agent holds epoch %d",
			errSuperseded, legacyManifestKey, parsed.WriterEpoch, parsed.Writer, s.lease.Epoch())
	}

	// The legacy manifest is bucket-global, so it describes partitions this
	// agent may not own. Under ownership only its unclaimed partitions can be
	// applied: another owner may already have moved past the position recorded
	// here, and adopting its partition state would rewind a log someone else is
	// writing.
	if s.ownership != nil {
		applicable := parsed.Partitions[:0:0]
		for _, e := range parsed.Partitions {
			if s.ownership.Owns(e.Topic, e.Partition) {
				applicable = append(applicable, e)
			}
		}
		parsed.Partitions = applicable
	}

	restored := 0
	for _, e := range parsed.Partitions {
		s.metadataCache.SetPartitionState(e.Topic, e.Partition, e.LogEndOffset, e.LogStartOffset, e.Segments)
		// Rewrite it in the new shape at the next checkpoint, so the upgrade
		// is a one-time cost rather than a permanent dependency on the old key.
		s.markManifestDirty(e.Topic, e.Partition)
		restored++
	}
	if restored > 0 {
		log.Printf("Migrated %d partition position(s) from legacy manifest %s", restored, legacyManifestKey)
	}
	return restored, nil
}
