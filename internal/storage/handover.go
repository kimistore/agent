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
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"kimistore/internal/metrics"
)

// HandoverReport describes what a handover cost, so the number phase 4 exists to
// produce is a measurement rather than an assumption.
type HandoverReport struct {
	Topic     string
	Partition int32
	Epoch     int64
	// Durable reports whether the partition's whole log was confirmed present in
	// object storage before the claim was released.
	Durable bool
	// Tail is the log end the next owner will recover from.
	Tail int64
	// Elapsed is the wall time the handover took, which is what an operator
	// watching a partition move actually cares about.
	Elapsed time.Duration
	// Released is false when the claim was deliberately kept, which happens when
	// the tail could not be made durable: handing over then would lose records
	// that an acks=all producer was told about.
	Released bool
}

// DefaultHandoverBudget bounds a handover.
//
// It has to cover an upload plus the manifest write, so it is deliberately longer
// than the engine's per-operation timeout. A handover that runs out of budget
// keeps its claim: the partition stays writable here, which is a slower recovery
// rather than a lost one.
const DefaultHandoverBudget = 30 * time.Second

// DrainPartition hands one partition to another agent without stopping this one.
//
// The sequence is the point, and the order is load-bearing:
//
//  1. seal the active segment and wait for the upload to land,
//  2. wait until the durable frontier covers the log end, so nothing the next
//     owner would not be able to read is left behind,
//  3. write the partition manifest, so the next owner recovers the position
//     without having to reconcile against object storage,
//  4. release the ownership claim, and only then stop serving the partition.
//
// Releasing first and flushing after would be the fatal inversion: the claim is
// gone the instant it is released, so another agent may take the partition while
// this one is still appending to it.
//
// If the tail cannot be made durable within the budget the claim is *kept*. The
// partition stays writable here and the caller is told why. The alternative --
// releasing anyway -- is what turns a slow object store into silent data loss.
func (s *StorageEngine) DrainPartition(ctx context.Context, topic string, partition int32, budget time.Duration) (HandoverReport, error) {
	start := time.Now()
	if budget <= 0 {
		budget = DefaultHandoverBudget
	}
	report := HandoverReport{Topic: topic, Partition: partition}

	if s.ownership == nil || !s.ownership.fenced {
		return report, fmt.Errorf("handover needs per-partition ownership: set KIMISTORE_PARTITION_OWNERSHIP")
	}
	if !s.Owns(topic, partition) {
		return report, fmt.Errorf("%w: %s/%d", ErrPartitionNotOwned, topic, partition)
	}
	// Ask the object store, not the local belief. A claim can be gone by the time
	// this runs, and every step below writes durable state on its behalf.
	if err := s.ownership.verifyClaim(ctx, topic, partition); err != nil {
		return report, err
	}

	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	tail := s.HighWaterMark(topic, partition)
	report.Tail = tail
	report.Epoch = s.partitionEpoch(topic, partition)

	// 1. Seal the active segment. Everything above the last uploaded segment is
	// local-only right now, and that is precisely the tail the next owner would
	// not see.
	s.walMgr.FlushPartition(topic, partition)

	// 2. Wait for it to be recoverable.
	if err := s.WaitDurable(ctx, topic, partition, tail, remaining(budget, start)); err != nil {
		report.Elapsed = time.Since(start)
		log.Printf("Handover: keeping %s/%d at epoch %d: the tail is not durable yet (%v)",
			topic, partition, report.Epoch, err)
		metrics.HandoverKept.Inc()
		return report, fmt.Errorf("handover of %s/%d aborted: %w", topic, partition, err)
	}
	report.Durable = true

	// The wait above can take seconds, which is plenty of time for the claim to
	// have been taken. Re-check before writing anything durable on its behalf: a
	// manifest stamped with a stale epoch would tell the new owner about a log end
	// that belongs to a writer it superseded.
	if err := s.ownership.verifyClaim(ctx, topic, partition); err != nil {
		report.Elapsed = time.Since(start)
		log.Printf("Handover: not handing over %s/%d: %v", topic, partition, err)
		metrics.HandoverKept.Inc()
		return report, fmt.Errorf("handover of %s/%d aborted: %w", topic, partition, err)
	}

	// 3. The manifest, so the next owner starts from the recorded position rather
	// than reconciling against object storage on startup.
	s.markManifestDirty(topic, partition)
	if err := s.SaveManifestContext(ctx); err != nil {
		report.Elapsed = time.Since(start)
		log.Printf("Handover: keeping %s/%d at epoch %d: the manifest could not be written (%v)",
			topic, partition, report.Epoch, err)
		metrics.HandoverKept.Inc()
		return report, fmt.Errorf("handover of %s/%d aborted: manifest: %w", topic, partition, err)
	}

	// 4. Release. The partition stops being served the moment this returns.
	if err := s.ownership.releasePartition(ctx, topic, partition); err != nil {
		report.Elapsed = time.Since(start)
		metrics.HandoverKept.Inc()
		return report, fmt.Errorf("handover of %s/%d: release: %w", topic, partition, err)
	}

	// 5. Tell the cluster now rather than at the next refresh tick: a peer that
	// still routes to this agent for a partition it can no longer serve will get
	// errors until the tick lands.
	s.publishRouting(ctx)

	// The local WAL for a partition this agent no longer owns must not be read:
	// its contents predate the handover and the new owner has moved on.
	s.dropPartitionFromView(topic, partition)

	report.Released = true
	report.Elapsed = time.Since(start)
	metrics.HandoverCompleted.Inc()
	metrics.HandoverSeconds.Observe(report.Elapsed.Seconds())
	metrics.PartitionsOwned.Set(float64(s.ownedCount()))
	log.Printf("Handover: %s/%d released at epoch %d in %s; log end %d, durable",
		topic, partition, report.Epoch, report.Elapsed.Truncate(time.Millisecond), tail)
	return report, nil
}

// remaining is how much of the handover budget is left, floored so a caller that
// has already spent it gets a prompt refusal rather than an unbounded wait.
func remaining(budget time.Duration, start time.Time) time.Duration {
	left := budget - time.Since(start)
	if left < time.Second {
		return time.Second
	}
	return left
}

// dropPartitionFromView removes a handed-over partition from this agent's
// in-memory state.
//
// Its WAL directory is left on disk deliberately: it is the handover's only copy
// of anything that was written but never flushed, and deleting it on the drain
// path would turn a recoverable situation into a lost one.
func (s *StorageEngine) dropPartitionFromView(topic string, partition int32) {
	s.metadataCache.RemovePartition(topic, partition)
	s.durableMu.Lock()
	delete(s.durable, partitionDurabilityKey(topic, partition))
	delete(s.waiters, partitionDurabilityKey(topic, partition))
	s.durableMu.Unlock()
	s.forgetManifestDirty(topic)
}

// ownedCount is how many partitions this agent currently holds.
func (s *StorageEngine) ownedCount() int {
	if s.ownership == nil {
		return 0
	}
	return s.ownership.count()
}

// DrainPartitions hands over several partitions, stopping at the first that
// cannot be handed over safely.
//
// It is not transactional: a partition that was already released stays released.
// That is deliberate -- each handover is independently safe, and rolling one back
// would mean re-claiming a partition another agent may already have taken.
func (s *StorageEngine) DrainPartitions(ctx context.Context, refs []PartitionRef, budget time.Duration) ([]HandoverReport, error) {
	out := make([]HandoverReport, 0, len(refs))
	for _, ref := range refs {
		report, err := s.DrainPartition(ctx, ref.Topic, ref.Partition, budget)
		out = append(out, report)
		if err != nil {
			return out, errors.Join(err, fmt.Errorf("%d of %d partition(s) handed over", len(out), len(refs)))
		}
	}
	return out, nil
}

// OwnedPartitionRefs lists the partitions this agent currently holds a claim on,
// as PartitionRef values suitable for DrainPartition.
//
// OwnedPartitions reports keys of the form "topic/partition". This parses them,
// so the two cannot disagree about what "owned" means.
//
// The list comes from the local view, so a partition whose claim has moved may
// still appear. DrainOwned asks the object store before acting on any of them.
func (s *StorageEngine) OwnedPartitionRefs() ([]PartitionRef, error) {
	keys := s.OwnedPartitions()
	out := make([]PartitionRef, 0, len(keys))
	for _, key := range keys {
		topic, partition, err := splitOwnedPartitionKey(key)
		if err != nil {
			return nil, err
		}
		out = append(out, PartitionRef{Topic: topic, Partition: partition})
	}
	return out, nil
}

// splitOwnedPartitionKey reads a "topic/partition" ownership key.
//
// The split takes the last slash rather than the first. A topic name may contain
// a slash, and splitting on the first one would read "a/b/2" as topic "a" and
// partition 0 rather than the 2.
func splitOwnedPartitionKey(key string) (string, int32, error) {
	topic, part, ok := strings.Cut(key, "/")
	if !ok {
		return "", 0, fmt.Errorf("owned partition key %q is not topic/partition", key)
	}
	// A topic with a slash in it leaves more segments after the first cut, so
	// rejoin everything before the final segment.
	if idx := strings.LastIndex(key, "/"); idx != len(topic) {
		topic = key[:idx]
		part = key[idx+1:]
	}
	pid, err := strconv.Atoi(part)
	if err != nil {
		return "", 0, fmt.Errorf("owned partition key %q has a non-numeric partition: %w", key, err)
	}
	return topic, int32(pid), nil
}

// DrainOwned hands over every partition this agent owns, and is the call a
// SIGTERM handler should make.
//
// It differs from DrainPartitions in two ways that matter during shutdown. It
// does not stop at the first failure, because one partition with a slow tail
// must not strand the claims on all the others; the process is leaving either
// way, and every claim it keeps costs the cluster a full TTL. And a partition
// that could not be handed over safely is left held rather than released, so
// the claim expires on its own schedule instead of being handed to another agent
// while its tail is still local-only.
//
// The returned error joins every failure, so a caller can report the count
// without losing the individual reasons.
func (s *StorageEngine) DrainOwned(ctx context.Context, budget time.Duration) ([]HandoverReport, error) {
	refs, err := s.OwnedPartitionRefs()
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, nil
	}

	if budget <= 0 {
		budget = DrainShutdownBudget
	}
	log.Printf("Draining %d owned partition(s) before shutdown, budget %s", len(refs), budget)

	// The deadline is enforced by the context rather than by summing per-partition
	// budgets. Dividing a budget across N partitions does not bound the total once
	// the per-partition share falls under a second: the floor that keeps each
	// attempt meaningful then makes the run as long as N seconds, and a process
	// killed mid-drain loses the seal it was attempting.
	//
	// So the context is the hard bound and the per-partition budget is advisory,
	// derived from what is actually left.
	deadline := time.Now().Add(budget)
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	reports := make([]HandoverReport, 0, len(refs))
	var errs []error
	released, kept := 0, 0

	for _, ref := range refs {
		left := time.Until(deadline)
		if left <= 0 {
			errs = append(errs, fmt.Errorf("%s/%d: not attempted, the drain budget ran out", ref.Topic, ref.Partition))
			kept++
			continue
		}

		// At least a second per attempt, but never more than the time that is
		// left. DrainPartition substitutes its own default for a non-positive
		// budget, which is 30 seconds and would ignore the deadline entirely.
		share := left / time.Duration(len(refs)-len(reports))
		if share < time.Second {
			share = time.Second
		}
		if share > left {
			share = left
		}

		report, drainErr := s.DrainPartition(ctx, ref.Topic, ref.Partition, share)
		reports = append(reports, report)
		if drainErr != nil {
			errs = append(errs, fmt.Errorf("%s/%d: %w", ref.Topic, ref.Partition, drainErr))
			kept++
			continue
		}
		released++
	}

	summary := fmt.Sprintf("drained %d of %d partition(s); %d kept their claim", released, len(refs), kept)
	if len(errs) == 0 {
		log.Printf("Drain: %s", summary)
		return reports, nil
	}

	log.Printf("Drain: %s", summary)
	return reports, errors.Join(errs...)
}

// DrainShutdownBudget bounds the whole shutdown drain.
//
// It has to cover the slowest partition upload plus its manifest write, and it
// has to sit inside whatever grace period the supervisor gives the process. A
// budget larger than the grace period is worse than a small one, because the
// process is killed mid-drain and loses the seal it was attempting.
const DrainShutdownBudget = 25 * time.Second

// PartitionRef names one partition to hand over.
type PartitionRef struct {
	Topic     string
	Partition int32
}
