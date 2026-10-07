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
	"strconv"
	"strings"
	"time"

	"kimistore/internal/metrics"
)

// ErrDurableTimeout reports that a record appended with acks=all was not
// confirmed present in object storage before the producer's deadline.
//
// It is deliberately not "the write failed": the record is in the local WAL
// and will almost certainly reach object storage anyway. What could not be
// proven is durability, and a producer that is told an offset it cannot later
// recover would be lied to. Reporting a timeout makes the producer retry, at
// the cost of a possible duplicate until idempotent producers land.
var ErrDurableTimeout = errors.New("record not durable in object storage within the deadline")

// DefaultFlushInterval is how often the durability loop seals the active
// segment of a partition that has acks=all producers waiting on it. It is
// therefore the upper bound on ack latency for acks=all under light load, and
// the window over which many small appends are coalesced into one object-store
// PUT. A larger interval buys fewer PUTs and slower acks; both are accepted.
var DefaultFlushInterval = 1 * time.Second

// durableState is the per-partition durability bookkeeping.
//
// frontier is the highest offset known to be contiguously present in object
// storage. pending holds segments that have been uploaded out of order, keyed
// by their base offset, so the frontier can jump forward once the gap ahead of
// them fills. Without the pending set, an upload pool that finishes a later
// segment first would let the frontier claim offsets whose predecessor is not
// stored yet.
type durableState struct {
	frontier int64
	pending  map[int64]int64 // base -> end, uploaded but not yet contiguous
}

func partitionDurabilityKey(topic string, partition int32) string {
	return topic + "/" + strconv.Itoa(int(partition))
}

// splitPartitionKey reverses partitionDurabilityKey. A Kafka topic name cannot
// contain '/', so splitting on the last one is unambiguous.
func splitPartitionKey(key string) (topic string, partition int32, ok bool) {
	i := strings.LastIndex(key, "/")
	if i <= 0 || i == len(key)-1 {
		return "", 0, false
	}
	p, err := strconv.ParseInt(key[i+1:], 10, 32)
	if err != nil {
		return "", 0, false
	}
	return key[:i], int32(p), true
}

// ensureDurable seeds a partition's durability frontier the first time it is
// touched. baseline is the log end offset *before* the append about to happen:
// everything below it is already recoverable (it came from object storage at
// startup, or from an earlier confirmed upload), so it is a sound floor.
//
// It only ever creates the entry. A later call must not move the frontier,
// because a concurrent upload may already have advanced it.
func (s *StorageEngine) ensureDurable(topic string, partition int32, baseline int64) {
	if baseline < 0 {
		baseline = 0
	}
	key := partitionDurabilityKey(topic, partition)

	s.durableMu.Lock()
	if _, ok := s.durable[key]; !ok {
		s.durable[key] = &durableState{frontier: baseline}
	}
	s.durableMu.Unlock()
}

// markSegmentDurable records that a sealed segment is now in object storage,
// and wakes any producer waiting on an offset inside it.
func (s *StorageEngine) markSegmentDurable(topic string, partition int32, base, end int64) {
	if end <= base {
		return
	}
	key := partitionDurabilityKey(topic, partition)

	s.durableMu.Lock()
	st, ok := s.durable[key]
	if !ok {
		st = &durableState{frontier: base}
		s.durable[key] = st
	}
	// A segment landing past the frontier means the frontier is behind. That is
	// the case worth repairing, so note it and drop the lock: the check below
	// touches the filesystem and must not be held across an append's wait.
	staleFrontier := base > st.frontier && len(st.pending) == 0
	s.durableMu.Unlock()

	// Repair a frontier that fell behind without a real gap under it.
	//
	// The switch below stashes a segment whose base is past the frontier into
	// pending, and pending is only ever drained by a segment whose base is
	// already at or below the frontier. If the frontier is behind because a
	// *seed* was behind, no such segment will ever arrive: every later segment
	// also lands past the frontier, also goes to pending, and the partition
	// wedges permanently. Every acks=all producer on it then times out forever
	// even though its records are sitting in object storage.
	//
	// The frontier is seeded once per partition from the log end offset, so a
	// restart whose discovery under-reported it reproduces this exactly.
	//
	// Because the uploader is serial and seals are contiguous, a lower segment
	// cannot be in flight behind this one, so the only thing that could fill the
	// gap is a sealed segment still on local disk. If there is none, everything
	// below base is already durable and the frontier may skip forward.
	//
	// Without a WAL to inspect there is nothing to prove absence with, so the
	// frontier is left alone.
	if staleFrontier && s.walMgr != nil && !s.walMgr.SealedBelow(topic, partition, base) {
		s.durableMu.Lock()
		// Re-check under the lock: a concurrent mark may have advanced the
		// frontier or filled pending while the check was in flight.
		if base > st.frontier && len(st.pending) == 0 {
			st.frontier = base
		}
		s.durableMu.Unlock()
	}

	s.durableMu.Lock()
	advanced := false
	switch {
	case base <= st.frontier && end > st.frontier:
		st.frontier = end
		advanced = true
		// A later segment may have completed first. Now that the gap is
		// filled, advance through everything that abuts.
		for {
			progress := false
			for b, e := range st.pending {
				if b <= st.frontier && e > st.frontier {
					st.frontier = e
					delete(st.pending, b)
					progress = true
				}
			}
			if !progress {
				break
			}
		}
	case base > st.frontier:
		if st.pending == nil {
			st.pending = make(map[int64]int64)
		}
		if cur, ok := st.pending[base]; !ok || end > cur {
			st.pending[base] = end
		}
	}

	if advanced {
		// Broadcast: replace the channel so waiters that parked on the old
		// one observe the new frontier after re-checking.
		close(s.durableCh)
		s.durableCh = make(chan struct{})
	}
	s.durableMu.Unlock()
}

// DurableOffset reports the highest offset known to be recoverable from object
// storage for a partition, or -1 when the partition has never been written.
func (s *StorageEngine) DurableOffset(topic string, partition int32) int64 {
	s.durableMu.Lock()
	defer s.durableMu.Unlock()
	if st, ok := s.durable[partitionDurabilityKey(topic, partition)]; ok {
		return st.frontier
	}
	return -1
}

// WaitDurable blocks until the partition's durable frontier reaches target, or
// the deadline expires.
//
// target is the offset just past the appended batch. The wait is released by
// markSegmentDurable when the segment carrying it lands, so a producer is told
// its offset only after the bytes are in object storage.
func (s *StorageEngine) WaitDurable(ctx context.Context, topic string, partition int32, target int64, timeout time.Duration) error {
	key := partitionDurabilityKey(topic, partition)

	s.durableMu.Lock()
	if _, ok := s.durable[key]; !ok {
		// Defensive: AppendContext seeds the frontier before writing, so this
		// should not happen. Seeding at zero is the safe direction -- it can
		// only cause an unnecessary timeout, never a false acknowledgement.
		s.durable[key] = &durableState{frontier: 0}
	}
	if s.durable[key].frontier >= target {
		s.durableMu.Unlock()
		return nil
	}
	// Track the highest target waiting on this partition. Entries are not
	// removed on exit: once the frontier passes a target the entry is inert,
	// and leaving it in place avoids losing a concurrent waiter's need.
	if target > s.waiters[key] {
		s.waiters[key] = target
	}
	s.durableMu.Unlock()

	start := time.Now()
	if timeout <= 0 {
		timeout = DefaultFlushInterval
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		s.durableMu.Lock()
		cur := s.durable[key].frontier
		wake := s.durableCh
		s.durableMu.Unlock()

		if cur >= target {
			metrics.DurableWaitSeconds.Observe(time.Since(start).Seconds())
			return nil
		}

		select {
		case <-wake:
		case <-timer.C:
			metrics.DurableTimeouts.Inc()
			return fmt.Errorf("%w: %s/%d offset %d not in object storage after %s (durable offset %d)",
				ErrDurableTimeout, topic, partition, target, timeout, cur)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// flushLoop seals the active segment of every partition with a producer
// waiting for durability, on the flush interval. Sealing is what hands the
// segment to the uploader; the waiters are released when that upload lands.
func (s *StorageEngine) flushLoop() {
	defer s.wg.Done()

	interval := s.flushInterval
	if interval <= 0 {
		interval = DefaultFlushInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			s.flushDuePartitions()
		}
	}
}

// flushDuePartitions rolls each partition whose waiters are still behind the
// durable frontier. A partition with no waiter, or whose waiters are already
// satisfied, is left alone, so the flush cost is paid only by active acks=all
// traffic rather than by every partition in the log.
func (s *StorageEngine) flushDuePartitions() {
	type partitionRef struct {
		topic     string
		partition int32
	}
	var due []partitionRef

	s.durableMu.Lock()
	for key, target := range s.waiters {
		st := s.durable[key]
		var frontier int64
		if st != nil {
			frontier = st.frontier
		}
		if frontier >= target {
			// Inert: the waiters this entry stood for are already satisfied.
			// Drop it so the map stays bounded by the partitions actually
			// waiting, not every partition ever waited on.
			delete(s.waiters, key)
			continue
		}
		if topic, partition, ok := splitPartitionKey(key); ok {
			due = append(due, partitionRef{topic, partition})
		}
	}
	s.durableMu.Unlock()

	for _, p := range due {
		s.walMgr.FlushPartition(p.topic, p.partition)
		metrics.DurableFlushes.Inc()
	}
}
