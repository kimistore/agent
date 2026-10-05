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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"kimistore/internal/metrics"
	"kimistore/internal/storage/wal"
)

// Idempotent-producer errors. They map onto Kafka's producer error codes in
// the protocol layer; here they only have to say what went wrong.
var (
	// ErrUnknownProducer is a batch whose sequence is not zero but whose
	// producer has no state. Either the producer is new and broken, or the
	// agent restarted and lost the state; the client re-initialises either
	// way.
	ErrUnknownProducer = errors.New("unknown producer id")

	// ErrFencedProducerEpoch is a batch from an epoch older than the active
	// one. A newer incarnation of the same producer has taken over.
	ErrFencedProducerEpoch = errors.New("producer epoch is older than the active one")

	// ErrOutOfOrderSequence is a sequence ahead of the expected one that is
	// not just a pipelining artifact.
	ErrOutOfOrderSequence = errors.New("out of order sequence number")

	// ErrDuplicateSequence is a retry too old for the retained window. It
	// still must not be appended: the sequence says it was stored once.
	ErrDuplicateSequence = errors.New("duplicate sequence number outside the retained window")
)

const (
	// producerRecentBatches is how many of a producer's most recent batches
	// are retained so a retry can be handed back the offset it was originally
	// assigned. Kafka retains the same number.
	producerRecentBatches = 5

	// producerIDSeqKey is the persisted producer id allocator. Persisting it
	// is what stops a restart from reissuing an id: a new producer with a
	// recycled id would be mistaken for the old one and its first batch
	// dropped as a duplicate.
	producerIDSeqKey = "_producers/_seq"

	// sequenceWait bounds how long a batch whose sequence is ahead of the
	// expected one waits for the gap to fill. The connection layer handles
	// pipelined requests concurrently, so two batches from one producer can
	// arrive out of order; waiting briefly turns that into a retry at worst
	// instead of a spurious rejection.
	sequenceWait = 250 * time.Millisecond
)

// producerBatch records where a batch started and which sequence opened it,
// so a retry of that exact batch can be answered with its original offset.
type producerBatch struct {
	baseSequence int32
	baseOffset   int64
}

// producerEntry is one producer's state within one partition.
type producerEntry struct {
	epoch      int16
	nextSeq    int32
	lastOffset int64
	recent     []producerBatch
}

func (e *producerEntry) note(b producerBatch) {
	e.recent = append(e.recent, b)
	if len(e.recent) > producerRecentBatches {
		e.recent = e.recent[len(e.recent)-producerRecentBatches:]
	}
}

func (e *producerEntry) lookup(baseSequence int32) (int64, bool) {
	for _, b := range e.recent {
		if b.baseSequence == baseSequence {
			return b.baseOffset, true
		}
	}
	return 0, false
}

// partitionProducerState is all producer state for one partition. The lock is
// per partition so appends to different partitions do not serialise.
type partitionProducerState struct {
	mu      sync.Mutex
	entries map[int64]*producerEntry
}

func (s *StorageEngine) partitionProducers(topic string, partition int32) *partitionProducerState {
	key := partitionDurabilityKey(topic, partition)

	s.producerStatesMu.Lock()
	defer s.producerStatesMu.Unlock()
	ps := s.producerStates[key]
	if ps == nil {
		ps = &partitionProducerState{entries: make(map[int64]*producerEntry)}
		s.producerStates[key] = ps
	}
	return ps
}

// waitForSequence parks while a batch's sequence is ahead of the partition's
// expected one, giving an in-flight earlier batch time to land. It returns as
// soon as the sequence is no longer ahead, or the budget expires.
func (ps *partitionProducerState) waitForSequence(producerID int64, epoch int16, baseSequence int32, budget time.Duration) {
	deadline := time.Now().Add(budget)
	for {
		ps.mu.Lock()
		e := ps.entries[producerID]
		ps.mu.Unlock()

		if e == nil || e.epoch != epoch || baseSequence <= e.nextSeq {
			return
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// AppendIdempotentContext appends an idempotent producer's batch, rejecting a
// stale epoch, deduplicating a retry, and rejecting a sequence that is ahead
// of the expected one.
//
// It returns duplicate=true together with the offset the batch was originally
// assigned when the batch is a retry. That offset is what the producer must be
// told: appending the batch again would put the same records in the log twice.
func (s *StorageEngine) AppendIdempotentContext(ctx context.Context, topic string, partition int32, batch []byte, recordCount int, sync bool, producerID int64, epoch int16, baseSequence int32) (offset int64, duplicate bool, err error) {
	if err := ctx.Err(); err != nil {
		return -1, false, err
	}
	ps := s.partitionProducers(topic, partition)

	// Tolerate out-of-order delivery of pipelined batches before rejecting.
	ps.waitForSequence(producerID, epoch, baseSequence, sequenceWait)

	ps.mu.Lock()
	defer ps.mu.Unlock()

	e := ps.entries[producerID]
	switch {
	case e == nil:
		if baseSequence != 0 {
			return -1, false, fmt.Errorf("%w: producer %d is unknown and sequence %d is not 0",
				ErrUnknownProducer, producerID, baseSequence)
		}
		e = &producerEntry{epoch: epoch}
		ps.entries[producerID] = e

	case epoch < e.epoch:
		return -1, false, fmt.Errorf("%w: producer %d epoch %d is older than %d",
			ErrFencedProducerEpoch, producerID, epoch, e.epoch)

	case epoch > e.epoch:
		if baseSequence != 0 {
			return -1, false, fmt.Errorf("%w: producer %d opened epoch %d at sequence %d, not 0",
				ErrOutOfOrderSequence, producerID, epoch, baseSequence)
		}
		e.epoch = epoch
		e.nextSeq = 0
		e.lastOffset = 0
		e.recent = nil

	case baseSequence < e.nextSeq:
		// A retry of something already stored. This is the window D2 opens
		// when an ack times out: without this, the retry is appended again.
		if off, ok := e.lookup(baseSequence); ok {
			metrics.ProducerDuplicates.Inc()
			s.producerDedups.Add(1)
			return off, true, nil
		}
		return -1, false, fmt.Errorf("%w: producer %d sequence %d is older than the %d-batch window",
			ErrDuplicateSequence, producerID, baseSequence, producerRecentBatches)

	case baseSequence > e.nextSeq:
		return -1, false, fmt.Errorf("%w: producer %d expected sequence %d, got %d",
			ErrOutOfOrderSequence, producerID, e.nextSeq, baseSequence)
	}

	offset, aerr := s.appendCore(ctx, topic, partition, batch, recordCount, sync)
	if offset < 0 {
		return -1, false, aerr
	}

	// The record is in the log even if the response was cancelled, so the
	// sequence must advance: a later retry has to be recognised as one.
	e.nextSeq = baseSequence + int32(recordCount)
	e.lastOffset = offset
	e.note(producerBatch{baseSequence: baseSequence, baseOffset: offset})
	return offset, false, aerr
}

// ProducerDedups reports how many retried idempotent batches have been
// recognised and answered with their original offset.
func (s *StorageEngine) ProducerDedups() int64 {
	return s.producerDedups.Load()
}

// AllocateProducerID hands out the next producer id and persists the counter
// before the id is used, so a crash cannot reissue it.
func (s *StorageEngine) AllocateProducerID(ctx context.Context) (int64, error) {
	s.producerIDMu.Lock()
	defer s.producerIDMu.Unlock()

	id := s.nextProducerID
	body := []byte(strconv.FormatInt(id+1, 10))
	if err := s.objPut(ctx, producerIDSeqKey, bytes.NewReader(body)); err != nil {
		return -1, err
	}
	s.nextProducerID = id + 1
	metrics.ProducerIDsAllocated.Inc()
	return id, nil
}

// loadProducerIDSeq restores the allocator. A missing object is a fresh
// bucket: ids start at 1.
func (s *StorageEngine) loadProducerIDSeq(ctx context.Context) {
	rc, err := s.objGet(ctx, producerIDSeqKey)
	if err != nil {
		return
	}
	data, readErr := io.ReadAll(rc)
	_ = rc.Close()
	if readErr != nil {
		return
	}
	v, parseErr := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if parseErr != nil || v < 1 {
		return
	}
	s.producerIDMu.Lock()
	if v > s.nextProducerID {
		s.nextProducerID = v
	}
	s.producerIDMu.Unlock()
}

// recoverProducerState rebuilds recent idempotent producer state from the
// local WAL tail.
//
// It exists so a retried batch after a restart is still recognised as a
// duplicate. Scanning only the newest local segment is deliberate: the
// records a producer might retry are exactly the ones that were not
// acknowledged, and on a crash those are the ones still on local disk. A
// fresh machine has no local tail, which is the one case where idempotence
// resets across a restart; that is documented.
func (s *StorageEngine) recoverProducerState(ctx context.Context) {
	_ = ctx
	topics, err := os.ReadDir(s.walDir)
	if err != nil {
		return
	}
	recovered := 0
	for _, topicEntry := range topics {
		if !topicEntry.IsDir() {
			continue
		}
		topic := topicEntry.Name()
		partDirs, err := os.ReadDir(filepath.Join(s.walDir, topic))
		if err != nil {
			continue
		}
		for _, pd := range partDirs {
			if !pd.IsDir() {
				continue
			}
			pid64, err := strconv.ParseInt(pd.Name(), 10, 32)
			if err != nil {
				continue
			}
			file := newestSegmentFile(filepath.Join(s.walDir, topic, pd.Name()))
			if file == "" {
				continue
			}
			recovered += s.recoverProducerStateFromFile(topic, int32(pid64), file)
		}
	}

	// The persisted allocator should already be ahead of anything recovered,
	// but a failed allocator write must not be able to reissue an id that is
	// already in the log.
	maxPid := int64(-1)
	s.producerStatesMu.Lock()
	for _, ps := range s.producerStates {
		ps.mu.Lock()
		for pid := range ps.entries {
			if pid > maxPid {
				maxPid = pid
			}
		}
		ps.mu.Unlock()
	}
	s.producerStatesMu.Unlock()
	if maxPid >= 0 {
		s.producerIDMu.Lock()
		if maxPid+1 > s.nextProducerID {
			s.nextProducerID = maxPid + 1
		}
		s.producerIDMu.Unlock()
	}

	if recovered > 0 {
		log.Printf("Producer state: recovered %d producer(s) from the local WAL", recovered)
	}
}

// recoverProducerStateFromFile scans one segment and folds its producer
// batches into the partition's state. It returns how many producers were seen
// for the first time.
func (s *StorageEngine) recoverProducerStateFromFile(topic string, partition int32, path string) int {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()

	ps := s.partitionProducers(topic, partition)
	ps.mu.Lock()
	defer ps.mu.Unlock()

	seen := 0
	header := make([]byte, 12)
	for {
		if _, err := io.ReadFull(f, header); err != nil {
			break
		}
		offset := int64(binary.BigEndian.Uint64(header[0:8]))
		size := int32(binary.BigEndian.Uint32(header[8:12]))
		if size < 0 || int64(size) > maxWalEntrySize {
			break
		}
		body := make([]byte, size)
		if _, err := io.ReadFull(f, body); err != nil {
			break
		}
		pb, ok := wal.ProducerBatchHeaderOf(body)
		if !ok {
			continue
		}
		e := ps.entries[pb.ProducerID]
		if e == nil || pb.Epoch > e.epoch {
			e = &producerEntry{epoch: pb.Epoch}
			ps.entries[pb.ProducerID] = e
			seen++
		}
		if pb.Epoch == e.epoch {
			e.nextSeq = pb.BaseSequence + pb.Records
			e.lastOffset = offset
			e.note(producerBatch{baseSequence: pb.BaseSequence, baseOffset: offset})
		}
	}
	return seen
}

// newestSegmentFile returns the active segment when it holds data, otherwise
// the sealed segment with the highest base offset. Those are the two places a
// producer's most recent batches can be.
func newestSegmentFile(dir string) string {
	active := filepath.Join(dir, "active.log")
	if info, err := os.Stat(active); err == nil && info.Size() > 0 {
		return active
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return ""
	}
	best := ""
	bestOffset := int64(-1)
	for _, e := range entries {
		if e.IsDir() || e.Name() == "active.log" || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		off, err := strconv.ParseInt(strings.TrimSuffix(e.Name(), ".log"), 10, 64)
		if err != nil {
			continue
		}
		if off > bestOffset {
			bestOffset = off
			best = filepath.Join(dir, e.Name())
		}
	}
	return best
}
