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

package protocol

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"time"

	"kimistore/internal/metrics"
	"kimistore/internal/storage"
	"kimistore/internal/storage/wal"
)

// maxProduceTimeout caps how long an acks=all produce waits for its segment
// to reach object storage, regardless of the client's own timeout field. A var
// so tests can shrink it rather than wait out a deliberate stall.
var maxProduceTimeout = 30 * time.Second

// produceError maps a storage error onto the Kafka code a producer can act on.
// The distinction that matters is retriability: REQUEST_TIMED_OUT tells the
// producer the record may or may not be stored and that retrying is correct,
// whereas the generic code tells it the request failed for a reason a retry
// will not fix. The producer-sequence codes are what an idempotent client
// uses to decide whether to re-send, re-init or fail.
func produceError(err error) int16 {
	switch {
	case errors.Is(err, storage.ErrDurableTimeout):
		return ErrRequestTimedOut
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ErrRequestTimedOut
	case errors.Is(err, storage.ErrLeaseLost), errors.Is(err, storage.ErrLeaseHeld):
		// Another agent owns the log, so this broker is the wrong one to
		// retry against until routing exists (phase 3).
		return ErrNotLeaderForPartition
	case errors.Is(err, storage.ErrUnknownProducer):
		return ErrUnknownProducerID
	case errors.Is(err, storage.ErrFencedProducerEpoch):
		return ErrInvalidProducerEpoch
	case errors.Is(err, storage.ErrOutOfOrderSequence):
		return ErrOutOfOrderSequence
	case errors.Is(err, storage.ErrDuplicateSequence):
		return ErrDuplicateSequence
	default:
		return ErrUnknown
	}
}

// handleInitProducerId allocates a producer id for an idempotent producer.
//
// The agent does not implement transactions, so a transactional producer is
// given an id but will fail when it tries to open a transaction. Idempotent,
// non-transactional producers -- what Grafana Mimir uses -- work.
func handleInitProducerId(ctx context.Context, dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// V0-V1 Request: transactional_id (nullable string) | transaction_timeout_ms (int32).
	if _, err := dec.String(); err != nil {
		return nil, err
	}
	if _, err := dec.Int32(); err != nil {
		return nil, err
	}

	pid, err := store.AllocateProducerID(ctx)

	// V1+ Response leads with throttle_time_ms; v0 does not have it.
	if version >= 1 {
		enc.Int32(0)
	}
	if err != nil {
		log.Printf("InitProducerId: could not allocate a producer id: %v", err)
		enc.Int16(ErrUnknown)
		enc.Int64(-1)
		enc.Int16(-1)
		return enc.Bytes(), nil
	}
	log.Printf("InitProducerId: allocated producer id %d", pid)
	enc.Int16(ErrNone)
	enc.Int64(pid)
	enc.Int16(0) // epoch
	return enc.Bytes(), nil
}

func handleProduce(ctx context.Context, dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16, _ ServerConfig) ([]byte, error) {
	// Produce Request V3:
	// TransactionalID (Nullable String)
	// Acks (int16)
	// Timeout (int32)
	// TopicArray

	if version >= 3 {
		_, err := dec.String() // transactional_id
		if err != nil {
			return nil, err
		}
	}

	acks, err := dec.Int16()
	if err != nil {
		return nil, err
	}
	timeout, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	// log.Printf("Produce: Acks=%d Timeout=%d", acks, timeout)

	// Durability policy, matching Kafka's acks semantics:
	//   acks=0  -> no response expected, producer does not want a durability
	//              guarantee, so skip the fsync and absorb the cost.
	//   acks=1  -> the leader has it, so fsync locally and ack.
	//   acks=-1 -> all in-sync replicas ack. This agent is single-node, so
	//              "all" is taken to mean the strongest guarantee it can give:
	//              the offset is not acknowledged until the segment holding it
	//              is in object storage (posture D2, see
	//              docs/ha-architecture.md). An offset a producer was told
	//              about is then one it can recover.
	durable := acks != 0
	awaitObjectStore := acks < 0

	// Bound the object-store wait by the producer's own timeout field, capped
	// so a huge client value cannot pin a handler goroutine indefinitely.
	durableTimeout := maxProduceTimeout
	if timeout > 0 {
		if d := time.Duration(timeout) * time.Millisecond; d < durableTimeout {
			durableTimeout = d
		}
	}

	// Topics Array
	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	// Produce Response:
	//   V0: Responses
	//   V1+: Responses | ThrottleTimeMs
	//
	// ThrottleTimeMs comes *after* the topic array. That is the order in
	// ProduceResponse.json, and Kafka serialises fields in declaration order,
	// so the trailing position is the correct one.
	// ThrottleTimeMs is written after the topic array, once it is known; see
	// the response schema note below.

	enc.Int32(count) // Response Topic Count matches Request

	for i := 0; i < int(count); i++ {
		topic, err := dec.String()
		if err != nil {
			return nil, err
		}

		enc.String(topic)

		// Partition Array
		pCount, err := dec.Int32()
		if err != nil {
			return nil, err
		}

		enc.Int32(pCount)

		for j := 0; j < int(pCount); j++ {
			partition, err := dec.Int32()
			if err != nil {
				return nil, err
			}

			msgSetSize, err := dec.Int32()
			if err != nil {
				return nil, err
			}

			if msgSetSize < 0 {
				return nil, fmt.Errorf("invalid negative message set size: %d", msgSetSize)
			}

			if msgSetSize > 100*1024*1024 { // 100MB safety limit
				return nil, fmt.Errorf("message set size too large: %d", msgSetSize)
			}

			// The MessageSet (Bytes). We treat it as opaque.
			// dec.Bytes() expects a length prefix. But MessageSetSize IS the length.
			// But wait, dec.Bytes() reads an Int32 length first.
			// In Produce Request, the structure is:
			// Partition (int32)
			// MessageSetSize (int32)
			// MessageSet (n bytes)

			// My default dec.Bytes() reads the length.
			// So if I use dec.Bytes(), it will read the 'next' length? No.
			// I should manually read 'msgSetSize' bytes.

			if dec.remaining() < int(msgSetSize) {
				// error
				return nil, fmt.Errorf("insufficient data for message set")
			}

			// Read the data
			batchData := dec.data[dec.off : dec.off+int(msgSetSize)]
			dec.off += int(msgSetSize)

			// The record count is what the log end offset advances by, so it
			// has to be exactly right. CountMessageSet handles legacy
			// messages, compressed wrappers, and RecordBatch V2 in both the
			// bare and the message-set-wrapped encodings.
			recordCount := wal.CountMessageSet(batchData)
			if recordCount == 0 {
				// A blob we could not frame still holds at least one record;
				// returning 0 would stall the offset and make every later
				// write collide with it.
				recordCount = 1
			}

			var offset int64
			// A batch carrying a producer id is idempotent: deduplicate it
			// against the producer's sequence state rather than appending a
			// retry a second time. A batch with a producer id of -1 has no
			// sequence to reconcile and takes the plain path.
			if pb, idempotent := wal.ProducerBatchHeaderOf(batchData); idempotent {
				offset, _, err = store.AppendIdempotentContext(ctx, topic, partition, batchData, recordCount, durable,
					pb.ProducerID, pb.Epoch, pb.BaseSequence)
			} else {
				offset, err = store.AppendContext(ctx, topic, partition, batchData, recordCount, durable)
			}
			if err == nil && awaitObjectStore {
				// The record is in the local WAL. Do not acknowledge the offset
				// until the segment holding it is in object storage, so the
				// producer is never told about a record it cannot recover.
				err = store.WaitDurable(ctx, topic, partition, offset+int64(recordCount), durableTimeout)
			}

			// Write Response Partition
			enc.Int32(partition)
			if err != nil {
				log.Printf("Storage append error: %v", err)
				enc.Int16(produceError(err))
				enc.Int64(-1)
				if version >= 2 {
					enc.Int64(-1) // LogAppendTime
				}
			} else {
				enc.Int16(0) // No Error
				enc.Int64(offset)
				if version >= 2 {
					enc.Int64(-1) // LogAppendTime
				}
				// log.Printf("Produce Resp: Topic=%s Partition=%d Offset=%d Error=0", topic, partition, offset)
			}
		}
	}

	if acks == 0 {
		// acks=0 means the producer does not want a response. The batch is
		// already written; there is simply nothing to send back.
		return nil, nil
	}
	if version >= 1 {
		// Written after the topic array, per the response schema.
		enc.Int32(0) // ThrottleTimeMs
	}
	return enc.Bytes(), nil
}

// fetchPart is one partition requested by a Fetch.
type fetchPart struct {
	partition int32
	offset    int64
	// maxBytes is this partition's share of the response, from
	// partition_max_bytes. It is the per-partition ceiling a client uses to
	// size its receive buffers, so returning more than this makes a
	// well-behaved client overrun them.
	maxBytes int32
}

// fetchRequest is a decoded Fetch request. Decoding is separated from
// responding so the response can be rebuilt after a long-poll wait, which a
// single streaming pass over the wire buffer cannot do.
type fetchRequest struct {
	version       int16
	minBytes      int32
	maxWaitMs     int32
	totalMaxBytes int32
	partsByTopic  []fetchTopic
}

type fetchTopic struct {
	topic string
	parts []fetchPart
}

// maxFetchWaitMs caps how long a Fetch will park regardless of the client's
// MaxWaitMs, bounding the resources a long poll can hold. A var so tests can
// shrink it rather than wait it out.
var maxFetchWaitMs int32 = 1000

// defaultFetchBudget is the response ceiling used when a client sends no
// MaxBytes, and the per-partition floor so a partition is never starved.
const defaultFetchBudget = int32(50 * 1024 * 1024)

// maxRecordsPerPartition caps how many stored records a single Fetch returns
// for one partition, or zero for no cap.
//
// Filling the client's byte budget in one round trip is what stops a consumer
// from becoming a request loop, so the default is no cap. The knob exists
// because a client that cannot decode a multi-batch response would otherwise
// silently see only the first one; set it to 1 for such clients.
var maxRecordsPerPartition = 0

// truncateToBlobs cuts a fetched blob after the first n message set entries.
func truncateToBlobs(data []byte, n int64) []byte {
	if n <= 0 {
		return data
	}
	pos := 0
	for i := int64(0); i < n; i++ {
		if pos+msgSetHeaderLen > len(data) {
			break
		}
		sz := int(int32(binary.BigEndian.Uint32(data[pos+8 : pos+12])))
		if sz < 0 || pos+msgSetHeaderLen+sz > len(data) {
			break
		}
		pos += msgSetHeaderLen + sz
	}
	if pos <= 0 || pos >= len(data) {
		return data
	}
	return data[:pos]
}

// msgSetHeaderLen is the size of a message set entry header.
const msgSetHeaderLen = 12

func handleFetch(ctx context.Context, dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	req, err := decodeFetch(dec, version)
	if err != nil {
		return nil, err
	}

	// Long polling. Previously maxWait was discarded and a caught-up consumer
	// got an empty response straight away, so the client re-polled
	// immediately: a tight request loop per consumer, burning CPU and request
	// budget for an answer that could not have been different. When the client
	// asked for data (minBytes > 0) and none is available, park briefly so an
	// arriving append is served in this same round trip.
	if req.minBytes > 0 && req.maxWaitMs > 0 && !anyDataAvailable(store, req) {
		wait := req.maxWaitMs
		if wait > maxFetchWaitMs {
			wait = maxFetchWaitMs
		}
		waitForData(ctx, store, wait)
	}

	// Response header. ThrottleTimeMs leads the body for V1+.
	if version >= 1 {
		enc.Int32(0)
	}
	enc.Int32(int32(len(req.partsByTopic)))

	// remaining is what the response may still spend. A client's MaxBytes is
	// the size of the receive buffer it has allocated, so overrunning it
	// causes the client to drop the connection rather than the broker to be
	// helpful.
	remaining := req.totalMaxBytes
	if remaining <= 0 {
		remaining = defaultFetchBudget
	}

	for _, ft := range req.partsByTopic {
		enc.String(ft.topic)
		enc.Int32(int32(len(ft.parts)))

		for _, p := range ft.parts {
			hw := store.HighWaterMark(ft.topic, p.partition)
			// This broker replicates nothing, so the last stable offset is the
			// log end. log_start_offset is the retention-aware earliest offset,
			// which is what lets a client find the log start without a
			// separate ListOffsets round trip.
			lastStable := hw
			logStart := store.LogStartOffset(ft.topic, p.partition)

			// Budget for this partition: the smaller of its own request and
			// whatever is left of the response budget.
			budget := int64(remaining)
			if p.maxBytes > 0 && int64(p.maxBytes) < budget {
				budget = int64(p.maxBytes)
			}
			if budget <= 0 {
				budget = 1 // always return at least the partition header
			}

			switch {
			case p.offset == hw:
				// Caught up. Answer without touching storage, so a
				// caught-up consumer costs no object store calls.
				enc.Int32(p.partition)
				enc.Int16(ErrNone)
				enc.Int64(hw)
				encodeFetchTail(enc, version, lastStable, logStart, nil)
				continue
			case p.offset > hw:
				enc.Int32(p.partition)
				enc.Int16(ErrOffsetOutOfRange)
				enc.Int64(hw)
				encodeFetchTail(enc, version, lastStable, logStart, nil)
				continue
			}

			// Stop if the log start has moved past the requested offset, and
			// say so with the real log start so the client can reset to
			// something that exists. Reporting OffsetOutOfRange against a
			// hardcoded log start of 0 is what turns a consumer that resets to
			// "earliest" into an infinite retry loop.
			if logStart > 0 && p.offset < logStart {
				enc.Int32(p.partition)
				enc.Int16(ErrOffsetOutOfRange)
				enc.Int64(hw)
				encodeFetchTail(enc, version, lastStable, logStart, nil)
				continue
			}

			data, _, rerr := store.ReadBatchContext(ctx, ft.topic, p.partition, p.offset, budget)
			if rerr == nil && maxRecordsPerPartition > 0 {
				data = truncateToBlobs(data, int64(maxRecordsPerPartition))
			}

			enc.Int32(p.partition)
			if rerr != nil {
				enc.Int16(ErrOffsetOutOfRange)
				enc.Int64(hw)
				encodeFetchTail(enc, version, lastStable, logStart, nil)
				continue
			}
			enc.Int16(ErrNone)
			enc.Int64(hw)
			// The trailing fields vary by version and must always be written
			// in the same order the protocol defines, so they are emitted in
			// one place rather than per branch.
			if version >= 4 {
				enc.Int64(lastStable)
			}
			if version >= 5 {
				enc.Int64(logStart)
			}
			if version >= 4 {
				enc.Int32(0) // aborted transactions: none, this broker has none
			}
			enc.PutBytes(data)
			remaining -= int32(len(data))
		}
	}

	return enc.Bytes(), nil
}

// encodeFetchTail writes the per-partition fields that follow the high
// watermark, then an empty record set.
func encodeFetchTail(enc *Encoder, version int16, lastStable, logStart int64, records []byte) {
	if version >= 4 {
		enc.Int64(lastStable)
	}
	if version >= 5 {
		enc.Int64(logStart)
	}
	if version >= 4 {
		enc.Int32(0) // aborted transactions
	}
	enc.PutBytes(records)
}

// anyDataAvailable reports whether at least one requested partition has
// unread data. An offset at or beyond the high watermark has nothing to
// return: equal means caught up, beyond means out of range.
func anyDataAvailable(store *storage.StorageEngine, req *fetchRequest) bool {
	for _, ft := range req.partsByTopic {
		for _, p := range ft.parts {
			if p.offset < store.HighWaterMark(ft.topic, p.partition) {
				return true
			}
		}
	}
	return false
}

// waitForData blocks until an append is signalled or the budget expires. The
// caller re-checks availability afterwards, since the signal is a broadcast
// and may have been triggered by an append to a different partition.
func waitForData(ctx context.Context, store *storage.StorageEngine, waitMs int32) {
	metrics.FetchLongPolls.Inc()

	timer := time.NewTimer(time.Duration(waitMs) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-store.DataSignal():
		metrics.FetchLongPollWakeups.WithLabelValues("data").Inc()
	case <-timer.C:
		metrics.FetchLongPollWakeups.WithLabelValues("timeout").Inc()
	case <-ctx.Done():
		// The client is gone, so there is nobody left to answer. Parking for
		// the rest of the wait would hold a handler goroutine and an
		// in-flight slot on a connection that has already moved on.
		metrics.FetchLongPollWakeups.WithLabelValues("cancelled").Inc()
	}
}

func decodeFetch(dec *Decoder, version int16) (*fetchRequest, error) {
	req := &fetchRequest{version: version, totalMaxBytes: -1}

	// ReplicaId (int32), MaxWaitTime (int32), MinBytes (int32)
	if _, err := dec.Int32(); err != nil { // replicaID
		return nil, err
	}
	maxWait, err := dec.Int32()
	if err != nil {
		return nil, err
	}
	req.maxWaitMs = maxWait

	minBytes, err := dec.Int32()
	if err != nil {
		return nil, err
	}
	req.minBytes = minBytes

	if version >= 3 {
		totalMaxBytes, err := dec.Int32()
		if err != nil {
			return nil, err
		}
		req.totalMaxBytes = totalMaxBytes
	}

	// V4+ adds IsolationLevel (int8) after MaxBytes.
	if version >= 4 {
		if _, err := dec.Int8(); err != nil {
			return nil, err
		}
	}

	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	for i := 0; i < int(count); i++ {
		topic, err := dec.String()
		if err != nil {
			return nil, err
		}
		pCount, err := dec.Int32()
		if err != nil {
			return nil, err
		}
		ft := fetchTopic{topic: topic}
		for j := 0; j < int(pCount); j++ {
			partition, err := dec.Int32()
			if err != nil {
				return nil, err
			}
			offset, err := dec.Int64()
			if err != nil {
				return nil, err
			}
			partitionMaxBytes, err := dec.Int32()
			if err != nil {
				return nil, err
			}
			ft.parts = append(ft.parts, fetchPart{partition: partition, offset: offset, maxBytes: partitionMaxBytes})
		}
		req.partsByTopic = append(req.partsByTopic, ft)
	}
	return req, nil
}
