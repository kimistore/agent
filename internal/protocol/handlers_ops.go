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
	"fmt"
	"log"
	"time"

	"kimistore/internal/metrics"
	"kimistore/internal/storage"
	"kimistore/internal/storage/wal"
)

func handleProduce(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
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
	_ = timeout

	// Durability policy, matching Kafka's acks semantics:
	//   acks=0  -> no response expected, producer does not want a durability
	//              guarantee, so skip the fsync and absorb the cost.
	//   acks=1  -> leader ack; the record must survive a crash.
	//   acks=-1 -> all in-sync replicas ack; same requirement here, since
	//              this agent is single-node.
	// A single-node agent is its own only replica, so acks>=1 means fsync.
	durable := acks != 0

	// Topics Array
	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	// We will build the response as we process.
	// Produce Response V0:
	// TopicArray

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

			// APPEND TO STORAGE
			// Parse batch to count messages (MessageSet V0/V1)
			recordCount := wal.CountMessageSet(batchData)
			if recordCount == 0 {
				// Empty batch? or parse error?
				// Just fallback to 1 to avoid sticking offset
				recordCount = 1
			}

			offset, err := store.Append(topic, partition, batchData, recordCount, durable)

			// Write Response Partition
			enc.Int32(partition)
			if err != nil {
				log.Printf("Storage append error: %v", err)
				enc.Int16(10) // Error: MessageSizeTooLarge
				// Generic error: 1 unknown
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

	// ThrottleTimeMs (int32) - V1+
	if version >= 1 {
		enc.Int32(0)
	}

	if acks == 0 {
		return nil, nil
	}
	return enc.Bytes(), nil
}

// fetchPart is one partition requested by a Fetch.
type fetchPart struct {
	partition int32
	offset    int64
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

	// responseBytes accumulates bytes returned, to enforce totalMaxBytes.
	responseBytes int32
}

type fetchTopic struct {
	topic string
	parts []fetchPart
}

// maxFetchWaitMs caps how long a Fetch will park regardless of the client's
// MaxWaitMs, bounding the resources a long poll can hold. A var so tests can
// shrink it rather than wait it out.
var maxFetchWaitMs int32 = 1000

func handleFetch(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
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
		waitForData(store, wait)
	}

	// Response header. ThrottleTimeMs leads the body for V1+.
	if version >= 1 {
		enc.Int32(0)
	}
	enc.Int32(int32(len(req.partsByTopic)))

	for _, ft := range req.partsByTopic {
		enc.String(ft.topic)
		enc.Int32(int32(len(ft.parts)))

		for _, p := range ft.parts {
			// Enforce totalMaxBytes if V3+
			if req.totalMaxBytes > 0 && req.responseBytes >= req.totalMaxBytes {
				enc.Int32(p.partition)
				enc.Int16(0) // No Error
				enc.Int64(store.HighWaterMark(ft.topic, p.partition))
				enc.Int32(0) // MessageSetSize 0
				continue
			}

			hw := store.HighWaterMark(ft.topic, p.partition)

			// Fast path: already at the end of the log. Answer without
			// touching storage, so a caught-up consumer costs no S3 calls.
			if p.offset == hw {
				enc.Int32(p.partition)
				enc.Int16(0)  // No Error
				enc.Int64(hw) // HighwaterMark
				enc.Int32(0)  // MessageSetSize 0
				continue
			}
			if p.offset > hw {
				enc.Int32(p.partition)
				enc.Int16(1) // OffsetOutOfRange
				enc.Int64(hw)
				enc.Int32(0)
				continue
			}

			data, rerr := store.Read(ft.topic, p.partition, p.offset)

			enc.Int32(p.partition)
			if rerr != nil {
				enc.Int16(1) // OffsetOutOfRange
				enc.Int64(hw)
				enc.Int32(0)
			} else {
				enc.Int16(0) // No error
				enc.Int64(hw)
				enc.PutBytes(data)
				req.responseBytes += int32(len(data))
			}
		}
	}

	return enc.Bytes(), nil
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
func waitForData(store *storage.StorageEngine, waitMs int32) {
	metrics.FetchLongPolls.Inc()

	timer := time.NewTimer(time.Duration(waitMs) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-store.DataSignal():
		metrics.FetchLongPollWakeups.WithLabelValues("data").Inc()
	case <-timer.C:
		metrics.FetchLongPollWakeups.WithLabelValues("timeout").Inc()
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
			if _, err := dec.Int32(); err != nil { // partitionMaxBytes
				return nil, err
			}
			ft.parts = append(ft.parts, fetchPart{partition: partition, offset: offset})
		}
		req.partsByTopic = append(req.partsByTopic, ft)
	}
	return req, nil
}
