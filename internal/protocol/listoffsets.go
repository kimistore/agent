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
	"kimistore/internal/storage"
)

func handleListOffsets(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// ListOffsets Request V0-V1:
	// ReplicaId (int32)
	// Topics Array
	//   TopicName (string)
	//   Partitions Array
	//     Partition (int32)
	//     Timestamp (int64): -1 = latest, -2 = earliest
	//     MaxNumOffsets (int32) - V0 only

	_, err := dec.Int32() // ReplicaID - ignore
	if err != nil {
		return nil, err
	}

	// V2 adds IsolationLevel (int8) after ReplicaId.
	if version >= 2 {
		if _, err := dec.Int8(); err != nil {
			return nil, err
		}
	}

	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	// Buffer the response, because V2+ leads with ThrottleTimeMs.
	type partitionResponse struct {
		partition int32
		timestamp int64
		offset    int64
	}
	type topicResponse struct {
		topic      string
		partitions []partitionResponse
	}
	responses := make([]topicResponse, 0, count)

	for i := 0; i < int(count); i++ {
		topic, err := dec.String()
		if err != nil {
			return nil, err
		}

		pCount, err := dec.Int32()
		if err != nil {
			return nil, err
		}

		tr := topicResponse{topic: topic, partitions: make([]partitionResponse, 0, pCount)}

		for j := 0; j < int(pCount); j++ {
			partition, err := dec.Int32()
			if err != nil {
				return nil, err
			}

			timestamp, err := dec.Int64() // -1: Latest, -2: Earliest
			if err != nil {
				return nil, err
			}

			if version == 0 {
				_, err = dec.Int32() // MaxNumOffsets - ignore for V0
				if err != nil {
					return nil, err
				}
			}

			// Resolve the requested boundary against the real log position.
			// Reporting 0 for "earliest" is only correct while nothing has ever
			// been reclaimed: once retention has deleted the first segments,
			// a consumer resetting to earliest is sent to an offset that no
			// longer exists, gets OffsetOutOfRange, resets to earliest again,
			// and spins forever.
			var offset int64
			var respTimestamp int64 = -1
			switch timestamp {
			case -1: // Latest: the offset the next record will be written at
				offset = store.HighWaterMark(topic, partition)
			case -2: // Earliest: the oldest offset still retrievable
				offset = store.LogStartOffset(topic, partition)
				respTimestamp = -1
			default:
				// Timestamp-based lookup is not implemented; answer with the
				// log end so a client polling for "anything after T" makes
				// progress rather than blocking.
				offset = store.HighWaterMark(topic, partition)
			}

			tr.partitions = append(tr.partitions, partitionResponse{
				partition: partition,
				timestamp: respTimestamp,
				offset:    offset,
			})
		}
		responses = append(responses, tr)
	}

	// Write response.
	//
	// ThrottleTimeMs was added in ListOffsets v2, not v1. Emitting it for v1
	// shifts every following field by four bytes, and a client decodes the
	// topic array length from the wrong place and comes back with no topics
	// at all -- a silent wrong answer rather than an error.
	if version >= 2 {
		enc.Int32(0) // ThrottleTimeMs
	}

	enc.Int32(int32(len(responses)))

	for _, tr := range responses {
		enc.String(tr.topic)
		enc.Int32(int32(len(tr.partitions)))

		for _, pr := range tr.partitions {
			enc.Int32(pr.partition)
			enc.Int16(0) // No Error

			if version == 0 {
				// V0: Offsets Array
				enc.Int32(1) // 1 Offset returned
				enc.Int64(pr.offset)
			} else {
				// V1+: Timestamp (int64) + Offset (int64)
				enc.Int64(-1)        // Timestamp (not tracked)
				enc.Int64(pr.offset) // Actual offset
			}
		}
	}

	return enc.Bytes(), nil
}
