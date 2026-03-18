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

	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	// Buffer response data since V1+ needs ThrottleTimeMs at start
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

			// Get actual offset from storage
			var offset int64
			switch timestamp {
			case -1: // Latest (next offset to be written)
				offset = store.HighWaterMark(topic, partition)
			case -2: // Earliest
				offset = 0 // We don't support log compaction/deletion yet
			default:
				// Timestamp-based lookup not implemented, return latest
				offset = store.HighWaterMark(topic, partition)
			}

			tr.partitions = append(tr.partitions, partitionResponse{
				partition: partition,
				timestamp: timestamp,
				offset:    offset,
			})
		}
		responses = append(responses, tr)
	}

	// Write response
	// V1+: ThrottleTimeMs at start
	if version >= 1 {
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
