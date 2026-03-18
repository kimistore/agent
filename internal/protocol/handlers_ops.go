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
	_ = acks
	_ = timeout

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

			offset, err := store.Append(topic, partition, batchData, recordCount)

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

func handleFetch(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// Fetch Request V0-V3:
	// ReplicaId (int32)
	// MaxWaitTime (int32)
	// MinBytes (int32)
	// MaxBytes (int32) -- added in V3
	// TopicArray

	replicaID, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	maxWait, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	minBytes, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	totalMaxBytes := int32(-1)
	if version >= 3 {
		totalMaxBytes, err = dec.Int32()
		if err != nil {
			return nil, err
		}
	}

	// log.Printf("Fetch: Replica=%d Wait=%d MinBytes=%d MaxBytes=%d", replicaID, maxWait, minBytes, totalMaxBytes)
	_ = replicaID
	_ = maxWait
	_ = minBytes

	// We are ignoring Long Polling (MaxWait) for MVP phase.

	// Topics Array
	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	// Fetch Response V1+ adds ThrottleTime at the front of response.
	if version >= 1 {
		enc.Int32(0) // ThrottleTimeMs 0
	}

	enc.Int32(count)
	currentResponseSize := int32(0)

	for i := 0; i < int(count); i++ {
		topic, err := dec.String()
		if err != nil {
			return nil, err
		}

		enc.String(topic)

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

			fetchOffset, err := dec.Int64()
			if err != nil {
				return nil, err
			}

			partitionMaxBytes, err := dec.Int32()
			if err != nil {
				return nil, err
			}
			_ = partitionMaxBytes

			// Enforce totalMaxBytes if V3+
			if totalMaxBytes > 0 && currentResponseSize >= totalMaxBytes {
				// We reached the limit, return empty for remaining partitions
				enc.Int32(partition)
				enc.Int16(0) // No Error
				enc.Int64(store.HighWaterMark(topic, partition))
				enc.Int32(0) // MessageSetSize 0
				continue
			}

			// READ FROM STORAGE
			hw := store.HighWaterMark(topic, partition)

			// Fast Path: If at HW, return empty immediately without checking storage (avoids S3 calls)
			if fetchOffset == hw {
				enc.Int32(partition)
				enc.Int16(0)  // No Error
				enc.Int64(hw) // HighwaterMark
				enc.Int32(0)  // MessageSetSize 0
				continue
			}
			if fetchOffset > hw {
				enc.Int32(partition)
				enc.Int16(1) // OffsetOutOfRange
				enc.Int64(hw)
				enc.Int32(0)
				continue
			}

			data, err := store.Read(topic, partition, fetchOffset)

			enc.Int32(partition)
			if err != nil {
				enc.Int16(1) // OffsetOutOfRange
				enc.Int64(hw)
				enc.Int32(0)
			} else {
				enc.Int16(0) // No error
				enc.Int64(hw)
				enc.PutBytes(data) // MessageSet raw
				currentResponseSize += int32(len(data))
			}
		}
	}

	return enc.Bytes(), nil
}
