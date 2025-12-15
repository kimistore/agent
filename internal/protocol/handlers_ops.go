package protocol

import (
	"encoding/binary"
	"fmt"
	"log"

	"go-stream/internal/storage"
)

func handleProduce(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// Produce Request V0:
	// Acks (int16)
	// Timeout (int32)
	// TopicArray

	acks, err := dec.Int16()
	if err != nil {
		return nil, err
	}
	timeout, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	log.Printf("Produce: Acks=%d Timeout=%d", acks, timeout)

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
			recordCount := countMessageSet(batchData)
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
				enc.Int16(10) // Error: MessageSizeTooLarge or similar? 10=MessageSizeTooLarge, 1=OffsetOutOfRange...
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
			}
		}
	}

	// ThrottleTimeMs (int32) - V1+
	if version >= 1 {
		enc.Int32(0)
	}

	return enc.Bytes(), nil
}

func handleFetch(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// Fetch Request V0:
	// ReplicaId (int32)
	// MaxWaitTime (int32)
	// MinBytes (int32)
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

	log.Printf("Fetch: Replica=%d Wait=%d MinBytes=%d", replicaID, maxWait, minBytes)

	// We are ignoring Long Polling (MaxWait) for MVP phase.

	// Topics Array
	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	// Fetch Response V0 (similar structure)
	// V1 adds ThrottleTime at the front of response.
	// Docs say: V1 FetchResponse: ThrottleTime [TopicResponse]

	if version >= 1 {
		enc.Int32(0) // ThrottleTimeMs 0
	}

	enc.Int32(count)

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

			maxBytes, err := dec.Int32()
			if err != nil {
				return nil, err
			}
			_ = maxBytes

			// READ FROM STORAGE
			hw := store.HighWaterMark(topic, partition)

			data, err := store.Read(topic, partition, fetchOffset)

			enc.Int32(partition)
			if err != nil {
				// If requested offset is >= HighWaterMark, return NoError and empty data
				if fetchOffset == hw {
					enc.Int16(0)  // No Error
					enc.Int64(hw) // HighwaterMark
					enc.Int32(0)  // MessageSetSize 0
				} else if fetchOffset > hw {
					enc.Int16(1) // OffsetOutOfRange
					enc.Int64(hw)
					enc.Int32(0)
				} else {
					// Actual read error (e.g. data lost/corrupt or unexpected)
					// If it's "not found" but < hw, it implies gap or deleted.
					// For now, treat as OffsetOutOfRange to force client reset?
					// Or Unknown (1).
					enc.Int16(1)
					enc.Int64(hw)
					enc.Int32(0)
				}
			} else {
				enc.Int16(0) // No error
				// HighwaterMark: Next offset. We don't track it easily yet from Read().
				// Read() returns data. We need to parse data to know how many messages?
				// Or store.Read() should return NextOffset.

				// Ideally store.Read() returns (data, nextOffset).
				// My interface was: Read(topic, partition, offset) ([]byte, error)
				// I should update it to return nextOffset too.

				// Simpler hack: We assume we read *some* messages.
				// YES! My WAL Append increments offset by 1 per "Batch".
				// So if we read successfully, HWMark is at least fetchOffset + 1.

				enc.Int64(hw) // HighwaterMark
				// enc.Int32(int32(len(data))) // PutBytes adds this
				enc.PutBytes(data) // MessageSet raw
			}
		}
	}

	return enc.Bytes(), nil
}

func countMessageSet(data []byte) int {
	count := 0
	pos := 0
	// MessageSet Entry: Offset(8) + Size(4) + Msg(Size)
	for pos <= len(data)-12 {
		// Offset is data[pos : pos+8] (We don't need value)
		// Size is data[pos+8 : pos+12]
		size := binary.BigEndian.Uint32(data[pos+8 : pos+12])

		totalLen := 12 + int(size)
		if pos+totalLen > len(data) {
			break
		}

		count++
		pos += totalLen
	}
	return count
}
