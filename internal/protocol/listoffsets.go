package protocol

import (
	"go-stream/internal/storage"
)

func handleListOffsets(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// ListOffsets V0:
	// ReplicaId (int32)
	// Topics Array

	_, err := dec.Int32() // ReplicaID - ignore
	if err != nil {
		return nil, err
	}

	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	// ListOffsets V1 does NOT have ThrottleTimeMs. (V2 does)
	// if version >= 1 {
	// 	enc.Int32(0) // ThrottleTimeMs
	// }

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

			timeVal, err := dec.Int64() // -1: Latest, -2: Earliest
			if err != nil {
				return nil, err
			}
			_ = timeVal

			if version == 0 {
				_, err = dec.Int32() // MaxNumOffsets - ignore for V0 (usually 1)
				if err != nil {
					return nil, err
				}
			}

			// Response:
			// Partition
			// ErrorCode
			enc.Int32(partition)
			enc.Int16(0) // No Error

			if version == 0 {
				// V0: Offsets Array
				ops := int32(1)
				enc.Int32(ops) // 1 Offset returned

				// Return 0 for now
				enc.Int64(0)
			} else {
				// V1: Timestamp (int64) + Offset (int64)
				enc.Int64(-1) // Timestamp (No timestamp associated)
				enc.Int64(0)  // Offset
			}
		}
	}

	// ThrottleTimeMs (int32) - Added in V2 or V1?
	// ListOffsets Response V1: ThrottleTime [Topic]
	// ListOffsets Response V2: ThrottleTime [Topic]

	// Wait, ListOffsets V1 DOES have ThrottleTimeMs at start of response?
	// Kafka Protocol: ListOffsets Response V1.
	// throttle_time_ms (int32)
	// responses (array)

	// BUT I structure my response code by writing Topics first.
	// I need to change the structure of writing response if V1.
	// My handleListOffsets writes immediately to `enc`.

	// I need to verify if ThrottleTime is FIRST or LAST.
	// Usually V1+ responses put ThrottleTime at the end, EXCEPT Fetch/ListOffsets sometimes?
	// ListOffsets V1: ThrottleTimeMs is FIELD 0.

	// So if version >= 1, I need to insert Int32(0) BEFORE `enc.Int32(count)`.
	// But `count` is written inside loop? No `count` is written at line 18.

	// I'll fix this in next edit.

	return enc.Bytes(), nil
}
