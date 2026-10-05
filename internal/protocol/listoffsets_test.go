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
	"os"
	"testing"

	"kimistore/internal/storage"
)

func TestListOffsets(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "listoffsets_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	engine, err := storage.NewStorageEngine(tmpDir, &MockObjectStore{}, "test-bucket", storage.RetentionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Create topic and write some data
	topic := "test-offsets-topic"
	partition := int32(0)

	// Write 5 batches to get HWM of 5
	for i := 0; i < 5; i++ {
		_, err := engine.Append(topic, partition, []byte("test-data"), 1, true)
		if err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	// Verify HWM is 5
	hwm := engine.HighWaterMark(topic, partition)
	if hwm != 5 {
		t.Fatalf("Expected HWM 5, got %d", hwm)
	}

	t.Run("V0_Latest", func(t *testing.T) {
		// Build ListOffsets V0 request
		// ReplicaId (int32) + TopicCount (int32) + Topic (string) + PartitionCount (int32)
		// + Partition (int32) + Timestamp (int64) + MaxNumOffsets (int32)
		reqEnc := NewEncoder()
		reqEnc.Int32(-1)        // ReplicaId
		reqEnc.Int32(1)         // Topic count
		reqEnc.String(topic)    // Topic name
		reqEnc.Int32(1)         // Partition count
		reqEnc.Int32(partition) // Partition
		reqEnc.Int64(-1)        // Timestamp: -1 = latest
		reqEnc.Int32(1)         // MaxNumOffsets (V0 only)

		dec := NewDecoder(reqEnc.Bytes())
		respEnc := NewEncoder()

		respBytes, err := handleListOffsets(dec, respEnc, engine, 0)
		if err != nil {
			t.Fatalf("handleListOffsets failed: %v", err)
		}

		// Decode V0 response
		respDec := NewDecoder(respBytes)
		topicCount, _ := respDec.Int32()
		if topicCount != 1 {
			t.Errorf("Expected 1 topic, got %d", topicCount)
		}

		respTopic, _ := respDec.String()
		if respTopic != topic {
			t.Errorf("Expected topic %s, got %s", topic, respTopic)
		}

		partCount, _ := respDec.Int32()
		if partCount != 1 {
			t.Errorf("Expected 1 partition, got %d", partCount)
		}

		respPartition, _ := respDec.Int32()
		if respPartition != partition {
			t.Errorf("Expected partition %d, got %d", partition, respPartition)
		}

		errCode, _ := respDec.Int16()
		if errCode != 0 {
			t.Errorf("Expected no error, got %d", errCode)
		}

		// V0: Offsets array
		offsetCount, _ := respDec.Int32()
		if offsetCount != 1 {
			t.Errorf("Expected 1 offset, got %d", offsetCount)
		}

		offset, _ := respDec.Int64()
		if offset != 5 {
			t.Errorf("Expected offset 5 (HWM), got %d", offset)
		}
	})

	t.Run("V0_Earliest", func(t *testing.T) {
		reqEnc := NewEncoder()
		reqEnc.Int32(-1)        // ReplicaId
		reqEnc.Int32(1)         // Topic count
		reqEnc.String(topic)    // Topic name
		reqEnc.Int32(1)         // Partition count
		reqEnc.Int32(partition) // Partition
		reqEnc.Int64(-2)        // Timestamp: -2 = earliest
		reqEnc.Int32(1)         // MaxNumOffsets (V0 only)

		dec := NewDecoder(reqEnc.Bytes())
		respEnc := NewEncoder()

		respBytes, err := handleListOffsets(dec, respEnc, engine, 0)
		if err != nil {
			t.Fatalf("handleListOffsets failed: %v", err)
		}

		respDec := NewDecoder(respBytes)
		respDec.Int32()  // topic count
		respDec.String() // topic
		respDec.Int32()  // partition count
		respDec.Int32()  // partition
		respDec.Int16()  // error code
		respDec.Int32()  // offset count

		offset, _ := respDec.Int64()
		if offset != 0 {
			t.Errorf("Expected earliest offset 0, got %d", offset)
		}
	})

	t.Run("V1_Latest", func(t *testing.T) {
		// Build ListOffsets V1 request (no MaxNumOffsets field)
		reqEnc := NewEncoder()
		reqEnc.Int32(-1)        // ReplicaId
		reqEnc.Int32(1)         // Topic count
		reqEnc.String(topic)    // Topic name
		reqEnc.Int32(1)         // Partition count
		reqEnc.Int32(partition) // Partition
		reqEnc.Int64(-1)        // Timestamp: -1 = latest

		dec := NewDecoder(reqEnc.Bytes())
		respEnc := NewEncoder()

		respBytes, err := handleListOffsets(dec, respEnc, engine, 1)
		if err != nil {
			t.Fatalf("handleListOffsets failed: %v", err)
		}

		// Decode V1 response. There is no throttle field in v1, so the
		// topic count comes first; reading a throttle here is what shifted
		// every later field by four bytes.
		respDec := NewDecoder(respBytes)

		topicCount, _ := respDec.Int32()
		if topicCount != 1 {
			t.Errorf("Expected 1 topic, got %d", topicCount)
		}

		respTopic, _ := respDec.String()
		if respTopic != topic {
			t.Errorf("Expected topic %s, got %s", topic, respTopic)
		}

		partCount, _ := respDec.Int32()
		if partCount != 1 {
			t.Errorf("Expected 1 partition, got %d", partCount)
		}

		respPartition, _ := respDec.Int32()
		if respPartition != partition {
			t.Errorf("Expected partition %d, got %d", partition, respPartition)
		}

		errCode, _ := respDec.Int16()
		if errCode != 0 {
			t.Errorf("Expected no error, got %d", errCode)
		}

		// V1: Timestamp + Offset (no array)
		timestamp, _ := respDec.Int64()
		_ = timestamp // We return -1 for timestamp

		offset, _ := respDec.Int64()
		if offset != 5 {
			t.Errorf("Expected offset 5 (HWM), got %d", offset)
		}
	})

	t.Run("V1_Earliest", func(t *testing.T) {
		reqEnc := NewEncoder()
		reqEnc.Int32(-1)        // ReplicaId
		reqEnc.Int32(1)         // Topic count
		reqEnc.String(topic)    // Topic name
		reqEnc.Int32(1)         // Partition count
		reqEnc.Int32(partition) // Partition
		reqEnc.Int64(-2)        // Timestamp: -2 = earliest

		dec := NewDecoder(reqEnc.Bytes())
		respEnc := NewEncoder()

		respBytes, err := handleListOffsets(dec, respEnc, engine, 1)
		if err != nil {
			t.Fatalf("handleListOffsets failed: %v", err)
		}

		respDec := NewDecoder(respBytes)
		// ListOffsets v1 has no throttle field; it was added in v2. Skipping
		// it here is what a conformant client does, and asserting on it is
		// what hid the bug this replaced.
		respDec.Int32()  // topic count
		respDec.String() // topic
		respDec.Int32()  // partition count
		respDec.Int32()  // partition
		respDec.Int16()  // error code
		respDec.Int64()  // timestamp

		offset, _ := respDec.Int64()
		if offset != 0 {
			t.Errorf("Expected earliest offset 0, got %d", offset)
		}
	})

	t.Run("NonExistentTopic", func(t *testing.T) {
		reqEnc := NewEncoder()
		reqEnc.Int32(-1)
		reqEnc.Int32(1)
		reqEnc.String("non-existent-topic")
		reqEnc.Int32(1)
		reqEnc.Int32(0)
		reqEnc.Int64(-1)

		dec := NewDecoder(reqEnc.Bytes())
		respEnc := NewEncoder()

		respBytes, err := handleListOffsets(dec, respEnc, engine, 1)
		if err != nil {
			t.Fatalf("handleListOffsets failed: %v", err)
		}

		respDec := NewDecoder(respBytes)
		// ListOffsets v1 has no throttle field; it was added in v2. Skipping
		// it here is what a conformant client does, and asserting on it is
		// what hid the bug this replaced.
		respDec.Int32()  // topic count
		respDec.String() // topic
		respDec.Int32()  // partition count
		respDec.Int32()  // partition
		respDec.Int16()  // error code
		respDec.Int64()  // timestamp

		offset, _ := respDec.Int64()
		// Non-existent topic should return 0 (empty)
		if offset != 0 {
			t.Errorf("Expected offset 0 for non-existent topic, got %d", offset)
		}
	})
}
