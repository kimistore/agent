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

package wal

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"testing"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

func TestCountMessageSet(t *testing.T) {
	// 1. Empty
	if n := CountMessageSet([]byte{}); n != 0 {
		t.Errorf("expected 0 for empty, got %d", n)
	}

	// 2. Single Message
	// Offset(8) + Size(4) + Body(Size)
	msg1Body := []byte("hello")
	msg1 := make([]byte, 12+len(msg1Body))
	binary.BigEndian.PutUint64(msg1[0:8], 0)
	binary.BigEndian.PutUint32(msg1[8:12], uint32(len(msg1Body)))
	copy(msg1[12:], msg1Body)

	if n := CountMessageSet(msg1); n != 1 {
		t.Errorf("expected 1, got %d", n)
	}

	// 3. Two Messages
	msg2Body := []byte("world")
	msg2 := make([]byte, 12+len(msg2Body))
	binary.BigEndian.PutUint64(msg2[0:8], 1)
	binary.BigEndian.PutUint32(msg2[8:12], uint32(len(msg2Body)))
	copy(msg2[12:], msg2Body)

	batch := append(msg1, msg2...)
	if n := CountMessageSet(batch); n != 2 {
		t.Errorf("expected 2, got %d", n)
	}

	// 4. Corrupt/Incomplete (Trailing bytes)
	incomplete := append(batch, []byte{0, 0, 0}...)
	if n := CountMessageSet(incomplete); n != 2 {
		t.Errorf("expected 2 for incomplete tail, got %d", n)
	}
}

// Helper to create a simple message
func createMessage(magic byte, key, value []byte) []byte {
	var buf bytes.Buffer
	buf.Write([]byte{0, 0, 0, 0}) // CRC (ignored)
	buf.WriteByte(magic)          // Magic
	buf.WriteByte(0)              // Attributes (No compression)

	if magic >= 1 {
		buf.Write(make([]byte, 8)) // Timestamp
	}

	// Key
	if key == nil {
		binary.Write(&buf, binary.BigEndian, int32(-1))
	} else {
		binary.Write(&buf, binary.BigEndian, int32(len(key)))
		buf.Write(key)
	}

	// Value
	if value == nil {
		binary.Write(&buf, binary.BigEndian, int32(-1))
	} else {
		binary.Write(&buf, binary.BigEndian, int32(len(value)))
		buf.Write(value)
	}

	// Calculate size and wrap in MessageSet entry
	// MessageSet Entry: Offset(8) + Size(4) + Msg
	msgBytes := buf.Bytes()
	var entry bytes.Buffer
	binary.Write(&entry, binary.BigEndian, int64(0))             // Offset
	binary.Write(&entry, binary.BigEndian, int32(len(msgBytes))) // Size
	entry.Write(msgBytes)

	return entry.Bytes()
}

func compressData(codec byte, data []byte) []byte {
	var buf bytes.Buffer
	switch codec {
	case 1: // GZIP
		w := gzip.NewWriter(&buf)
		w.Write(data)
		w.Close()
	case 2: // Snappy
		// Standard snappy
		return snappy.Encode(nil, data)
	case 3: // LZ4
		w := lz4.NewWriter(&buf)
		w.Write(data)
		w.Close()
	}
	return buf.Bytes()
}

func TestCountMessageSet_Compressed(t *testing.T) {
	// Create a batch of 3 messages
	m1 := createMessage(0, []byte("k1"), []byte("v1"))
	m2 := createMessage(0, []byte("k2"), []byte("v2"))
	m3 := createMessage(0, []byte("k3"), []byte("v3"))
	batch := append(append(m1, m2...), m3...)

	// 1. Test Uncompressed
	if n := CountMessageSet(batch); n != 3 {
		t.Errorf("Uncompressed: expected 3, got %d", n)
	}

	// 2. Test GZIP
	gzipData := compressData(1, batch)
	// Wrap in a message with GZIP attribute
	gzipMsg := createWrapperMessage(1, gzipData)
	if n := CountMessageSet(gzipMsg); n != 3 {
		t.Errorf("GZIP: expected 3, got %d", n)
	}

	// 3. Test Snappy
	snappyData := compressData(2, batch)
	snappyMsg := createWrapperMessage(2, snappyData)
	if n := CountMessageSet(snappyMsg); n != 3 {
		t.Errorf("Snappy: expected 3, got %d", n)
	}

	// 4. Test LZ4
	lz4Data := compressData(3, batch)
	lz4Msg := createWrapperMessage(3, lz4Data)
	if n := CountMessageSet(lz4Msg); n != 3 {
		t.Errorf("LZ4: expected 3, got %d", n)
	}
}

func createWrapperMessage(codec byte, value []byte) []byte {
	// Wrapper message usually has Magic=1 (to support Attributes properly?)
	// Actually V0 supports attributes too.
	// But let's use V1 for safety as wrapping usually implies modern usage.
	magic := byte(1)

	var buf bytes.Buffer
	buf.Write([]byte{0, 0, 0, 0}) // CRC
	buf.WriteByte(magic)
	buf.WriteByte(codec & 0x07) // Attributes: Compression

	if magic >= 1 {
		buf.Write(make([]byte, 8)) // Timestamp
	}

	// Key (null)
	binary.Write(&buf, binary.BigEndian, int32(-1))

	// Value (Compressed Data)
	binary.Write(&buf, binary.BigEndian, int32(len(value)))
	buf.Write(value)

	msgBytes := buf.Bytes()
	var entry bytes.Buffer
	binary.Write(&entry, binary.BigEndian, int64(0))
	binary.Write(&entry, binary.BigEndian, int32(len(msgBytes)))
	entry.Write(msgBytes)

	return entry.Bytes()
}
