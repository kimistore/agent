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
	"hash/crc32"
	"testing"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

// ---- wire-format builders -------------------------------------------------
//
// These reproduce what real producers emit. The previous fixtures in this file
// omitted the batchLength field and used raw snappy without Kafka's length
// prefix, which made them agree with the parser's mistakes instead of with the
// protocol.

// legacyMessage builds a magic-0/1 message (crc | magic | attributes |
// [timestamp] | key | value).
func legacyMessage(magic byte, key, value []byte) []byte {
	var buf bytes.Buffer
	buf.Write([]byte{0, 0, 0, 0})
	buf.WriteByte(magic)
	buf.WriteByte(0) // no compression
	if magic >= 1 {
		buf.Write(make([]byte, 8)) // timestamp
	}
	if key == nil {
		binary.Write(&buf, binary.BigEndian, int32(-1))
	} else {
		binary.Write(&buf, binary.BigEndian, int32(len(key)))
		buf.Write(key)
	}
	if value == nil {
		binary.Write(&buf, binary.BigEndian, int32(-1))
	} else {
		binary.Write(&buf, binary.BigEndian, int32(len(value)))
		buf.Write(value)
	}
	return buf.Bytes()
}

// wrapMessageSet frames a message as a message set entry. Java, librdkafka and
// sarama all send this shape.
func wrapMessageSet(outerOffset int64, msgs ...[]byte) []byte {
	var out bytes.Buffer
	for _, m := range msgs {
		binary.Write(&out, binary.BigEndian, outerOffset)
		binary.Write(&out, binary.BigEndian, int32(len(m)))
		out.Write(m)
		outerOffset++
	}
	return out.Bytes()
}

// wrapBare frames a message as a single entry with no outer wrapper. This is
// what segmentio/kafka-go emits, because a RecordBatch is self-describing.
func wrapBare(m []byte) []byte { return append([]byte(nil), m...) }

// recordBatchV2 builds a real RecordBatch (magic 2) carrying count records.
func recordBatchV2(baseOffset int64, count int32, attributes int16, records []byte) []byte {
	// 61 bytes of header precede the records.
	const headerLen = 61
	batchLength := headerLen - 12 + len(records)

	buf := make([]byte, 0, 12+batchLength)
	var tmp [8]byte
	var tmp2 [2]byte

	binary.BigEndian.PutUint64(tmp[:8], uint64(baseOffset))
	buf = append(buf, tmp[:8]...) // baseOffset
	binary.BigEndian.PutUint32(tmp[:4], uint32(batchLength))
	buf = append(buf, tmp[:4]...)                   // batchLength
	binary.BigEndian.PutUint32(tmp[:4], 0xFFFFFFFF) // partitionLeaderEpoch
	buf = append(buf, tmp[:4]...)
	buf = append(buf, 2)                   // magic
	binary.BigEndian.PutUint32(tmp[:4], 0) // crc placeholder
	buf = append(buf, tmp[:4]...)
	binary.BigEndian.PutUint16(tmp2[:2], uint16(attributes))
	buf = append(buf, tmp2[:2]...) // attributes
	binary.BigEndian.PutUint32(tmp[:4], uint32(count-1))
	buf = append(buf, tmp[:4]...) // lastOffsetDelta
	binary.BigEndian.PutUint64(tmp[:8], 0)
	buf = append(buf, tmp[:8]...) // firstTimestamp
	binary.BigEndian.PutUint64(tmp[:8], 0)
	buf = append(buf, tmp[:8]...) // maxTimestamp
	binary.BigEndian.PutUint64(tmp[:8], 0xFFFFFFFFFFFFFFFF)
	buf = append(buf, tmp[:8]...) // producerId
	binary.BigEndian.PutUint16(tmp2[:2], 0xFFFF)
	buf = append(buf, tmp2[:2]...) // producerEpoch
	binary.BigEndian.PutUint32(tmp[:4], 0xFFFFFFFF)
	buf = append(buf, tmp[:4]...) // baseSequence
	binary.BigEndian.PutUint32(tmp[:4], uint32(count))
	buf = append(buf, tmp[:4]...) // recordsCount
	buf = append(buf, records...)

	sum := crc32.Checksum(buf[21:], castagnoli)
	binary.BigEndian.PutUint32(buf[17:21], sum)
	return buf
}

// nRecordsV2 produces a RecordBatch with count trivial records.
func nRecordsV2(baseOffset int64, count int32) []byte {
	var records bytes.Buffer
	for i := int32(0); i < count; i++ {
		records.WriteByte(1)       // record length
		records.WriteByte(0)       // record attributes
		records.WriteByte(0)       // timestamp delta
		records.WriteByte(byte(i)) // offset delta
		records.WriteByte(0xFF)    // null key
		records.WriteByte(0x01)
		records.WriteByte(byte('v'))
		records.WriteByte(0x00) // no headers
	}
	return recordBatchV2(baseOffset, count, 0, records.Bytes())
}

// kafkaSnappy applies the 4-byte big-endian length prefix Kafka puts in front
// of a snappy block.
func kafkaSnappy(raw []byte) []byte {
	out := make([]byte, 4+len(raw))
	binary.BigEndian.PutUint32(out[:4], uint32(len(raw)))
	copy(out[4:], raw)
	return out
}

func compressWith(codec byte, data []byte) []byte {
	var buf bytes.Buffer
	switch codec {
	case 1:
		w := gzip.NewWriter(&buf)
		w.Write(data)
		w.Close()
	case 2:
		return kafkaSnappy(snappy.Encode(nil, data))
	case 3:
		w := lz4.NewWriter(&buf)
		w.Write(data)
		w.Close()
	}
	return buf.Bytes()
}

// compressedLegacyWrapper builds the V1 wrapper message that carries a
// compressed inner message set, as a real producer emits.
func compressedLegacyWrapper(codec byte, payload []byte) []byte {
	var buf bytes.Buffer
	buf.Write([]byte{0, 0, 0, 0})
	buf.WriteByte(1)                                // magic
	buf.WriteByte(codec)                            // attributes
	buf.Write(make([]byte, 8))                      // timestamp
	binary.Write(&buf, binary.BigEndian, int32(-1)) // null key
	binary.Write(&buf, binary.BigEndian, int32(len(payload)))
	buf.Write(payload)
	return buf.Bytes()
}

// ---- tests ----------------------------------------------------------------

func TestCountMessageSet_Legacy(t *testing.T) {
	if n := CountMessageSet([]byte{}); n != 0 {
		t.Errorf("empty: got %d, want 0", n)
	}

	single := wrapMessageSet(0, legacyMessage(0, []byte("k"), []byte("hello")))
	if n := CountMessageSet(single); n != 1 {
		t.Errorf("single legacy message: got %d, want 1", n)
	}

	three := wrapMessageSet(0,
		legacyMessage(1, []byte("k1"), []byte("v1")),
		legacyMessage(1, []byte("k2"), []byte("v2")),
		legacyMessage(1, []byte("k3"), []byte("v3")),
	)
	if n := CountMessageSet(three); n != 3 {
		t.Errorf("three legacy messages: got %d, want 3", n)
	}

	// A truncated tail must not be counted as a record.
	truncated := append(append([]byte(nil), three...), 0, 0, 0)
	if n := CountMessageSet(truncated); n != 3 {
		t.Errorf("truncated tail: got %d, want 3", n)
	}
}

// TestCountMessageSet_LegacyCompressed is the regression guard for the codec
// handling, including Kafka's snappy length prefix.
func TestCountMessageSet_LegacyCompressed(t *testing.T) {
	inner := wrapMessageSet(0,
		legacyMessage(0, []byte("k1"), []byte("v1")),
		legacyMessage(0, []byte("k2"), []byte("v2")),
		legacyMessage(0, []byte("k3"), []byte("v3")),
	)

	for _, tc := range []struct {
		name  string
		codec byte
	}{{"gzip", 1}, {"snappy", 2}, {"lz4", 3}} {
		t.Run(tc.name, func(t *testing.T) {
			wrapper := wrapMessageSet(0, compressedLegacyWrapper(tc.codec, compressWith(tc.codec, inner)))
			if n := CountMessageSet(wrapper); n != 3 {
				t.Errorf("%s compressed: got %d, want 3", tc.name, n)
			}
		})
	}
}

// TestCountMessageSet_RecordBatch covers both encodings a real producer can
// use. Getting the wrapped case wrong is what made the broker advance offsets
// by one per batch for every client except kafka-go.
func TestCountMessageSet_RecordBatch(t *testing.T) {
	t.Run("bare", func(t *testing.T) {
		for _, count := range []int32{1, 3, 5, 100, 1000} {
			batch := wrapBare(nRecordsV2(0, count))
			if n := CountMessageSet(batch); n != int(count) {
				t.Errorf("bare batch of %d: got %d", count, n)
			}
		}
	})

	t.Run("wrapped", func(t *testing.T) {
		for _, count := range []int32{1, 3, 5, 100, 1000} {
			batch := wrapMessageSet(0, nRecordsV2(0, count))
			if n := CountMessageSet(batch); n != int(count) {
				t.Errorf("wrapped batch of %d: got %d", count, n)
			}
		}
	})

	t.Run("mixed message set", func(t *testing.T) {
		blob := wrapMessageSet(0,
			legacyMessage(0, []byte("k"), []byte("v")),
			nRecordsV2(1, 3),
		)
		if n := CountMessageSet(blob); n != 4 {
			t.Errorf("mixed: got %d, want 4", n)
		}
	})

	t.Run("compressed record batch needs no decompression", func(t *testing.T) {
		// The record count is declared in the header, so a compressed batch
		// must be counted without inflating it.
		records := []byte{0, 0, 0}
		batch := recordBatchV2(0, 7, 1 /* gzip */, records)
		if n := CountMessageSet(wrapMessageSet(0, batch)); n != 7 {
			t.Errorf("compressed v2 batch: got %d, want 7", n)
		}
	})
}

// TestPatchOffsets_RecordBatch checks the read path re-addresses both the
// outer entry and the batch's own base offset, and leaves the CRC valid.
func TestPatchOffsets_RecordBatch(t *testing.T) {
	t.Run("bare", func(t *testing.T) {
		batch := nRecordsV2(0, 4)
		out := patchOffsets(batch, 100, 4)

		if got := int64(binary.BigEndian.Uint64(out[0:8])); got != 100 {
			t.Errorf("base offset = %d, want 100", got)
		}
		if !isRecordBatch(out) {
			t.Error("patched batch no longer self-describing")
		}
		assertBatchCRC(t, out)
	})

	t.Run("wrapped", func(t *testing.T) {
		blob := wrapMessageSet(0, nRecordsV2(0, 4))
		out := patchOffsets(blob, 100, 4)

		// A wrapped batch's outer entry is the batch's own first offset.
		if got := int64(binary.BigEndian.Uint64(out[0:8])); got != 100 {
			t.Errorf("outer entry offset = %d, want 100", got)
		}
		inner := out[msgSetEntryHeaderLen:]
		// Clients read the inner base offset in preference to the outer one.
		if got := int64(binary.BigEndian.Uint64(inner[0:8])); got != 100 {
			t.Errorf("inner base offset = %d, want 100", got)
		}
		if !isRecordBatch(inner) {
			t.Error("inner batch no longer self-describing")
		}
		assertBatchCRC(t, inner)
	})
}

// TestPatchOffsets_LeavesDeltaAlone is the regression guard for a bug that
// silently dropped records: lastOffsetDelta is a delta from baseOffset, and
// adding the base offset to it makes a client read past the end of the batch
// and skip everything in between.
func TestPatchOffsets_LeavesDeltaAlone(t *testing.T) {
	delta := func(b []byte) int32 {
		return int32(binary.BigEndian.Uint32(b[rbLastOffsetDelta : rbLastOffsetDelta+4]))
	}

	batch := nRecordsV2(0, 10)
	before := delta(batch)
	out := patchOffsets(batch, 500, 10)

	if got := delta(out); got != before {
		t.Errorf("lastOffsetDelta = %d, want %d; a client computes the batch's last offset as baseOffset+delta", got, before)
	}
	if got := int64(binary.BigEndian.Uint64(out[rbBaseOffset : rbBaseOffset+8])); got != 500 {
		t.Errorf("base offset = %d, want 500", got)
	}
	// baseOffset lives outside the checksummed region, so the CRC must still
	// verify without a rewrite.
	assertBatchCRC(t, out)
}

// TestPatchedBatchIsReadableAtNewOffset checks the arithmetic a client uses:
// records are addressed as baseOffset+index and the batch ends at
// baseOffset+lastOffsetDelta.
func TestPatchedBatchIsReadableAtNewOffset(t *testing.T) {
	const count = 10
	out := patchOffsets(nRecordsV2(0, count), 700, count)

	base := int64(binary.BigEndian.Uint64(out[rbBaseOffset : rbBaseOffset+8]))
	delta := int32(binary.BigEndian.Uint32(out[rbLastOffsetDelta : rbLastOffsetDelta+4]))

	if first := base; first != 700 {
		t.Errorf("first record offset = %d, want 700", first)
	}
	if last := base + int64(delta); last != 700+count-1 {
		t.Errorf("last record offset = %d, want %d; the next fetch would start beyond the log end", last, 700+count-1)
	}
}

func assertBatchCRC(t *testing.T, batch []byte) {
	t.Helper()
	want := binary.BigEndian.Uint32(batch[17:21])
	got := crc32.Checksum(batch[21:], castagnoli)
	if want != got {
		t.Errorf("CRC = %08x, recomputed %08x -- clients verify this", want, got)
	}
}

// TestUnwrapBatches covers the read-path re-framing that makes data written
// by a Java or librdkafka producer readable at all.
//
// Both of kafka-go's decoders assume a bare RecordBatch, so a wrapped batch is
// read as a base offset where a leader epoch belongs and rejected on its
// checksum. Re-framing is the only thing a broker can do about it.
func TestUnwrapBatches(t *testing.T) {
	t.Run("bare batch is left alone", func(t *testing.T) {
		bare := nRecordsV2(0, 5)
		if NeedsUnwrapping(bare) {
			t.Error("a bare batch must not be flagged for unwrapping")
		}
		out, changed := UnwrapBatches(bare, 0)
		if changed || len(out) != len(bare) {
			t.Error("a bare batch must pass through untouched")
		}
	})

	t.Run("wrapped batch becomes bare and is re-addressed", func(t *testing.T) {
		wrapped := wrapMessageSet(0, nRecordsV2(0, 5))
		if !NeedsUnwrapping(wrapped) {
			t.Fatal("a wrapped batch must be detected")
		}
		out, changed := UnwrapBatches(wrapped, 700)
		if !changed {
			t.Fatal("expected the blob to be re-framed")
		}
		if !isRecordBatch(out) {
			t.Fatal("result is still wrapped")
		}
		if got := int64(binary.BigEndian.Uint64(out[rbBaseOffset : rbBaseOffset+8])); got != 700 {
			t.Errorf("base offset = %d, want 700", got)
		}
		if got := int32(binary.BigEndian.Uint32(out[rbLastOffsetDelta : rbLastOffsetDelta+4])); got != 4 {
			t.Errorf("lastOffsetDelta = %d, want 4; it is a delta and must not absorb the base offset", got)
		}
		assertBatchCRC(t, out)
	})

	t.Run("multiple wrapped batches are re-addressed in sequence", func(t *testing.T) {
		blob := append(
			wrapMessageSet(0, nRecordsV2(0, 3)),
			wrapMessageSet(3, nRecordsV2(3, 4))...,
		)
		out, changed := UnwrapBatches(blob, 0)
		if !changed {
			t.Fatal("expected re-framing")
		}
		// Both batches must now be bare, at 0 and 3.
		first := binary.BigEndian.Uint64(out[0:8])
		firstLen := binary.BigEndian.Uint32(out[8:12])
		second := binary.BigEndian.Uint64(out[12+int(firstLen) : 20+int(firstLen)])
		if first != 0 || second != 3 {
			t.Errorf("batch offsets = %d, %d; want 0, 3", first, second)
		}
	})

	t.Run("legacy messages are left alone", func(t *testing.T) {
		legacy := wrapMessageSet(0, legacyMessage(0, []byte("k"), []byte("v")))
		if NeedsUnwrapping(legacy) {
			t.Error("a legacy message must not be flagged for unwrapping")
		}
	})
}

// TestPatchOffsets_LegacyMessageSet covers the flat legacy message set, which
// is what a client is forced into when the broker only offers Produce v0.
//
// A legacy message set is one entry per message, so each entry takes exactly
// one offset. Advancing by the blob's total record count instead would give
// entry 1 offset 100 instead of 1, and every consumer would read the batch at
// wildly wrong positions.
func TestPatchOffsets_LegacyMessageSet(t *testing.T) {
	blob := wrapMessageSet(0,
		legacyMessage(1, []byte("k0"), []byte("v0")),
		legacyMessage(1, []byte("k1"), []byte("v1")),
		legacyMessage(1, []byte("k2"), []byte("v2")),
	)
	if n := CountMessageSet(blob); n != 3 {
		t.Fatalf("fixture holds %d records, want 3", n)
	}

	out := patchOffsets(blob, 500, 3)

	pos := 0
	for i := 0; i < 3; i++ {
		size := int32(binary.BigEndian.Uint32(out[pos+8 : pos+12]))
		got := int64(binary.BigEndian.Uint64(out[pos : pos+8]))
		if want := int64(500 + i); got != want {
			t.Errorf("entry %d offset = %d, want %d", i, got, want)
		}
		pos += msgSetEntryHeaderLen + int(size)
	}
}

// TestPatchOffsets_CompressedWrapper pins Kafka's convention that a compressed
// wrapper carries the *last* offset of the batch it stands for. Readers
// recover the first by subtracting, so setting it to the base would shift the
// whole batch.
func TestPatchOffsets_CompressedWrapper(t *testing.T) {
	inner := wrapMessageSet(0,
		legacyMessage(0, []byte("k0"), []byte("v0")),
		legacyMessage(0, []byte("k1"), []byte("v1")),
		legacyMessage(0, []byte("k2"), []byte("v2")),
	)
	blob := wrapMessageSet(0, compressedLegacyWrapper(1, compressWith(1, inner)))

	out := patchOffsets(blob, 700, 3)
	if got := int64(binary.BigEndian.Uint64(out[0:8])); got != 702 {
		t.Errorf("wrapper offset = %d, want 702 (the last offset of the batch)", got)
	}
}
