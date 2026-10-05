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
	"fmt"
	"hash/crc32"
	"io"
	"log"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

// Wire-format geometry.
//
// A "message set" is a sequence of entries, each laid out as:
//
//	offset (8) | size (4) | message[size]
//
// The message is either a legacy message (magic 0/1) or, for Kafka 0.11+,
// a RecordBatch (magic 2). A RecordBatch is itself self-describing:
//
//	baseOffset (8) | batchLength (4) | partitionLeaderEpoch (4) | magic (1) |
//	crc (4) | attributes (2) | lastOffsetDelta (4) | firstTimestamp (8) |
//	maxTimestamp (8) | producerId (8) | producerEpoch (2) | baseSequence (4) |
//	recordsCount (4) | records...
//
// Because a RecordBatch begins with its own offset+length pair, some producers
// (notably segmentio/kafka-go) emit the batch *bare*, with no outer message
// set entry, while others (the Java client, librdkafka, sarama) wrap it in
// one. Both encodings are valid Kafka and both appear in production, so
// everything below has to tell them apart rather than assume one of them.
const (
	// msgSetEntryHeaderLen is the size of an outer message set entry header.
	msgSetEntryHeaderLen = 12

	// recordBatchHeaderLen is the number of bytes from the start of a
	// RecordBatch up to and including recordsCount. A batch is always at
	// least this long, so it doubles as a cheap "is this a RecordBatch?"
	// test.
	recordBatchHeaderLen = 61

	// Field offsets within a RecordBatch, relative to its first byte.
	rbBaseOffset      = 0
	rbBatchLength     = 8
	rbMagic           = 16
	rbCRC             = 17
	rbAttributes      = 21
	rbLastOffsetDelta = 23
	rbProducerID      = 43
	rbProducerEpoch   = 51
	rbBaseSequence    = 53
	rbRecordsCount    = 57

	// recordBatchMagicV2 is the only magic byte value a RecordBatch can carry.
	recordBatchMagicV2 = 2

	// A RecordBatch's CRC covers attributes onwards, which is why baseOffset
	// can be rewritten without invalidating it: patching the offsets must
	// never move the CRC's starting point.
)

// castagnoli is the CRC-32C table the Kafka record batch format is defined in
// terms of. Tests use it to build and verify fixtures.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// isRecordBatch reports whether b starts with a plausible RecordBatch header.
//
// The magic byte alone is too weak a signal: it is read at rbMagic, and a
// wrapped batch places its outer entry header in front of it. Requiring the
// declared batchLength to match the buffer length is what makes this safe for
// the bare encoding without false-positiving on a message set.
func isRecordBatch(b []byte) bool {
	if len(b) < recordBatchHeaderLen || b[rbMagic] != recordBatchMagicV2 {
		return false
	}
	return int32(binary.BigEndian.Uint32(b[rbBatchLength:rbBatchLength+4])) == int32(len(b)-12)
}

// recordBatchInfo is the subset of a RecordBatch header the broker needs in
// order to account for and re-address the records it carries.
type recordBatchInfo struct {
	RecordsCount    int32
	LastOffsetDelta int32
	Attributes      int16
	Compressed      bool
}

// parseRecordBatch reads a RecordBatch header. The caller must have already
// established that the buffer is one via isRecordBatch.
func parseRecordBatch(b []byte) (recordBatchInfo, error) {
	if !isRecordBatch(b) {
		return recordBatchInfo{}, fmt.Errorf("not a v2 record batch")
	}
	attrs := int16(binary.BigEndian.Uint16(b[rbAttributes : rbAttributes+2]))
	return recordBatchInfo{
		RecordsCount:    int32(binary.BigEndian.Uint32(b[rbRecordsCount : rbRecordsCount+4])),
		LastOffsetDelta: int32(binary.BigEndian.Uint32(b[rbLastOffsetDelta : rbLastOffsetDelta+4])),
		Attributes:      attrs,
		Compressed:      attrs&0x07 != 0,
	}, nil
}

// ProducerBatchHeader is the idempotence-relevant subset of a v2 RecordBatch:
// which producer wrote it, at which epoch, and the sequence its first record
// carries.
type ProducerBatchHeader struct {
	ProducerID   int64
	Epoch        int16
	BaseSequence int32
	Records      int32
}

// ProducerBatchHeaderOf extracts a producer batch's idempotence header from a
// produced record blob.
//
// It accepts both encodings the produce path sees: a bare RecordBatch and a
// RecordBatch wrapped in a message set entry. ok is false for a legacy
// message, an unparseable batch, or a batch whose producer id is -1 -- a
// producer that is not idempotent and therefore has no sequence state to
// reconcile.
func ProducerBatchHeaderOf(data []byte) (ProducerBatchHeader, bool) {
	batch := data
	if !isRecordBatch(batch) {
		if len(data) < msgSetEntryHeaderLen {
			return ProducerBatchHeader{}, false
		}
		size := int32(binary.BigEndian.Uint32(data[8:12]))
		if size < 0 || msgSetEntryHeaderLen+int(size) > len(data) {
			return ProducerBatchHeader{}, false
		}
		batch = data[msgSetEntryHeaderLen : msgSetEntryHeaderLen+int(size)]
		if !isRecordBatch(batch) {
			return ProducerBatchHeader{}, false
		}
	}

	pid := int64(binary.BigEndian.Uint64(batch[rbProducerID : rbProducerID+8]))
	if pid < 0 {
		return ProducerBatchHeader{}, false
	}
	return ProducerBatchHeader{
		ProducerID:   pid,
		Epoch:        int16(binary.BigEndian.Uint16(batch[rbProducerEpoch : rbProducerEpoch+2])),
		BaseSequence: int32(binary.BigEndian.Uint32(batch[rbBaseSequence : rbBaseSequence+4])),
		Records:      int32(binary.BigEndian.Uint32(batch[rbRecordsCount : rbRecordsCount+4])),
	}, true
}

// setRecordBatchBaseOffset re-addresses a RecordBatch to a new base offset.
//
// Only baseOffset moves. lastOffsetDelta is a *delta* from that base, and
// clients compute a record's offset as baseOffset + delta and the batch's last
// offset as baseOffset + lastOffsetDelta. Adding the base offset to the delta
// as well makes the client read past the end of the batch and skip the records
// in between, silently.
//
// The CRC needs no repair: a batch's checksum covers attributes onwards, and
// baseOffset sits before it, so the two are independent by design.
func setRecordBatchBaseOffset(batch []byte, baseOffset int64) error {
	if !isRecordBatch(batch) {
		return fmt.Errorf("not a v2 record batch")
	}
	binary.BigEndian.PutUint64(batch[rbBaseOffset:rbBaseOffset+8], uint64(baseOffset))
	return nil
}

// legacyLayout describes where a legacy (magic 0/1) message keeps its payload:
// crc(4) | magic(1) | attributes(1) | [timestamp(8)] | key | value.
//
// valueOff and valueLen locate the value field, which is where a compressed
// batch hides the real message set. valueLen is -1 for a null value.
type legacyLayout struct {
	valueOff   int
	valueLen   int
	magic      byte
	compressed bool
	codec      byte
}

func parseLegacyMessage(body []byte) (legacyLayout, bool) {
	if len(body) < 6 {
		return legacyLayout{}, false
	}
	l := legacyLayout{
		magic:      body[4],
		compressed: body[5]&0x07 != 0,
		codec:      body[5] & 0x07,
	}
	off := 6
	if l.magic >= 1 {
		off += 8
	}

	readLen := func() (int, bool) {
		if len(body) < off+4 {
			return 0, false
		}
		n := int(int32(binary.BigEndian.Uint32(body[off : off+4])))
		off += 4
		return n, true
	}

	keyLen, ok := readLen()
	if !ok {
		return legacyLayout{}, false
	}
	if keyLen >= 0 {
		off += keyLen
		if len(body) < off {
			return legacyLayout{}, false
		}
	}

	valLen, ok := readLen()
	if !ok {
		return legacyLayout{}, false
	}
	if valLen >= 0 && len(body) < off+valLen {
		return legacyLayout{}, false
	}

	l.valueOff = off
	l.valueLen = valLen
	return l, true
}

// countLegacyCompressed decompresses a legacy wrapper and counts the records
// inside it. Returns 0 if the payload cannot be decoded, so the caller can
// fall back to counting the wrapper as a single record.
func countLegacyCompressed(body []byte, codec byte) int {
	l, ok := parseLegacyMessage(body)
	if !ok || !l.compressed || l.valueLen <= 0 {
		return 0
	}
	raw, err := decompress(codec, body[l.valueOff:l.valueOff+l.valueLen])
	if err != nil {
		return 0
	}
	return CountMessageSet(raw)
}

// CountMessageSet returns the number of records in a produced record blob.
//
// Both encodings are handled: a bare RecordBatch, and a message set whose
// entries wrap either a RecordBatch or a legacy message. Getting this wrong
// is not a cosmetic problem: the count is what the broker advances its
// per-partition offset by, so undercounting makes the broker hand out offsets
// it has already used, and consumers then fetch past the high watermark and
// lose data.
func CountMessageSet(data []byte) int {
	// A bare RecordBatch has no outer entry; its leading offset+length pair
	// is the batch's own, which is exactly why the two encodings look alike
	// for the first 12 bytes.
	if isRecordBatch(data) {
		info, err := parseRecordBatch(data)
		if err != nil {
			return 1
		}
		if info.Compressed {
			// A compressed batch still declares its record count, so there is
			// no need to decompress it to count.
			return int(info.RecordsCount)
		}
		return int(info.RecordsCount)
	}

	count := 0
	pos := 0
	for pos+msgSetEntryHeaderLen <= len(data) {
		size := int32(binary.BigEndian.Uint32(data[pos+8 : pos+12]))
		if size < 0 {
			break
		}
		total := msgSetEntryHeaderLen + int(size)
		if pos+total > len(data) {
			break // truncated or corrupt trailing entry
		}
		count += countEntry(data[pos+12 : pos+total])
		pos += total
	}
	if count == 0 && len(data) >= msgSetEntryHeaderLen {
		// A blob we could not frame at all still holds at least one record.
		// Returning 0 would stall the offset and make every later write
		// collide with this one.
		return 1
	}
	return count
}

// countEntry counts the records in a single message set entry body.
func countEntry(body []byte) int {
	if isRecordBatch(body) {
		info, err := parseRecordBatch(body)
		if err != nil {
			return 1
		}
		return int(info.RecordsCount)
	}

	l, ok := parseLegacyMessage(body)
	if !ok {
		return 1
	}
	if l.compressed {
		if n := countLegacyCompressed(body, body[5]&0x07); n > 0 {
			return n
		}
		log.Printf("WAL: could not decompress legacy batch; counting wrapper as 1 record")
	}
	return 1
}

func decompress(codec byte, in []byte) ([]byte, error) {
	switch codec {
	case 1: // GZIP
		r, err := gzip.NewReader(bytes.NewReader(in))
		if err != nil {
			return nil, err
		}
		defer func() { _ = r.Close() }()
		return io.ReadAll(r)
	case 2: // Snappy (Kafka framing: 4-byte big-endian length prefix)
		return decodeKafkaSnappy(in)
	case 3: // LZ4 (Kafka uses the legacy frame format)
		return io.ReadAll(lz4.NewReader(bytes.NewReader(in)))
	default:
		return nil, fmt.Errorf("unsupported compression codec: %d", codec)
	}
}

// decodeKafkaSnappy unwraps the length-prefixed framing Kafka uses on top of
// raw snappy blocks before handing the payload to the snappy decoder.
func decodeKafkaSnappy(in []byte) ([]byte, error) {
	if len(in) < 4 {
		return nil, fmt.Errorf("snappy payload too short")
	}
	n := int(binary.BigEndian.Uint32(in[:4]))
	if n < 0 || 4+n > len(in) {
		return nil, fmt.Errorf("snappy length prefix %d out of range", n)
	}
	return snappy.Decode(nil, in[4:4+n])
}

// UnwrapBatches rewrites a stored record blob so every RecordBatch it carries
// is served bare rather than inside a message set entry.
//
// Why this exists: a RecordBatch already begins with its own offset and length,
// so it is self-describing, and the two encodings differ only by whether the
// producer also wrote an outer message set entry around it. Both are valid
// Kafka. But segmentio/kafka-go -- the client Grafana Mimir 3.0 is built on --
// assumes the bare form in *both* of its decoders: it reads the first eight
// bytes as the base offset and the next four as the batch length. Handed a
// wrapped batch it reads the inner base offset as a leader epoch, a base
// offset byte as the magic, and the remainder of the base offset as the
// checksum, then rejects the batch.
//
// Since a consumer cannot read what the producer wrote, the broker has to
// normalise. Re-framing costs one header copy per batch and makes data
// readable by the target client regardless of which client produced it.
//
// The offsets are not left alone: the bare batch's base offset is rewritten to
// the log offset the caller asked for, which is the whole point of the read
// path.
func UnwrapBatches(blob []byte, baseOffset int64) ([]byte, bool) {
	if !NeedsUnwrapping(blob) {
		return blob, false
	}

	var out []byte
	next := baseOffset
	pos := 0
	for pos+msgSetEntryHeaderLen <= len(blob) {
		size := int32(binary.BigEndian.Uint32(blob[pos+8 : pos+12]))
		if size < 0 || pos+msgSetEntryHeaderLen+int(size) > len(blob) {
			break
		}
		body := blob[pos+msgSetEntryHeaderLen : pos+msgSetEntryHeaderLen+int(size)]
		if !isRecordBatch(body) {
			// A legacy message, or something unrecognised: leave it alone
			// rather than guess.
			break
		}
		batch := make([]byte, len(body))
		copy(batch, body)
		_ = setRecordBatchBaseOffset(batch, next)
		out = append(out, batch...)
		next += int64(countEntry(body))
		pos += msgSetEntryHeaderLen + int(size)
	}
	if len(out) == 0 {
		return blob, false
	}
	return out, true
}

// NeedsUnwrapping reports whether a fetched blob contains a wrapped RecordBatch
// and therefore needs re-framing before it can be served.
func NeedsUnwrapping(blob []byte) bool {
	if isRecordBatch(blob) {
		return false
	}
	pos := 0
	for pos+msgSetEntryHeaderLen <= len(blob) {
		size := int32(binary.BigEndian.Uint32(blob[pos+8 : pos+12]))
		if size < 0 || pos+msgSetEntryHeaderLen+int(size) > len(blob) {
			return false
		}
		if isRecordBatch(blob[pos+msgSetEntryHeaderLen : pos+msgSetEntryHeaderLen+int(size)]) {
			return true
		}
		pos += msgSetEntryHeaderLen + int(size)
	}
	return false
}
