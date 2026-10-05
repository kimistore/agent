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

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kimistore/internal/storage/wal"
)

// recordBatch builds a real RecordBatch (magic 2) carrying n records, in the
// message-set-wrapped form a Java or librdkafka producer would send. Tests
// need genuine wire bytes: a fixture that agrees with the parser's mistakes
// cannot catch them.
// zigzag encodes a signed integer the way Kafka record batches do.
func zigzag(v int32) byte { return byte((v << 1) ^ (v >> 31)) }

func recordBatch(baseOffset int64, n int32) []byte {
	const headerLen = 61

	var records bytes.Buffer
	for i := int32(0); i < n; i++ {
		// Record fields are zigzag varints. One record is:
		//   length(1) attributes(1) timestampDelta(1) offsetDelta(1)
		//   keyLength(-1 => null) valueLength value headerCount(0)
		// which is 7 bytes of body, so the length varint is zigzag(7) = 14.
		records.WriteByte(0x0E)      // record length
		records.WriteByte(0x00)      // record attributes
		records.WriteByte(0x00)      // timestamp delta
		records.WriteByte(zigzag(i)) // offset delta
		records.WriteByte(0x01)      // key length: zigzag(-1), a null key
		records.WriteByte(0x02)      // value length: zigzag(1)
		records.WriteByte(byte('v')) // value
		records.WriteByte(0x00)      // header count
	}
	rec := records.Bytes()

	batchLength := headerLen - 12 + len(rec)
	buf := make([]byte, 0, 12+batchLength)
	var t8 [8]byte
	var t2 [2]byte

	binary.BigEndian.PutUint64(t8[:8], uint64(baseOffset))
	buf = append(buf, t8[:8]...)
	binary.BigEndian.PutUint32(t8[:4], uint32(batchLength))
	buf = append(buf, t8[:4]...)
	binary.BigEndian.PutUint32(t8[:4], 0xFFFFFFFF) // partitionLeaderEpoch
	buf = append(buf, t8[:4]...)
	buf = append(buf, 2)                  // magic
	binary.BigEndian.PutUint32(t8[:4], 0) // crc placeholder
	buf = append(buf, t8[:4]...)
	buf = append(buf, t2[:2]...) // attributes
	binary.BigEndian.PutUint32(t8[:4], uint32(n-1))
	buf = append(buf, t8[:4]...)
	binary.BigEndian.PutUint64(t8[:8], 0)
	buf = append(buf, t8[:8]...) // firstTimestamp
	binary.BigEndian.PutUint64(t8[:8], 0)
	buf = append(buf, t8[:8]...) // maxTimestamp
	binary.BigEndian.PutUint64(t8[:8], 0xFFFFFFFFFFFFFFFF)
	buf = append(buf, t8[:8]...) // producerId
	binary.BigEndian.PutUint16(t2[:2], 0xFFFF)
	buf = append(buf, t2[:2]...) // producerEpoch
	binary.BigEndian.PutUint32(t8[:4], 0xFFFFFFFF)
	buf = append(buf, t8[:4]...) // baseSequence
	binary.BigEndian.PutUint32(t8[:4], uint32(n))
	buf = append(buf, t8[:4]...) // recordsCount
	buf = append(buf, rec...)

	sum := crc32.Checksum(buf[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(buf[17:21], sum)

	// Wrap it in a message set entry, the way the Java client, librdkafka and
	// sarama do.
	wrapped := make([]byte, 12, 12+len(buf))
	binary.BigEndian.PutUint64(wrapped[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(wrapped[8:12], uint32(len(buf)))
	wrapped = append(wrapped, buf...)
	return wrapped
}

func batchOfN(n int) []byte { return recordBatch(0, int32(n)) }

// waitForSegment polls the object store until a sealed segment shows up, so a
// test can assert on the state a restart actually encounters.
func waitForSegment(t *testing.T, store *rehydrateStore, key string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rc, err := store.Get(context.Background(), key); err == nil {
			rc.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("segment %s was never uploaded", key)
}

// localSealedSegments counts sealed (non-active) segments on local disk.
func localSealedSegments(t *testing.T, walDir string) int {
	t.Helper()
	n := 0
	_ = filepath.WalkDir(walDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".log") && d.Name() != "active.log" {
			n++
		}
		return nil
	})
	return n
}

// TestRestartContinuesLogEndOffset is the regression guard for the failure
// that let a restart hand out offsets that were already taken.
//
// Sealed segments are deleted once uploaded, so after a restart the local WAL
// is empty. An engine recovering its log end offset from local files alone
// rewinds every partition to 0, and the next producer is told its records
// landed at offsets whose data already sits in object storage.
func TestRestartContinuesLogEndOffset(t *testing.T) {
	store := newRehydrateStore()

	// Roll small so a single append seals a segment and the uploader moves
	// it to object storage, which is the state a restart really finds.
	restore := sealAfterEveryAppend(t)
	defer restore()

	dir := t.TempDir()
	se, err := NewStorageEngine(dir, store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := se.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	if _, err := se.Append("orders", 0, batchOfN(500), 500, true); err != nil {
		t.Fatalf("append partition 0: %v", err)
	}
	if _, err := se.Append("orders", 1, batchOfN(700), 700, true); err != nil {
		t.Fatalf("append partition 1: %v", err)
	}
	// One more append each to trigger the roll of the previous segment.
	if _, err := se.Append("orders", 0, batchOfN(1), 1, true); err != nil {
		t.Fatalf("append partition 0 (roll): %v", err)
	}
	if _, err := se.Append("orders", 1, batchOfN(1), 1, true); err != nil {
		t.Fatalf("append partition 1 (roll): %v", err)
	}

	waitForSegment(t, store, "orders/0/00000000000000000000.log")
	waitForSegment(t, store, "orders/1/00000000000000000000.log")

	if n := localSealedSegments(t, dir); n != 0 {
		t.Fatalf("expected the local WAL to be drained, %d sealed segment(s) left", n)
	}
	se.Close()

	// Second incarnation, with an empty local WAL directory and no manifest,
	// so the position has to be recovered from the segments themselves.
	se2, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("engine 2: %v", err)
	}
	defer se2.Close()
	se2.SetCoordinator(nil)

	// 500 + 1 and 700 + 1: the roll-triggering append counts too.
	if got, want := se2.HighWaterMark("orders", 0), int64(501); got != want {
		t.Errorf("partition 0 high watermark after restart = %d, want %d", got, want)
	}
	if got, want := se2.HighWaterMark("orders", 1), int64(701); got != want {
		t.Errorf("partition 1 high watermark after restart = %d, want %d", got, want)
	}

	parts, err := se2.GetPartitions("orders")
	if err != nil {
		t.Fatalf("get partitions: %v", err)
	}
	if len(parts) != 2 {
		t.Errorf("partitions after restart = %v, want [0 1]; a collapsed partition set starves consumers", parts)
	}

	off, err := se2.Append("orders", 0, batchOfN(10), 10, true)
	if err != nil {
		t.Fatalf("append after restart: %v", err)
	}
	if off != 501 {
		t.Errorf("offset after restart = %d, want 501 (must not collide with stored data)", off)
	}
}

// TestRestartFromManifest covers the cheaper path: the manifest written on
// every checkpoint records the position directly.
func TestRestartFromManifest(t *testing.T) {
	store := newRehydrateStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := se.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := se.Append("orders", 0, batchOfN(42), 42, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := se.SaveManifest(); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	se.Close()

	se2, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("engine 2: %v", err)
	}
	defer se2.Close()
	se2.SetCoordinator(nil)

	if !se2.TopicExists("orders") {
		t.Error("topic not recovered from object storage")
	}
	if got := se2.HighWaterMark("orders", 0); got != 42 {
		t.Errorf("recovered high watermark = %d, want 42", got)
	}
}

// TestListOffsetsLogStartAfterRetention covers the livelock: a consumer that
// resets to "earliest" must be handed an offset that still exists.
func TestListOffsetsLogStartAfterRetention(t *testing.T) {
	store := newRehydrateStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer se.Close()

	if err := se.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := se.Append("orders", 0, batchOfN(40), 40, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	se.metadataCache.SetPartitionState("orders", 0, 40, 20, nil)

	if got := se.LogStartOffset("orders", 0); got != 20 {
		t.Errorf("log start = %d, want 20", got)
	}
	// A fetch below the log start must be refused, not served or reported as
	// in range, otherwise the client retries the same dead offset forever.
	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()
}

// TestCheckpointRestoresCommittedOffsets pins the fix for a field that was
// written on every checkpoint and read back on none.
func TestCheckpointRestoresCommittedOffsets(t *testing.T) {
	store := newRehydrateStore()

	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	se.SaveOffset("g1", "orders", 0, 5)
	se.SaveOffset("g2", "orders", 0, 20)
	if err := se.SaveCheckpoint(); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	se.Close()

	raw, err := store.Get(context.Background(), "_meta/checkpoint.json")
	if err != nil {
		t.Fatalf("checkpoint missing: %v", err)
	}
	body, _ := io.ReadAll(raw)
	raw.Close()
	if !strings.Contains(string(body), "committed_offsets") {
		t.Fatalf("checkpoint does not carry committed offsets: %s", body)
	}

	se2, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("engine 2: %v", err)
	}
	defer se2.Close()
	if err := se2.LoadCheckpoint(); err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}

	if got := se2.FirstAllowedOffset("orders", 0); got != 5 {
		t.Errorf("FirstAllowedOffset after checkpoint restore = %d, want 5", got)
	}
}

// TestReadBatchFillsBudget checks a fetch returns several records rather than
// one per round trip.
func TestReadBatchFillsBudget(t *testing.T) {
	store := newRehydrateStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer se.Close()

	if err := se.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := se.Append("orders", 0, recordBatch(int64(i*10), 10), 10, true); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// A generous budget must collapse several batches into one answer.
	data, next, err := se.ReadBatch("orders", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if next <= 10 {
		t.Errorf("next offset = %d, want more than 10: a single record per fetch is a request loop", next)
	}
	if len(data) == 0 {
		t.Error("empty read")
	}

	// A tight budget must be respected, or a client's receive buffer
	// overflows and it drops the connection.
	tight, tightNext, err := se.ReadBatch("orders", 0, 0, int64(len(batchOfN(10))+1))
	if err != nil {
		t.Fatalf("ReadBatch tight: %v", err)
	}
	if len(tight) > len(batchOfN(10))+1 {
		t.Errorf("read %d bytes, budget was %d", len(tight), len(batchOfN(10))+1)
	}
	if tightNext != 10 {
		t.Errorf("tight next offset = %d, want 10", tightNext)
	}
}

// sealAfterEveryAppend shrinks the segment roll threshold so a single append
// seals a segment and hands it to the uploader, which is the state a restart
// actually encounters. It returns a function that restores the threshold.
func sealAfterEveryAppend(t *testing.T) func() {
	t.Helper()
	previous := wal.MaxSegmentSize
	wal.MaxSegmentSize = 1
	return func() { wal.MaxSegmentSize = previous }
}
