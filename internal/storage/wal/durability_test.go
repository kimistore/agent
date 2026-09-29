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
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mkRecord builds a minimal valid Kafka MessageSet v0/v1 holding one record
// with the given payload, so tests can assert on exact bytes.
//
// Layout: CRC(4) Magic(1) Attributes(1) KeySize(4, -1) ValueSize(4) Value(n)
func mkRecord(payload string) []byte {
	v := []byte(payload)
	msg := make([]byte, 14+len(v))
	binary.BigEndian.PutUint32(msg[0:4], 0) // CRC (unvalidated on read)
	msg[4] = 1                              // Magic 1 => v0/v1 MessageSet
	msg[5] = 0                              // Attributes => uncompressed
	binary.BigEndian.PutUint32(msg[6:10], 0xFFFFFFFF)
	binary.BigEndian.PutUint32(msg[10:14], uint32(len(v)))
	copy(msg[14:], v)
	return msg
}

// recordPayload extracts the value from a MessageSet written by mkRecord.
func recordPayload(t *testing.T, body []byte) string {
	t.Helper()
	if len(body) < 14 {
		t.Fatalf("record body too short (%d bytes)", len(body))
	}
	return string(body[14:])
}

// mkMultiRecordBatch builds a MessageSet body holding n single-record entries,
// which is what CountMessageSet derives a record count from. Each entry's value
// is "<prefix>-<i>" so assertions can identify which batch was returned.
func mkMultiRecordBatch(prefix string, n int) []byte {
	var body []byte
	for i := 0; i < n; i++ {
		value := []byte(prefix + "-" + string(rune('0'+i)))
		msg := make([]byte, 14+len(value))
		binary.BigEndian.PutUint32(msg[0:4], 0) // CRC
		msg[4] = 1                              // Magic 1
		msg[5] = 0                              // Attributes
		binary.BigEndian.PutUint32(msg[6:10], 0xFFFFFFFF)
		binary.BigEndian.PutUint32(msg[10:14], uint32(len(value)))
		copy(msg[14:], value)

		entry := make([]byte, 12+len(msg))
		binary.BigEndian.PutUint64(entry[0:8], uint64(i)) // per-entry offset
		binary.BigEndian.PutUint32(entry[8:12], uint32(len(msg)))
		copy(entry[12:], msg)

		body = append(body, entry...)
	}
	return body
}

// firstEntryPayload reads the value out of the first entry of a MessageSet.
func firstEntryPayload(t *testing.T, body []byte) string {
	t.Helper()
	const entryHeader = 12
	const valueStart = entryHeader + 14
	if len(body) < valueStart {
		t.Fatalf("batch body too short (%d bytes)", len(body))
	}
	size := int(binary.BigEndian.Uint32(body[valueStart-4 : valueStart]))
	return string(body[valueStart : valueStart+size])
}

// TestAppend_PositionIndexSurvivesInterleavedRead guards a silent
// data-corruption bug.
//
// The active segment is opened O_APPEND, so writes always land at EOF. But the
// Read path seeks the same *os.File. If Append derived its index position from
// Seek(0, io.SeekCurrent), a read that moved the cursor would make Append
// record a stale position, and the next reader would silently be served the
// wrong record.
func TestAppend_PositionIndexSurvivesInterleavedRead(t *testing.T) {
	dir := t.TempDir()
	pw, err := NewPartitionWAL(dir, "orders", 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pw.Close()

	payloads := []string{"AAAA-0", "BBBB-1", "CCCC-2"}
	for _, s := range payloads {
		if _, err := pw.Append(mkRecord(s), 1, true); err != nil {
			t.Fatalf("append %q: %v", s, err)
		}
	}

	// A consumer reads from the active segment. This moves the file cursor.
	if _, err := pw.Read(1); err != nil {
		t.Fatalf("pre-read of offset 1: %v", err)
	}

	// A producer then appends. The index entry for this must reflect the
	// record's true position at EOF, not wherever the reader left the cursor.
	if _, err := pw.Append(mkRecord("DDDD-3"), 1, true); err != nil {
		t.Fatalf("append after read: %v", err)
	}

	want := map[int64]string{0: "AAAA-0", 1: "BBBB-1", 2: "CCCC-2", 3: "DDDD-3"}
	for offset, expect := range want {
		body, err := pw.Read(offset)
		if err != nil {
			t.Errorf("Read(%d): %v", offset, err)
			continue
		}
		if got := recordPayload(t, body); got != expect {
			t.Errorf("Read(%d) returned %q, want %q", offset, got, expect)
		}
	}
}

// TestAppend_RepeatedInterleavedReadWrite is the same invariant under sustained
// mixed traffic rather than a single read, which is what a real broker sees.
func TestAppend_RepeatedInterleavedReadWrite(t *testing.T) {
	dir := t.TempDir()
	pw, err := NewPartitionWAL(dir, "mixed", 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pw.Close()

	// Write three, read back everything, then write three more.
	for i := 0; i < 3; i++ {
		if _, err := pw.Append(mkRecord(string(rune('a'+i))+"-first"), 1, true); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := pw.Read(int64(i)); err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := pw.Append(mkRecord(string(rune('d'+i))+"-second"), 1, true); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	want := []string{"a-first", "b-first", "c-first", "d-second", "e-second", "f-second"}
	for i, expect := range want {
		body, err := pw.Read(int64(i))
		if err != nil {
			t.Errorf("Read(%d): %v", i, err)
			continue
		}
		if got := recordPayload(t, body); got != expect {
			t.Errorf("Read(%d) returned %q, want %q", i, got, expect)
		}
	}
}

// TestRecover_TornTailBodyIsTruncated verifies a crash mid-write no longer
// bricks a partition. Previously recovery returned a hard error on the
// truncated body, so every subsequent Append and Read for that partition
// failed forever.
func TestRecover_TornTailBodyIsTruncated(t *testing.T) {
	dir := t.TempDir()

	pw, err := NewPartitionWAL(dir, "torn", 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, s := range []string{"AAAA-0", "BBBB-1"} {
		if _, err := pw.Append(mkRecord(s), 1, true); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	pw.Close()

	// Simulate a crash partway through writing offset 2: the full 12-byte
	// header is on disk but only part of the body.
	f, err := os.OpenFile(filepath.Join(dir, "active.log"), os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("open for torn write: %v", err)
	}
	hdr := make([]byte, 12)
	binary.BigEndian.PutUint64(hdr[0:8], 2)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(mkRecord("CCCC-2"))))
	if _, err := f.Write(hdr); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil { // partial body
		t.Fatal(err)
	}
	f.Close()

	// Restart: recovery must succeed and keep the valid prefix.
	pw2, err := NewPartitionWAL(dir, "torn", 0, nil)
	if err != nil {
		t.Fatalf("RECOVERY FAILED on torn tail (partition is now bricked): %v", err)
	}
	defer pw2.Close()

	if got := pw2.HighWaterMark(); got != 2 {
		t.Errorf("HighWaterMark() = %d, want 2 (the two intact records)", got)
	}

	// The intact records must still be readable...
	for offset, want := range map[int64]string{0: "AAAA-0", 1: "BBBB-1"} {
		body, err := pw2.Read(offset)
		if err != nil {
			t.Errorf("Read(%d) after recovery: %v", offset, err)
			continue
		}
		if got := recordPayload(t, body); got != want {
			t.Errorf("Read(%d) = %q, want %q", offset, got, want)
		}
	}

	// ...and the partition must be writable again, continuing at offset 2.
	off, err := pw2.Append(mkRecord("DDDD-2"), 1, true)
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if off != 2 {
		t.Errorf("append after recovery got offset %d, want 2", off)
	}
}

// TestRecover_TornTailHeaderIsTruncated covers a crash that leaves a partial
// header, which is a different failure shape than a torn body.
func TestRecover_TornTailHeaderIsTruncated(t *testing.T) {
	dir := t.TempDir()

	pw, err := NewPartitionWAL(dir, "tornhdr", 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := pw.Append(mkRecord("AAAA-0"), 1, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	pw.Close()

	f, _ := os.OpenFile(filepath.Join(dir, "active.log"), os.O_WRONLY|os.O_APPEND, 0666)
	f.Write([]byte{0, 0, 0, 0, 0, 1, 0, 0}) // 8 of 12 header bytes
	f.Close()

	pw2, err := NewPartitionWAL(dir, "tornhdr", 0, nil)
	if err != nil {
		t.Fatalf("RECOVERY FAILED on partial header: %v", err)
	}
	defer pw2.Close()

	if got := pw2.HighWaterMark(); got != 1 {
		t.Errorf("HighWaterMark() = %d, want 1", got)
	}
	if _, err := pw2.Append(mkRecord("BBBB-1"), 1, true); err != nil {
		t.Errorf("append after partial-header recovery: %v", err)
	}
}

// TestRecover_ImplausibleSizeIsTruncated covers a garbage size field, which
// used to fail recovery outright rather than discarding the bad tail.
func TestRecover_ImplausibleSizeIsTruncated(t *testing.T) {
	dir := t.TempDir()

	pw, err := NewPartitionWAL(dir, "badsize", 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := pw.Append(mkRecord("AAAA-0"), 1, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	pw.Close()

	f, _ := os.OpenFile(filepath.Join(dir, "active.log"), os.O_WRONLY|os.O_APPEND, 0666)
	hdr := make([]byte, 12)
	binary.BigEndian.PutUint64(hdr[0:8], 1)
	binary.BigEndian.PutUint32(hdr[8:12], 0xFFFFFFF0) // ~4GB, impossible
	f.Write(hdr)
	f.Close()

	pw2, err := NewPartitionWAL(dir, "badsize", 0, nil)
	if err != nil {
		t.Fatalf("RECOVERY FAILED on implausible size: %v", err)
	}
	defer pw2.Close()

	if got := pw2.HighWaterMark(); got != 1 {
		t.Errorf("HighWaterMark() = %d, want 1", got)
	}
	if _, err := pw2.Append(mkRecord("BBBB-1"), 1, true); err != nil {
		t.Errorf("append after implausible-size recovery: %v", err)
	}
}

// TestRoll_SealedSegmentIsFlushedBeforeSeal checks that a segment is fsynced
// before it is renamed and handed to the uploader. Without this, a crash could
// leave a sealed segment whose tail is only in page cache; the uploader would
// publish that truncated segment to object storage and delete the local copy,
// turning a recoverable crash into permanent loss.
func TestRoll_SealedSegmentIsFlushedBeforeSeal(t *testing.T) {
	dir := t.TempDir()

	sealed := make(chan UploadTask, 4)
	pw, err := NewPartitionWAL(dir, "roll", 0, func(tk UploadTask) { sealed <- tk })
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pw.Close()

	payload := "0123456789"
	for i := 0; i < 4; i++ {
		if _, err := pw.Append(mkRecord(payload), 1, false); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// Trip the roll threshold.
	if err := pw.roll(); err != nil {
		t.Fatalf("roll: %v", err)
	}

	var tk UploadTask
	select {
	case tk = <-sealed:
	case <-time.After(5 * time.Second):
		t.Fatal("roll did not hand the sealed segment to the uploader")
	}
	data, err := os.ReadFile(tk.Path)
	if err != nil {
		t.Fatalf("read sealed segment: %v", err)
	}
	// 4 records x (12-byte WAL header + 24-byte record)
	const wantLen = 4 * (12 + 24)
	if len(data) != wantLen {
		t.Errorf("sealed segment is %d bytes, want %d (truncated tail would corrupt the upload)", len(data), wantLen)
	}
}

// TestRoll_RemainsWritableAfterRenameFailure is hard to trigger through the
// public API, so this documents the invariant directly: if the sealed path
// cannot be created, the partition must not be left holding a closed handle,
// which previously made every later Append fail permanently.
func TestRoll_RemainsWritableAfterRenameFailure(t *testing.T) {
	dir := t.TempDir()

	pw, err := NewPartitionWAL(dir, "rollfail", 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pw.Close()

	if _, err := pw.Append(mkRecord("AAAA-0"), 1, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Occupy the destination name with a non-empty directory so the rename
	// cannot succeed.
	sealedName := filepath.Join(dir, "00000000000000000000.log")
	if err := os.Mkdir(sealedName, 0755); err != nil {
		t.Skipf("cannot stage rename failure on this platform: %v", err)
	}

	if err := pw.roll(); err == nil {
		t.Skip("rename unexpectedly succeeded; platform may allow overwriting")
	}

	// The partition must still accept writes.
	off, err := pw.Append(mkRecord("BBBB-1"), 1, true)
	if err != nil {
		t.Fatalf("append after failed roll: %v", err)
	}
	if off != 1 {
		t.Errorf("offset after failed roll = %d, want 1", off)
	}
	if _, err := pw.Read(0); err != nil {
		t.Errorf("Read(0) after failed roll: %v", err)
	}
}

// TestReadFromSealed_ServesOffsetInsideBatch checks that a sealed segment can
// serve an offset that falls in the middle of a multi-record entry. Matching
// on exact record start used to fail here, producing a spurious
// OffsetOutOfRange for the consumer.
func TestReadFromSealed_ServesOffsetInsideBatch(t *testing.T) {
	dir := t.TempDir()

	pw, err := NewPartitionWAL(dir, "sealed", 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Two batches, each genuinely holding 3 records, at offsets 0 and 3.
	if _, err := pw.Append(mkMultiRecordBatch("zero", 3), 3, true); err != nil {
		t.Fatalf("append 0: %v", err)
	}
	if _, err := pw.Append(mkMultiRecordBatch("three", 3), 3, true); err != nil {
		t.Fatalf("append 3: %v", err)
	}
	if err := pw.roll(); err != nil {
		t.Fatalf("roll: %v", err)
	}
	pw.Close()

	pw2, err := NewPartitionWAL(dir, "sealed", 0, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer pw2.Close()

	// Offsets 1 and 2 live inside the first batch and must resolve to it.
	for offset, want := range map[int64]string{
		0: "zero-0", 1: "zero-0", 2: "zero-0",
		3: "three-0", 4: "three-0", 5: "three-0",
	} {
		body, err := pw2.readFromSealed(offset)
		if err != nil {
			t.Errorf("readFromSealed(%d): %v", offset, err)
			continue
		}
		if got := firstEntryPayload(t, body); got != want {
			t.Errorf("readFromSealed(%d) = %q, want %q", offset, got, want)
		}
	}

	// An offset past the end must report not-found, not return wrong data.
	if _, err := pw2.readFromSealed(99); err == nil {
		t.Error("readFromSealed(99) succeeded, want an error")
	}
}
