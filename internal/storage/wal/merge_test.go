package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// writeSegment lays down a segment of consecutive entries starting at base.
func writeSegment(t *testing.T, dir, name string, base int64, count int, bodySize int) string {
	t.Helper()
	path := filepath.Join(dir, name)
	var buf bytes.Buffer
	for i := int64(0); i < int64(count); i++ {
		off := base + i
		hdr := make([]byte, segmentHeaderLen)
		binary.BigEndian.PutUint64(hdr[0:8], uint64(off))
		binary.BigEndian.PutUint32(hdr[8:12], uint32(bodySize))
		buf.Write(hdr)
		body := make([]byte, bodySize)
		binary.BigEndian.PutUint64(body[0:8], uint64(off)) // identifiable per record
		for j := 8; j < bodySize; j++ {
			body[j] = byte(off % 251)
		}
		buf.Write(body)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// readEntries decodes a segment back into offset -> body, so a merged file can
// be compared against the originals entry by entry.
func readEntries(t *testing.T, path string) map[int64][]byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	out := map[int64][]byte{}
	for pos := 0; pos+segmentHeaderLen <= len(raw); {
		off := int64(binary.BigEndian.Uint64(raw[pos : pos+8]))
		size := int32(binary.BigEndian.Uint32(raw[pos+8 : pos+12]))
		body := raw[pos+segmentHeaderLen : pos+segmentHeaderLen+int(size)]
		out[off] = append([]byte(nil), body...)
		pos += segmentHeaderLen + int(size)
	}
	return out
}

// Merging must preserve every record byte for byte. A merge that loses a record
// is unrecoverable, so this compares decoded entries rather than file sizes.
func TestMergeSegments_PreservesEveryRecord(t *testing.T) {
	dir := t.TempDir()
	var sources []string
	before := map[int64][]byte{}

	base := int64(1000)
	for s := 0; s < 5; s++ {
		p := writeSegment(t, dir, fmt.Sprintf("seg%d.log", s), base, 4, 64)
		sources = append(sources, p)
		for off, body := range readEntries(t, p) {
			before[off] = body
		}
		base += 4
	}

	dst := filepath.Join(dir, "merged.log")
	res, err := MergeSegments(sources, dst, 1000)
	if err != nil {
		t.Fatalf("MergeSegments: %v", err)
	}

	after := readEntries(t, dst)
	if len(after) != len(before) {
		t.Fatalf("merged segment holds %d entries, want %d", len(after), len(before))
	}
	for off, want := range before {
		got, ok := after[off]
		if !ok {
			t.Errorf("offset %d missing from the merged segment", off)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("offset %d: body changed across the merge", off)
		}
	}
	if res.Entries != int64(len(before)) {
		t.Errorf("MergeResult.Entries = %d, want %d", res.Entries, len(before))
	}
	if res.FirstOffset != 1000 || res.LastOffset != 1019 {
		t.Errorf("merged range %d..%d, want 1000..1019", res.FirstOffset, res.LastOffset)
	}
}

// A gap between the segments passed in is not detected by the merge, and that
// is deliberate: offsets inside a segment need not be consecutive, so the only
// check available without the index is that entries ascend. A caller building
// the list from the index knows the ranges are adjacent and must assert it
// there.
//
// This test pins the weaker invariant the merge actually enforces, so the
// distinction stays explicit rather than being rediscovered later.
func TestMergeSegments_AllowsNonConsecutiveOffsets(t *testing.T) {
	dir := t.TempDir()
	a := writeSegment(t, dir, "a.log", 100, 3, 32)
	b := writeSegment(t, dir, "b.log", 110, 3, 32) // 109 -> 110, nothing at 103..109

	dst := filepath.Join(dir, "merged.log")
	if _, err := MergeSegments([]string{a, b}, dst, 100); err != nil {
		t.Fatalf("merge rejected a sparse but ascending range: %v", err)
	}
	got := readEntries(t, dst)
	for _, off := range []int64{100, 101, 102, 110, 111, 112} {
		if _, ok := got[off]; !ok {
			t.Errorf("offset %d missing from the merged segment", off)
		}
	}
}

// Overlapping segments are the same failure: a re-claimed partition writes the
// same base offset under a newer epoch.
func TestMergeSegments_RejectsOverlap(t *testing.T) {
	dir := t.TempDir()
	a := writeSegment(t, dir, "a.log", 100, 5, 32)
	b := writeSegment(t, dir, "b.log", 102, 3, 32)

	dst := filepath.Join(dir, "merged.log")
	if _, err := MergeSegments([]string{a, b}, dst, 100); err == nil {
		t.Fatal("merge succeeded across overlapping offsets; want an error")
	}
}

// A single segment is not a merge, and saying so is better than copying a file
// and reporting a compaction that reclaimed nothing.
func TestMergeSegments_RejectsSingleSource(t *testing.T) {
	dir := t.TempDir()
	a := writeSegment(t, dir, "a.log", 100, 2, 32)
	if _, err := MergeSegments([]string{a}, filepath.Join(dir, "m.log"), 100); err == nil {
		t.Fatal("merging one segment reported success; want an error")
	}
}

// A body larger than the copy buffer must survive intact, since the real
// partition carries records well past 1MB.
func TestMergeSegments_LargeEntry(t *testing.T) {
	dir := t.TempDir()
	a := writeSegment(t, dir, "a.log", 10, 2, 8)
	b := writeSegment(t, dir, "b.log", 12, 1, 3*mergeCopyBuffer)

	before := readEntries(t, b)
	dst := filepath.Join(dir, "merged.log")
	if _, err := MergeSegments([]string{a, b}, dst, 10); err != nil {
		t.Fatalf("MergeSegments: %v", err)
	}
	after := readEntries(t, dst)
	for off, want := range before {
		if !bytes.Equal(after[off], want) {
			t.Errorf("offset %d: %d-byte body did not survive the merge", off, len(want))
		}
	}
}

// A truncated source must fail loudly. Copying a partial entry would produce a
// segment whose last record is short, which a consumer would read as a
// truncated message rather than as a missing one.
func TestMergeSegments_RejectsTruncatedSource(t *testing.T) {
	dir := t.TempDir()
	a := writeSegment(t, dir, "a.log", 10, 3, 32)
	b := writeSegment(t, dir, "b.log", 13, 3, 32)

	raw, err := os.ReadFile(b)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, raw[:len(raw)-10], 0o644); err != nil {
		t.Fatal(err)
	}

	dst := filepath.Join(dir, "merged.log")
	if _, err := MergeSegments([]string{a, b}, dst, 10); err == nil {
		t.Fatal("merge succeeded over a truncated segment; want an error")
	}
}
