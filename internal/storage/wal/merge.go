package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// SegmentMerger combines consecutive sealed segments into one, preserving every
// record exactly.
//
// It exists because a partition with acks=all traffic seals its active segment
// on the flush interval rather than on size. On knode3 that produced segments of
// about 50KB against a 64MB roll threshold -- roughly 3,200 objects an hour,
// each carrying a handful of records and each needing its own index entry,
// object-store PUT and sidecar.
//
// Merging is safe to do online and preserves all data because the log format is
// a flat sequence of self-delimiting entries:
//
//	8-byte offset | 4-byte size | body
//
// The header carries the record's own absolute log offset, so entries copied
// from several segments remain correct at their new position: nothing is
// re-addressed and no offset is renumbered. A reader locates a record by
// scanning headers forward from the start of the segment, which is exactly what
// MergeSegments preserves.
const (
	segmentHeaderLen = 12
	// mergeCopyBuffer is the staging buffer for one entry body. Entries are
	// bounded by maxEntrySize, and copying through a fixed buffer rather than
	// allocating per entry keeps a merge of thousands of segments from turning
	// into a garbage-collection storm.
	mergeCopyBuffer = 1 << 20
)

// MergeResult reports what a merge produced, for logging and for the index
// update that follows it.
type MergeResult struct {
	FirstOffset int64
	LastOffset  int64
	Sources     int
	BytesIn     int64
	BytesOut    int64
	Entries     int64
}

// MergeSegments concatenates the given segments, in the order given, into a
// single segment at dstPath.
//
// The first entry's offset becomes the merged segment's base. The segments must
// be contiguous and ascending: the caller has the index and can assert that,
// whereas a gap would produce a segment that silently omits offsets, which is
// worse than not merging at all.
//
// Nothing is removed. The caller deletes the sources only after the merged
// object is durably in place and recorded in the index, so a crash leaves
// duplicate data rather than a hole.
func MergeSegments(sources []string, dstPath string, baseOffset int64) (*MergeResult, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("merge: no source segments")
	}
	if len(sources) == 1 {
		return nil, fmt.Errorf("merge: a single segment is not a merge")
	}

	out, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("merge: create %s: %w", dstPath, err)
	}
	defer func() { _ = out.Close() }()

	res := &MergeResult{FirstOffset: baseOffset}
	buf := make([]byte, mergeCopyBuffer)
	// -1 means "accept whatever the first entry is". After that the check is
	// monotonic rather than consecutive: offsets inside a segment need not be
	// contiguous -- a record batch can start at any offset -- so demanding
	// last+1 would reject valid input.
	var prevLast int64 = -1

	for _, src := range sources {
		f, err := os.Open(src)
		if err != nil {
			return nil, fmt.Errorf("merge: open %s: %w", src, err)
		}

		stat, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("merge: stat %s: %w", src, err)
		}
		res.Sources++
		res.BytesIn += stat.Size()

		first, last, entries, err := copySegment(f, out, buf, &res.BytesOut, prevLast)
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("merge: %s: %w", src, err)
		}
		res.Entries += entries
		if res.LastOffset == 0 {
			res.FirstOffset = first
		}
		res.LastOffset = last
		prevLast = last
	}

	if err := out.Sync(); err != nil {
		return nil, fmt.Errorf("merge: sync %s: %w", dstPath, err)
	}
	return res, nil
}

// copySegment appends every entry in src to w and returns the first and last
// offsets it wrote, plus the entry count.
//
// Contiguity is checked rather than assumed. wantNext is the offset that must
// come next, or -1 to accept whatever the first entry is. A mismatch means the
// caller passed segments with a gap or an overlap, and writing the result would
// produce a segment that lies about which offsets it holds.
func copySegment(src io.Reader, w io.Writer, buf []byte, bytesOut *int64, prev int64) (first, last, entries int64, err error) {
	header := make([]byte, segmentHeaderLen)

	for {
		n, err := io.ReadFull(src, header)
		if err != nil {
			// ReadFull reports a clean boundary as ErrUnexpectedEOF, not EOF:
			// it saw 0 of the 12 header bytes it wanted. Anything else -- a
			// partial header, or a real error -- is a truncated or unreadable
			// source. The writer only ever seals a flushed segment, so this is
			// corruption rather than a partial write, and copying it would leave
			// a merged segment whose last record is short.
			if n == 0 {
				break
			}
			return 0, 0, 0, fmt.Errorf("truncated entry header after %d bytes: %w", n, err)
		}

		off := int64(binary.BigEndian.Uint64(header[0:8]))
		size := int32(binary.BigEndian.Uint32(header[8:12]))
		if size < 0 || int64(size) > maxEntrySize {
			return 0, 0, 0, fmt.Errorf("implausible entry size %d at offset %d", size, off)
		}

		// Entries must ascend and must not go backwards. A source list built
		// from the index is ordered by base offset, so anything else means the
		// caller passed segments that overlap or are out of order -- which would
		// leave the merged segment holding two records at one offset.
		if prev >= 0 && off <= prev {
			return 0, 0, 0, fmt.Errorf("offsets out of order: %d follows %d", off, prev)
		}

		if entries == 0 {
			first = off
		}
		last = off
		entries++
		prev = off

		// The header is copied verbatim: its offset is absolute in the log's
		// offset space and stays correct at the new position.
		if _, err := w.Write(header); err != nil {
			return 0, 0, 0, err
		}
		*bytesOut += segmentHeaderLen

		remaining := int64(size)
		for remaining > 0 {
			n := int64(len(buf))
			if remaining < n {
				n = remaining
			}
			read, rerr := io.ReadFull(src, buf[:n])
			if rerr != nil {
				return 0, 0, 0, fmt.Errorf("truncated entry body at offset %d: %w", off, rerr)
			}
			if _, werr := w.Write(buf[:read]); werr != nil {
				return 0, 0, 0, werr
			}
			*bytesOut += int64(read)
			remaining -= int64(read)
		}
	}

	return first, last, entries, nil
}
