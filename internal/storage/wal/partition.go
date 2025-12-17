package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
// MaxSegmentSize is the threshold to roll a segment.
// Increased to 64MB to reduce invalid S3 PUT costs.
)

var MaxSegmentSize = int64(64 * 1024 * 1024) // 64MB

type PartitionWAL struct {
	dir              string
	activeFile       *os.File
	activeBaseOffset int64
	currentSize      int64

	nextOffset int64
	index      map[int64]int64 // Offset -> Position
	offsets    []int64         // Sorted list of offsets in active file

	mu sync.Mutex
}

func NewPartitionWAL(dir string) (*PartitionWAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	pw := &PartitionWAL{
		dir:   dir,
		index: make(map[int64]int64),
	}

	if err := pw.loadState(); err != nil {
		return nil, err
	}

	return pw, nil
}

func (p *PartitionWAL) loadState() error {
	// 1. Find if active.log exists
	activePath := filepath.Join(p.dir, "active.log")

	f, err := os.OpenFile(activePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	p.activeFile = f

	info, err := f.Stat()
	if err != nil {
		return err
	}
	p.currentSize = info.Size()

	// 2. Recover index and offsets from active.log
	if err := p.recoverActive(); err != nil {
		return err
	}

	// 3. Determine activeBaseOffset
	// If active file was empty, we need to infer next offset from sealed files.
	// However, if we preserve activeBaseOffset in a metadata file it would be safer.
	// For now, let's scan sealed files to find the max offset if active is empty.
	if p.nextOffset == 0 {
		// Find max encoded offset in dir
		maxEndOffset, err := p.findMaxSealedOffset()
		if err != nil {
			return err
		}
		p.nextOffset = maxEndOffset
		p.activeBaseOffset = maxEndOffset
	} else if p.currentSize > 0 {
		// We read some records, so activeBaseOffset should be the first record's offset.
		// We can get it from the index.
		minOff := int64(-1)
		for off := range p.index {
			if minOff == -1 || off < minOff {
				minOff = off
			}
		}
		if minOff != -1 {
			p.activeBaseOffset = minOff
		} else {
			// Should happen only if file has data but recover failed?
			// Or maybe we treat empty active file as extension of last sealed.
			// Assume initialized.
		}
	} else {
		// Active file empty, but p.nextOffset might be 0.
		// Need to check sealed files again
		maxEnd, _ := p.findMaxSealedOffset()
		p.nextOffset = maxEnd
		p.activeBaseOffset = maxEnd
	}

	return nil
}

func (p *PartitionWAL) findMaxSealedOffset() (int64, error) {
	files, err := os.ReadDir(p.dir)
	if err != nil {
		return 0, err
	}

	var offsets []int64
	for _, file := range files {
		if file.IsDir() || file.Name() == "active.log" {
			continue
		}
		if strings.HasSuffix(file.Name(), ".log") {
			name := strings.TrimSuffix(file.Name(), ".log")
			off, err := strconv.ParseInt(name, 10, 64)
			if err == nil {
				offsets = append(offsets, off)
			}
		}
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

	if len(offsets) == 0 {
		return 0, nil
	}

	// To know the *end* offset of the last sealed segment, we'd have to read it.
	// Simpler hack: We name files by StartOffset.
	// If we have 0.log, 100.log.
	// nextOffset is effectively unknown without reading the last file.
	// So let's open the last file and read it to end.

	lastStart := offsets[len(offsets)-1]
	lastPath := filepath.Join(p.dir, fmt.Sprintf("%020d.log", lastStart))

	f, err := os.Open(lastPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Scan to end
	// Use a lighter scanner if possible, or just reuse recovering logic?
	// Let's just assume we can trust the file content.

	// Optimization: Read last 12 bytes? No, variable size records.
	// Must scan.

	endOffset := lastStart
	// ... Scanning implementation ...
	// For MVP, if we restart, scanning all might be slow but safe.

	f.Seek(0, 0)
	for {
		header := make([]byte, 12)
		if _, err := io.ReadFull(f, header); err != nil {
			break
		}
		offset := int64(binary.BigEndian.Uint64(header[0:8]))
		size := binary.BigEndian.Uint32(header[8:12])

		// To correctly determine endOffset, we must read the body and count
		body := make([]byte, size)
		if _, err := io.ReadFull(f, body); err != nil {
			break
		}

		count := CountMessageSet(body)
		if count == 0 {
			count = 1
		} // Safety

		next := offset + int64(count)
		if next > endOffset {
			endOffset = next
		}
	}

	return endOffset, nil
}

func (p *PartitionWAL) recoverActive() error {
	p.activeFile.Seek(0, 0)
	pos := int64(0)
	p.offsets = nil

	for {
		header := make([]byte, 12)
		if _, err := io.ReadFull(p.activeFile, header); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		offset := int64(binary.BigEndian.Uint64(header[0:8]))
		size := binary.BigEndian.Uint32(header[8:12])

		p.index[offset] = pos
		p.offsets = append(p.offsets, offset)

		// Read body to count
		body := make([]byte, size)
		if _, err := io.ReadFull(p.activeFile, body); err != nil {
			return err
		}

		count := CountMessageSet(body)
		if count == 0 {
			count = 1
		}

		next := offset + int64(count)
		if next > p.nextOffset {
			p.nextOffset = next
		}

		pos += 12 + int64(size)
	}

	// offsets are appended in order naturally from log scan
	return nil
}

func (p *PartitionWAL) Append(batch []byte, recordCount int) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check for roll
	if p.currentSize > MaxSegmentSize {
		if err := p.roll(); err != nil {
			return 0, err
		}
	}

	// If activeBaseOffset is not set (first write), set it
	if p.currentSize == 0 {
		p.activeBaseOffset = p.nextOffset
	}

	offset := p.nextOffset
	size := uint32(len(batch))

	buf := make([]byte, 12)
	binary.BigEndian.PutUint64(buf[0:8], uint64(offset))
	binary.BigEndian.PutUint32(buf[8:12], size)

	pos, _ := p.activeFile.Seek(0, 1)

	n1, err := p.activeFile.Write(buf)
	if err != nil {
		return 0, err
	}
	n2, err := p.activeFile.Write(batch)
	if err != nil {
		return 0, err
	}

	p.currentSize += int64(n1 + n2)
	p.index[offset] = pos
	p.offsets = append(p.offsets, offset)

	// Increment nextOffset by the number of records
	if recordCount < 1 {
		recordCount = 1
	} // Safety, though caller should ensure
	p.nextOffset += int64(recordCount)

	return offset, nil
}

func (p *PartitionWAL) roll() error {
	log.Printf("Rolling active segment for %s at offset %d", p.dir, p.nextOffset)

	// Close active
	if err := p.activeFile.Close(); err != nil {
		return err
	}

	// Rename to sealed (using 0-padded activeBaseOffset)
	oldPath := filepath.Join(p.dir, "active.log")
	newPath := filepath.Join(p.dir, fmt.Sprintf("%020d.log", p.activeBaseOffset))

	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}

	// Open new active
	f, err := os.OpenFile(oldPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	p.activeFile = f
	p.currentSize = 0
	p.activeBaseOffset = p.nextOffset

	// Clear index for active segment (past segments are not indexed in memory for MVP)
	p.index = make(map[int64]int64)
	p.offsets = nil

	return nil
}

func (p *PartitionWAL) Read(offset int64) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check active using binary search on offsets
	// Find largest startOffset <= offset
	idx := sort.Search(len(p.offsets), func(i int) bool {
		return p.offsets[i] > offset
	})
	// idx is where p.offsets[i] > offset.
	// So p.offsets[idx-1] <= offset.
	// However, if idx == 0, it means p.offsets[0] > offset, so no element <= offset.
	// If idx == len, it means all elements <= offset.

	searchIdx := idx - 1
	if searchIdx >= 0 && searchIdx < len(p.offsets) {
		startOffset := p.offsets[searchIdx]
		pos := p.index[startOffset]

		// If found, verify it covers the offset
		data, err := p.readFromFile(p.activeFile, pos)
		if err == nil {
			// Check coverage requires count.
			count := CountMessageSet(data)
			if count == 0 {
				count = 1
			}
			if startOffset+int64(count) > offset {
				return data, nil
			}
		}
	}

	// Not in active. Check sealed files?
	// For MVP, we only serve from Active WAL explicitly here?
	// Plan says: "Locate: Determine if the offset is in the WAL (Hot) or S3 (Cold)."
	// But we might also have sealed WAL files that are not yet in S3 or are still local.
	// Let's strictly say: WAL read = Active WAL or local sealed WALs.
	// Implementing read from sealed WALs is needed.

	// Find file covering offset.
	// scan dir?
	// cache?
	// For now, let's scan dir.

	return p.readFromSealed(offset)
}

func (p *PartitionWAL) readFromFile(f *os.File, pos int64) ([]byte, error) {
	if _, err := f.Seek(pos, 0); err != nil {
		return nil, err
	}

	header := make([]byte, 12)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(header[8:12])

	body := make([]byte, size)
	if _, err := io.ReadFull(f, body); err != nil {
		return nil, err
	}

	msgOffset := int64(binary.BigEndian.Uint64(header[0:8]))

	// Determine total count (recursive)
	totalCount := CountMessageSet(body)
	if totalCount == 0 {
		totalCount = 1
	}

	// Patch offsets
	offsetPos := 0
	currentOff := msgOffset

	// Check if we have a single compressed wrapper
	// A wrapper implies shallowCount == 1 and totalCount > 1
	// Or simply if Attributes says compressed.
	// But simply: if shallow entries loop only finds 1 entry, and totalCount > 1.

	shallowCount := 0
	checkPos := 0
	for checkPos <= len(body)-12 {
		entrySize := binary.BigEndian.Uint32(body[checkPos+8 : checkPos+12])
		checkPos += 12 + int(entrySize)
		shallowCount++
	}

	isCompressedWrapper := (shallowCount == 1 && totalCount > 1)

	for offsetPos <= len(body)-12 {
		entrySize := binary.BigEndian.Uint32(body[offsetPos+8 : offsetPos+12])
		totalLen := 12 + int(entrySize)
		if offsetPos+totalLen > len(body) {
			break
		}

		writeOff := currentOff
		if isCompressedWrapper {
			// Use Last Offset
			writeOff = msgOffset + int64(totalCount) - 1
		}

		binary.BigEndian.PutUint64(body[offsetPos:offsetPos+8], uint64(writeOff))
		offsetPos += totalLen
		currentOff++
	}

	return body, nil
}

func (p *PartitionWAL) readFromSealed(offset int64) ([]byte, error) {
	// 1. List all .log files
	files, err := os.ReadDir(p.dir)
	if err != nil {
		return nil, err
	}

	var candidates []int64
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".log") && file.Name() != "active.log" {
			name := strings.TrimSuffix(file.Name(), ".log")
			off, err := strconv.ParseInt(name, 10, 64)
			if err == nil && off <= offset {
				candidates = append(candidates, off)
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] > candidates[j] })

	if len(candidates) == 0 {
		return nil, fmt.Errorf("offset %d not found in any local segment", offset)
	}

	// Try the closest start offset <= requested offset
	targetStart := candidates[0]
	path := filepath.Join(p.dir, fmt.Sprintf("%020d.log", targetStart))

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Linear scan of file to find offset. (Inefficient, but functional for MVP)
	// We don't inherit the index for sealed segments.

	for {
		header := make([]byte, 12)
		if _, err := io.ReadFull(f, header); err != nil {
			break
		}

		recOff := int64(binary.BigEndian.Uint64(header[0:8]))
		recSize := binary.BigEndian.Uint32(header[8:12])

		if recOff == offset {
			body := make([]byte, recSize)
			if _, err := io.ReadFull(f, body); err != nil {
				return nil, err
			}
			return body, nil
		}

		f.Seek(int64(recSize), io.SeekCurrent)
	}

	return nil, fmt.Errorf("offset %d not found in segment %s", offset, path)
}

func (p *PartitionWAL) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeFile != nil {
		return p.activeFile.Close()
	}
	return nil
}

func (p *PartitionWAL) HighWaterMark() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nextOffset
}
