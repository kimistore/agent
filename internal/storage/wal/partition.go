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
	// Small for testing/demo purposes.
	MaxSegmentSize = 1 * 1024 * 1024 // 1MB
)

type PartitionWAL struct {
	dir              string
	activeFile       *os.File
	activeBaseOffset int64
	currentSize      int64

	nextOffset int64
	index      map[int64]int64 // Offset -> Position in active file (only for active segment)

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
		if offset >= endOffset {
			endOffset = offset + 1
		}
		f.Seek(int64(size), io.SeekCurrent)
	}

	return endOffset, nil
}

func (p *PartitionWAL) recoverActive() error {
	p.activeFile.Seek(0, 0)
	pos := int64(0)

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
		if offset >= p.nextOffset {
			p.nextOffset = offset + 1
		}

		if _, err := p.activeFile.Seek(int64(size), io.SeekCurrent); err != nil {
			return err
		}

		pos += 12 + int64(size)
	}

	p.activeFile.Seek(0, 2)
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

	return nil
}

func (p *PartitionWAL) Read(offset int64) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check active
	if pos, ok := p.index[offset]; ok {
		return p.readFromFile(p.activeFile, pos)
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

	// PATCH: Rewrite the offset in the MessageSet to match the WAL offset.
	// We already know the offset is `offset` (passed to Read, but not here).
	// Wait, readFromFile has `pos` but not `offset`.
	// We need to pass offset to readFromFile or rely on caller?
	// Caller `Read` knows offset. `readFromFile` does not.
	// But `readFromFile` reads `header` which contains `msgOffset`.
	msgOffset := int64(binary.BigEndian.Uint64(header[0:8]))
	if len(body) >= 8 {
		binary.BigEndian.PutUint64(body[0:8], uint64(msgOffset))
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
