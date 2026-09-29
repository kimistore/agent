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
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"kimistore/internal/metrics"
)

const (
// MaxSegmentSize is the threshold to roll a segment.
// Increased to 64MB to reduce invalid S3 PUT costs.
)

var MaxSegmentSize = int64(64 * 1024 * 1024) // 64MB

// maxEntrySize bounds a single WAL entry. A size field larger than this is
// treated as corruption rather than a legitimate (if very large) record.
const maxEntrySize = 100 * 1024 * 1024 // 100MB

type UploadTask struct {
	Topic      string
	Partition  int32
	Path       string // Local disk path
	BaseOffset int64  // Starting offset for the segment
	Source     string // fast-path or reconciliation
}

type PartitionWAL struct {
	dir              string
	topic            string
	partition        int32
	activeFile       *os.File
	activeBaseOffset int64
	currentSize      int64

	nextOffset int64
	index      map[int64]int64 // Offset -> Position
	offsets    []int64         // Sorted list of offsets in active file

	onRoll func(UploadTask)

	// mu guards all mutable partition state: the file handle pointer, sizes,
	// offsets, the index, and the sync sequence counters.
	mu sync.Mutex

	// fileMu guards replacement of activeFile (closing and reopening during a
	// roll) against an fsync that is in flight against the old handle. Without
	// it, a sync could land on a file descriptor that roll had already closed
	// and that the OS had since reassigned to something else.
	//
	// Lock ordering is always fileMu then mu. Never acquire mu first.
	fileMu sync.RWMutex

	// commitMu serializes fsync calls so that concurrent appends can share one.
	// This is group commit: a batch of writes that arrive while an fsync is in
	// flight is covered by that fsync, instead of paying one per record.
	commitMu sync.Mutex

	// writeSeq counts every completed write; syncedSeq is the highest writeSeq
	// known to be on stable storage. Both guarded by mu.
	writeSeq  uint64
	syncedSeq uint64
}

func NewPartitionWAL(dir string, topic string, partition int32, onRoll func(UploadTask)) (*PartitionWAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	pw := &PartitionWAL{
		dir:       dir,
		topic:     topic,
		partition: partition,
		index:     make(map[int64]int64),
		onRoll:    onRoll,
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

		if size > 100*1024*1024 { // 100MB safety
			break // Corrupt
		}

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

// recoverActive rebuilds the in-memory index from active.log.
//
// A crash mid-write leaves a torn trailing record: a header with a partial
// body, a partial header, or a garbage size field. That is the single most
// common crash artifact for a write-ahead log, so it must be recoverable
// rather than fatal. The damage is confined to the tail, so we truncate back
// to the last known-good record boundary and carry on serving the rest.
func (p *PartitionWAL) recoverActive() error {
	p.activeFile.Seek(0, 0)
	pos := int64(0)
	p.offsets = nil

	for {
		header := make([]byte, 12)
		if _, err := io.ReadFull(p.activeFile, header); err != nil {
			if err == io.EOF {
				// Clean EOF on a record boundary. Nothing to repair.
				break
			}
			// A short read here is a torn header. Drop it.
			if err == io.ErrUnexpectedEOF {
				p.truncateTornTail(pos, 0, "partial entry header")
				break
			}
			return err
		}

		offset := int64(binary.BigEndian.Uint64(header[0:8]))
		size := binary.BigEndian.Uint32(header[8:12])

		if size > maxEntrySize {
			// Cannot trust the size field, so we cannot find the next record
			// boundary. Everything from pos onward is unusable.
			p.truncateTornTail(pos, size, "implausible entry size")
			break
		}

		p.index[offset] = pos
		p.offsets = append(p.offsets, offset)

		// Read body to count
		body := make([]byte, size)
		if _, err := io.ReadFull(p.activeFile, body); err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				// Torn body. This record never completed, so drop it along
				// with the index entry we just added.
				delete(p.index, offset)
				p.offsets = p.offsets[:len(p.offsets)-1]
				p.truncateTornTail(pos, size, "partial entry body")
				break
			}
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

// truncateTornTail discards a damaged trailing record and rewinds the file to
// the last known-good boundary. The reason is logged so the repair is never
// silent, and p.currentSize is updated to match the bytes actually on disk.
func (p *PartitionWAL) truncateTornTail(goodPos int64, badSize uint32, reason string) {
	metrics.WALRecoveryTruncations.Inc()
	log.Printf("WAL recovery: discarding damaged trailing record in %s (%s, claimed %d bytes at offset %d); truncating to %d bytes",
		p.dir, reason, badSize, goodPos, goodPos)

	if err := p.activeFile.Truncate(goodPos); err != nil {
		log.Printf("WAL recovery: TRUNCATE FAILED for %s: %v -- the partition may not reopen cleanly", p.dir, err)
		return
	}
	if _, err := p.activeFile.Seek(goodPos, io.SeekStart); err != nil {
		log.Printf("WAL recovery: seek after truncate failed for %s: %v", p.dir, err)
	}
	p.currentSize = goodPos
}

// syncDir fsyncs a directory so a rename performed inside it is durable.
// Called after sealing a segment, so the new name survives a crash.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		log.Printf("WAL: cannot open dir %s for sync: %v", dir, err)
		return
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		// Not fatal on platforms that do not support directory fsync.
		log.Printf("WAL: dir sync failed for %s: %v", dir, err)
	}
}

// openActive reopens active.log in append mode. Used on recovery paths where
// a previous failure may have left the handle closed or nil.
func (p *PartitionWAL) openActive() error {
	path := filepath.Join(p.dir, "active.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	p.activeFile = f
	p.currentSize = info.Size()
	return nil
}

// Append writes a batch to the active segment and returns its base offset.
//
// sync controls durability: when true the data is fsynced to stable storage
// before the offset is returned, so the caller may ack it to the client. This
// mirrors Kafka's acks semantics -- acks=0 means fire-and-forget, acks>=1
// means the producer is told "durable".
func (p *PartitionWAL) Append(batch []byte, recordCount int, sync bool) (int64, error) {
	p.mu.Lock()

	// A previous roll() may have failed to reopen the file. Recover rather
	// than failing every subsequent write on a closed handle.
	if p.activeFile == nil {
		if err := p.openActive(); err != nil {
			p.mu.Unlock()
			return 0, fmt.Errorf("reopen active log for %s: %w", p.dir, err)
		}
	}
	needRoll := p.currentSize > MaxSegmentSize
	p.mu.Unlock()

	// roll() acquires fileMu then mu, so it must not be called while holding
	// mu. The size check is repeated inside roll to make a concurrent double
	// roll harmless.
	if needRoll {
		if err := p.roll(); err != nil {
			return 0, err
		}
	}

	p.mu.Lock()
	if p.activeFile == nil {
		p.mu.Unlock()
		return 0, fmt.Errorf("active log for %s is not open", p.dir)
	}

	if p.currentSize == 0 {
		p.activeBaseOffset = p.nextOffset
	}

	offset := p.nextOffset
	size := uint32(len(batch))

	buf := make([]byte, 12)
	binary.BigEndian.PutUint64(buf[0:8], uint64(offset))
	binary.BigEndian.PutUint32(buf[8:12], size)

	// The active file is opened O_APPEND, so every write lands at EOF and
	// p.currentSize is the authoritative position of the next record. Do NOT
	// use Seek(0, io.SeekCurrent) here: Read paths share this same *os.File
	// and move its cursor, which would record a bogus position in p.index and
	// silently serve the wrong record to the next reader.
	pos := p.currentSize

	n1, err := p.activeFile.Write(buf)
	if err != nil {
		p.mu.Unlock()
		return 0, err
	}
	n2, err := p.activeFile.Write(batch)
	if err != nil {
		p.mu.Unlock()
		return 0, err
	}

	p.currentSize += int64(n1 + n2)
	p.index[offset] = pos
	p.offsets = append(p.offsets, offset)

	if recordCount < 1 {
		recordCount = 1
	}
	p.nextOffset += int64(recordCount)

	p.writeSeq++
	mySeq := p.writeSeq
	p.mu.Unlock()

	// Durability barrier. Must complete before the offset is returned, so a
	// successful ack genuinely means the bytes survived a crash.
	if sync {
		if err := p.commit(mySeq); err != nil {
			return 0, err
		}
	}

	return offset, nil
}

// commit blocks until every write up to seq is on stable storage.
//
// Group commit: the first caller through performs the fsync, and any caller
// that queues behind it finds its data already covered and returns without
// paying for a second fsync. Under concurrent load this collapses thousands of
// per-record fsyncs into roughly one per batch, which is the difference between
// a usable producer and one capped at a few hundred records per second.
func (p *PartitionWAL) commit(seq uint64) error {
	p.commitMu.Lock()
	defer p.commitMu.Unlock()

	p.mu.Lock()
	if p.syncedSeq >= seq {
		p.mu.Unlock()
		return nil
	}
	// Capture the write sequence this flush will cover BEFORE issuing it.
	// Reading writeSeq after the fsync would be unsound: a writer could land
	// between the fsync and that read, get folded into the new syncedSeq, and
	// be told "durable" on the strength of a flush that predates its bytes.
	// Only a snapshot taken beforehand is guaranteed to be covered.
	target := p.writeSeq
	p.mu.Unlock()

	// Take fileMu before reading activeFile so a concurrent roll cannot close
	// the handle between the read and the fsync.
	p.fileMu.RLock()
	p.mu.Lock()
	f := p.activeFile
	p.mu.Unlock()

	var err error
	switch {
	case f == nil:
		err = fmt.Errorf("no active log open")
	default:
		err = f.Sync()
	}
	p.fileMu.RUnlock()

	if err != nil {
		return fmt.Errorf("fsync %s: %w", p.dir, err)
	}

	p.mu.Lock()
	// Everything up to the pre-flush snapshot is now on stable storage.
	if target > p.syncedSeq {
		p.syncedSeq = target
	}
	p.mu.Unlock()
	return nil
}

// roll seals the active segment and starts a new one.
//
// Acquires fileMu then mu, matching commit's ordering. It must therefore be
// called without holding mu.
func (p *PartitionWAL) roll() error {
	p.fileMu.Lock()
	defer p.fileMu.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	// Another writer may have already rolled while we waited for the locks.
	// A second roll would seal an empty segment for no benefit. Only an empty
	// active segment short-circuits: an explicit roll of a small-but-nonempty
	// segment must still happen.
	if p.currentSize == 0 {
		return nil
	}

	log.Printf("Rolling active segment for %s at offset %d", p.dir, p.nextOffset)

	if p.activeFile == nil {
		return fmt.Errorf("no active log to roll for %s", p.dir)
	}

	// Flush the tail to stable storage BEFORE sealing. Without this, a crash
	// can leave a sealed segment whose last records exist only in page cache;
	// the uploader would then publish that truncated segment to S3 and delete
	// the local copy, turning a recoverable crash into permanent data loss.
	if err := p.activeFile.Sync(); err != nil {
		return fmt.Errorf("sync before roll for %s: %w", p.dir, err)
	}

	// Close active
	if err := p.activeFile.Close(); err != nil {
		return err
	}
	p.activeFile = nil

	// Rename to sealed (using 0-padded activeBaseOffset)
	oldPath := filepath.Join(p.dir, "active.log")
	newPath := filepath.Join(p.dir, fmt.Sprintf("%020d.log", p.activeBaseOffset))

	if err := os.Rename(oldPath, newPath); err != nil {
		// The rename failed but the data is still intact under the old name.
		// Reopen so the partition stays writable, otherwise every subsequent
		// Append would fail forever on a closed handle.
		log.Printf("roll: rename %s -> %s failed: %v; reopening active log", oldPath, newPath, err)
		if rerr := p.openActive(); rerr != nil {
			return fmt.Errorf("roll rename failed (%v) and reopen failed: %w", err, rerr)
		}
		return fmt.Errorf("roll %s: %w", p.dir, err)
	}

	// Make the rename itself durable, so the sealed name survives a crash
	// even if we die before the upload completes.
	syncDir(p.dir)

	// Open new active
	if err := p.openActive(); err != nil {
		return fmt.Errorf("open new active for %s: %w", p.dir, err)
	}
	baseOffset := p.activeBaseOffset
	p.activeBaseOffset = p.nextOffset

	// Clear index for active segment (past segments are not indexed in memory for MVP)
	p.index = make(map[int64]int64)
	p.offsets = nil

	// Trigger async upload
	if p.onRoll != nil {
		task := UploadTask{
			Topic:      p.topic,
			Partition:  p.partition,
			Path:       newPath,
			BaseOffset: baseOffset,
		}
		go p.onRoll(task)
	}

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

	return patchOffsets(body, msgOffset, totalCount), nil
}

// patchOffsets rewrites the offsets inside a MessageSet so they run
// sequentially from baseOffset. For a compressed wrapper holding many
// records, every outer entry takes the batch's last offset, matching how
// librdkafka expects a compressed batch to be addressed.
func patchOffsets(body []byte, msgOffset int64, totalCount int) []byte {
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

	offsetPos := 0
	currentOff := msgOffset

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

	return body
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

	// Scan to the record whose offset range covers the target. Matching on
	// recOff == offset alone is not enough: a single entry can hold many
	// records (compressed batches, or a multi-record MessageSet), and readers
	// legitimately ask for an offset in the middle of one.
	pos := int64(0)
	for {
		header := make([]byte, 12)
		if _, err := io.ReadFull(f, header); err != nil {
			break
		}

		recOff := int64(binary.BigEndian.Uint64(header[0:8]))
		recSize := binary.BigEndian.Uint32(header[8:12])

		if recSize > maxEntrySize {
			break // Corrupt
		}
		if recOff > offset {
			// Overshot: this segment cannot contain the target.
			break
		}

		body := make([]byte, recSize)
		if _, err := io.ReadFull(f, body); err != nil {
			break
		}

		count := CountMessageSet(body)
		if count == 0 {
			count = 1
		}

		if recOff+int64(count) > offset {
			// Target falls inside [recOff, recOff+count)
			return patchOffsets(body, recOff, count), nil
		}

		pos += 12 + int64(recSize)
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
