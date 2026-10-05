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

	// seed is the log end offset supplied at construction from durable
	// metadata. loadState prefers it over anything it can infer from local
	// files, because local files understate the log once segments have been
	// offloaded.
	seed int64
}

// NewPartitionWAL opens (or creates) the WAL for a partition.
//
// seed is the log end offset recovered from durable metadata: the checkpoint,
// or the segment inventory in object storage. It is the authority for where
// the log stands, because the local directory cannot answer that question
// once sealed segments have been offloaded and deleted. Pass 0 when there is
// nothing durable to restore, in which case the local files are used.
func NewPartitionWAL(dir string, topic string, partition int32, seed int64, onRoll func(UploadTask)) (*PartitionWAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	pw := &PartitionWAL{
		dir:       dir,
		topic:     topic,
		partition: partition,
		index:     make(map[int64]int64),
		onRoll:    onRoll,
		seed:      seed,
	}

	if err := pw.loadState(); err != nil {
		return nil, err
	}

	return pw, nil
}

// SeedPartition tells an already-open partition where the log actually ends.
// It is applied on startup, after the durable offset is known, and only ever
// moves the partition forward: rewinding here would let new writes collide
// with records already in object storage.
func (p *PartitionWAL) SeedPartition(logEndOffset int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if logEndOffset > p.nextOffset {
		p.nextOffset = logEndOffset
	}
	if p.currentSize == 0 {
		p.activeBaseOffset = logEndOffset
	} else if p.activeBaseOffset < logEndOffset {
		// The active file was written before the restore point; keep it
		// addressable but do not let the base offset run ahead of the data.
		p.activeBaseOffset = logEndOffset
	}
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

	// 3. Determine activeBaseOffset.
	//
	// Durable metadata wins. Falling back to local files alone is what used
	// to make a restart rewind the log to zero: sealed segments are deleted
	// after they are uploaded, so after a restart the directory is empty and
	// the recovered nextOffset is 0, which is then handed to the next
	// producer as if it were free.
	if p.seed > p.nextOffset {
		p.nextOffset = p.seed
	}

	if p.seed > 0 {
		// The log position is known durably. Sealed segments on local disk
		// are a subset of it, so there is nothing left to infer -- and
		// inferring from an empty directory would undo the restore.
		if p.currentSize > 0 {
			if min, ok := minOffset(p.index); ok && min < p.nextOffset {
				p.activeBaseOffset = min
			} else {
				p.activeBaseOffset = p.nextOffset
			}
		} else {
			p.activeBaseOffset = p.nextOffset
		}
	} else if p.nextOffset == 0 {
		// Nothing durable and nothing in the active file: infer from sealed
		// local segments, if any survived.
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
		}
		// Otherwise the active file contributed nothing readable and its base
		// offset stays where it was: the segment continues the log rather than
		// rewinding it, which is the only safe answer.
	} else {
		// Active file is empty but it holds records' offsets, so the base
		// offset is simply where the next write will land.
		p.activeBaseOffset = p.nextOffset
	}

	return nil
}

// minOffset returns the lowest offset present in an index.
func minOffset(idx map[int64]int64) (int64, bool) {
	best := int64(-1)
	for off := range idx {
		if best == -1 || off < best {
			best = off
		}
	}
	return best, best >= 0
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
	defer func() { _ = f.Close() }()

	// Scan to end
	// Use a lighter scanner if possible, or just reuse recovering logic?
	// Let's just assume we can trust the file content.

	// Optimization: Read last 12 bytes? No, variable size records.
	// Must scan.

	endOffset := lastStart
	// ... Scanning implementation ...
	// For MVP, if we restart, scanning all might be slow but safe.

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
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
	if _, err := p.activeFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
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
	defer func() { _ = d.Close() }()
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
		_ = f.Close()
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
	if f == nil {
		err = fmt.Errorf("no active log open")
	} else {
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

	// Hand the sealed segment to the uploader. The callback only enqueues, so
	// calling it inline keeps the handoff synchronous: a caller that seals on
	// shutdown must be able to wait for the queue afterwards and be sure the
	// task is already in it.
	if p.onRoll != nil {
		p.onRoll(UploadTask{
			Topic:      p.topic,
			Partition:  p.partition,
			Path:       newPath,
			BaseOffset: baseOffset,
		})
	}

	return nil
}

// Read returns the stored record blob covering offset, re-addressed so the
// offsets it carries agree with the log's own offset space.
func (p *PartitionWAL) Read(offset int64) ([]byte, error) {
	data, _, err := p.read(offset, 0, true)
	return data, err
}

// ReadBatch returns consecutive record blobs starting at offset, stopping
// once maxBytes of payload would be exceeded. It also returns the offset the
// caller should request next.
//
// Returning a single blob per Fetch is a throughput trap: a consumer would
// need one round trip per producer batch, and against object storage that is
// one GET per batch. Filling the caller's byte budget is what makes the fetch
// loop behave like a broker's.
func (p *PartitionWAL) ReadBatch(offset int64, maxBytes int64) ([]byte, int64, error) {
	if maxBytes <= 0 {
		maxBytes = maxInt64
	}
	return p.read(offset, maxBytes, false)
}

func (p *PartitionWAL) read(offset int64, maxBytes int64, single bool) ([]byte, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	idx := sort.Search(len(p.offsets), func(i int) bool { return p.offsets[i] > offset })
	searchIdx := idx - 1

	if searchIdx >= 0 && searchIdx < len(p.offsets) {
		startOffset := p.offsets[searchIdx]
		pos, ok := p.index[startOffset]
		if ok {
			if body, count, err := p.readEntryAt(p.activeFile, pos); err == nil {
				if startOffset+int64(count) > offset {
					out, next := accumulate(body, startOffset, int64(count), maxBytes, nil)
					if single {
						return out, next, nil
					}

					// Keep consuming the rest of the active segment while the
					// budget allows. One record per fetch turns a consumer
					// into a request loop.
					for i := searchIdx + 1; i < len(p.offsets) && int64(len(out)) < maxBytes; i++ {
						nextPos, ok := p.index[p.offsets[i]]
						if !ok {
							break
						}
						more, moreCount, err := p.readEntryAt(p.activeFile, nextPos)
						if err != nil {
							break
						}
						if int64(len(out))+int64(len(more)) > maxBytes {
							break
						}
						out = append(out, more...)
						next += int64(moreCount)
					}
					return out, next, nil
				}
			}
		}
	}

	return p.readFromSealedBatch(offset, maxBytes, single)
}

const maxInt64 = int64(^uint64(0) >> 1)

// accumulate appends one stored blob to out if it fits the budget, then keeps
// consuming consecutive blobs from the active segment while they do.
func accumulate(body []byte, startOffset, count, maxBytes int64, out []byte) ([]byte, int64) {
	if out == nil {
		out = make([]byte, 0, len(body))
	}
	if int64(len(out))+int64(len(body)) > maxBytes && len(out) > 0 {
		return out, startOffset
	}
	out = append(out, body...)
	next := startOffset + count
	return out, next
}

// readEntryAt reads a single stored entry (header + body) from f at pos and
// returns the raw body alongside the record count it holds.
func (p *PartitionWAL) readEntryAt(f *os.File, pos int64) ([]byte, int, error) {
	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return nil, 0, err
	}
	header := make([]byte, msgSetEntryHeaderLen)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, 0, err
	}
	size := int32(binary.BigEndian.Uint32(header[8:12]))
	if size < 0 || size > maxEntrySize {
		return nil, 0, fmt.Errorf("implausible entry size %d at %d", size, pos)
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(f, body); err != nil {
		return nil, 0, err
	}
	msgOffset := int64(binary.BigEndian.Uint64(header[0:8]))
	count := CountMessageSet(body)
	if count == 0 {
		count = 1
	}
	return patchOffsets(body, msgOffset, count), count, nil
}

// patchOffsets re-addresses a stored record blob into the log's offset space.
//
// A blob is either a bare RecordBatch or a message set whose entries wrap one.
// The two are handled separately because they carry the offset in different
// places: a bare batch's first 8 bytes *are* its base offset, whereas in a
// message set the offset lives in the outer entry header and a wrapped
// RecordBatch additionally has its own base offset inside. Rewriting only the
// outer one leaves the inner base offset stale, which every modern client
// reads in preference to it.
// PatchStoredBlob re-addresses a stored record blob into a log's offset space
// and returns the blob, so the cold and hot read paths agree on what the
// offsets mean.
func PatchStoredBlob(body []byte, msgOffset int64, totalCount int) []byte {
	return patchOffsets(body, msgOffset, totalCount)
}

// patchOffsets re-addresses a stored record blob so the offsets it carries
// agree with the log's own offset space.
//
// A blob is either a bare RecordBatch or a message set. The two are handled
// separately because the offset lives in a different place in each: a bare
// batch's first 8 bytes *are* its base offset, while a message set carries the
// offset in the entry header and a wrapped RecordBatch additionally has its own
// base offset inside. Rewriting only the outer one leaves the inner base offset
// stale, and every modern client reads the inner one in preference.
func patchOffsets(body []byte, msgOffset int64, totalCount int) []byte {
	if isRecordBatch(body) {
		if err := setRecordBatchBaseOffset(body, msgOffset); err == nil {
			return body
		}
		// Not a batch after all; fall through to message set handling.
	}
	assignMessageSetOffsets(body, msgOffset)
	return body
}

// assignMessageSetOffsets rewrites the offset of every message set entry,
// starting at base, and returns the offset the next entry will take.
//
// The per-entry advance is the part that has to be right. A legacy message set
// is a flat list of single messages, so each entry takes exactly one offset. A
// compressed wrapper is a single entry standing in for many, and carries the
// *last* offset of the batch: readers recover the first by subtracting, which
// is why setting it to the base instead would shift the whole batch.
func assignMessageSetOffsets(body []byte, base int64) int64 {
	off := base
	pos := 0
	for pos+msgSetEntryHeaderLen <= len(body) {
		size := int32(binary.BigEndian.Uint32(body[pos+8 : pos+12]))
		if size < 0 {
			break
		}
		total := msgSetEntryHeaderLen + int(size)
		if pos+total > len(body) {
			break
		}
		entry := body[pos+msgSetEntryHeaderLen : pos+total]

		if isRecordBatch(entry) {
			// Both offsets have to move: the entry header carries the
			// batch's base offset, and the batch carries its own copy of the
			// same thing, which is what clients actually read.
			binary.BigEndian.PutUint64(body[pos:pos+8], uint64(off))
			_ = setRecordBatchBaseOffset(entry, off)
			if info, err := parseRecordBatch(entry); err == nil {
				off += int64(info.RecordsCount)
			} else {
				off++
			}
		} else if count, wrapped := compressedEntryCount(entry); wrapped {
			binary.BigEndian.PutUint64(body[pos:pos+8], uint64(off+count-1))
			off += count
		} else {
			binary.BigEndian.PutUint64(body[pos:pos+8], uint64(off))
			off++
		}
		pos += total
	}
	return off
}

// compressedEntryCount reports how many records a legacy compressed wrapper
// stands for, and whether the entry is one at all.
func compressedEntryCount(entry []byte) (int64, bool) {
	l, ok := parseLegacyMessage(entry)
	if !ok || !l.compressed || l.valueLen <= 0 {
		return 0, false
	}
	raw, err := decompress(l.codec, entry[l.valueOff:l.valueOff+l.valueLen])
	if err != nil {
		return 0, false
	}
	return int64(CountMessageSet(raw)), true
}

// readFromSealed is the single-blob form of readFromSealedBatch.
func (p *PartitionWAL) readFromSealed(offset int64) ([]byte, error) {
	data, _, err := p.readFromSealedBatch(offset, 0, true)
	return data, err
}

func (p *PartitionWAL) readFromSealedBatch(offset int64, maxBytes int64, single bool) ([]byte, int64, error) {
	// 1. List all .log files
	files, err := os.ReadDir(p.dir)
	if err != nil {
		return nil, 0, err
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
		return nil, 0, fmt.Errorf("offset %d not found in any local segment", offset)
	}

	targetStart := candidates[0]
	path := filepath.Join(p.dir, fmt.Sprintf("%020d.log", targetStart))

	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()

	pos := int64(0)
	for {
		header := make([]byte, msgSetEntryHeaderLen)
		if _, err := io.ReadFull(f, header); err != nil {
			break
		}

		recOff := int64(binary.BigEndian.Uint64(header[0:8]))
		recSize := int32(binary.BigEndian.Uint32(header[8:12]))
		if recSize < 0 || recSize > maxEntrySize {
			break
		}
		if recOff > offset {
			break // this segment cannot contain the target
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
			patched := patchOffsets(body, recOff, count)
			// Fill the remaining budget from the rest of this segment so one
			// read can serve several batches.
			out, next := accumulate(patched, recOff, int64(count), maxBytes, nil)
			if !single && maxBytes > int64(len(patched)) {
				more, moreNext, err := p.drainSealed(f, out, next, maxBytes)
				if err == nil && len(more) > len(out) {
					return more, moreNext, nil
				}
			}
			return out, next, nil
		}
		pos += msgSetEntryHeaderLen + int64(recSize)
	}

	return nil, 0, fmt.Errorf("offset %d not found in segment %s", offset, path)
}

// drainSealed keeps reading entries from an open sealed segment until the
// budget is spent. onErr is deliberately non-fatal: a partial answer that
// already covers the requested offset beats failing the whole fetch.
func (p *PartitionWAL) drainSealed(f *os.File, out []byte, offset, maxBytes int64) ([]byte, int64, error) {
	next := offset
	for int64(len(out)) < maxBytes {
		header := make([]byte, msgSetEntryHeaderLen)
		if _, err := io.ReadFull(f, header); err != nil {
			return out, next, nil
		}
		recOff := int64(binary.BigEndian.Uint64(header[0:8]))
		recSize := int32(binary.BigEndian.Uint32(header[8:12]))
		if recSize < 0 || recSize > maxEntrySize {
			return out, next, nil
		}
		body := make([]byte, recSize)
		if _, err := io.ReadFull(f, body); err != nil {
			return out, next, nil
		}
		count := CountMessageSet(body)
		if count == 0 {
			count = 1
		}
		if int64(len(out))+int64(len(body)) > maxBytes {
			break
		}
		out = append(out, patchOffsets(body, recOff, count)...)
		next = recOff + int64(count)
	}
	return out, next, nil
}

// Discard closes the active file without sealing or uploading it.
//
// Used when a topic is being deleted: sealing there would publish the
// segment to object storage, and the upload could land after the delete and
// put the objects back.
func (p *PartitionWAL) Discard() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeFile == nil {
		return nil
	}
	err := p.activeFile.Close()
	p.activeFile = nil
	return err
}

// Close seals the active segment before closing the file.
//
// Sealing matters because this agent is expected to run with an ephemeral
// local disk: durable data lives in object storage, and the active segment is
// the only copy of whatever has not rolled yet. Closing without sealing
// discards it on a graceful restart, which is the exact failure the
// object-store-backed design exists to avoid.
//
// A segment with no records is left alone; an empty segment would only cost
// an object.
func (p *PartitionWAL) Close() error {
	p.mu.Lock()
	size := p.currentSize
	p.mu.Unlock()

	if size > 0 {
		if err := p.roll(); err != nil {
			// Report the sealing failure but still close the file, so the
			// caller is not left with a leaked descriptor.
			p.mu.Lock()
			if p.activeFile != nil {
				_ = p.activeFile.Close()
			}
			p.mu.Unlock()
			return err
		}
	}

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
