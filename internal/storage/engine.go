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
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"kimistore/internal/coordinator"
	"kimistore/internal/metrics"
	"kimistore/internal/storage/index"
	"kimistore/internal/storage/wal"
)

type StorageEngine struct {
	walMgr   *wal.Manager
	objStore ObjectStore
	bucket   string
	walDir   string

	quit chan struct{}
	wg   sync.WaitGroup

	// Parallel Uploader
	uploadChan      chan wal.UploadTask
	inFlightUploads sync.Map // path -> struct{}

	// Cache for S3 List results (topic/partition -> []keys)
	segmentCache map[string][]string
	cacheMu      sync.RWMutex

	// Buffer for offsets (groupID/topic/partition -> offset)
	offsetBuf   map[string]int64
	offsetBufMu sync.Mutex

	// committedByGroup tracks each group's committed offset per topic/partition.
	// The retention log-start is the minimum across groups, computed on demand.
	//
	// This must be per-group rather than a single running minimum: a single
	// min map can never increase, so a group that advances its commit would
	// leave retention permanently over-protecting segments it no longer needs.
	committedMu      sync.Mutex
	committedByGroup map[string]map[string]int64
	// committedAuthoritative is set once consumer offsets have been loaded
	// from object storage. Until then retention must not run, because an
	// empty map would look exactly like "no consumers" and invite deletion of
	// data a consumer still needs.
	committedAuthoritative bool

	retentionCfg RetentionConfig

	metadataCache *MetadataCache
	coordinator   *coordinator.Coordinator

	// dataCh is a broadcast latch signalled on every successful append, so a
	// long-polling Fetch can park until there is something to read instead of
	// returning empty immediately and being re-polled at full speed. Guarded
	// by dataMu.
	dataMu sync.Mutex
	dataCh chan struct{}
}

func NewStorageEngine(walDir string, objStore ObjectStore, bucket string, retentionCfg RetentionConfig) (*StorageEngine, error) {
	se := &StorageEngine{
		objStore:         objStore,
		bucket:           bucket,
		walDir:           walDir,
		quit:             make(chan struct{}),
		uploadChan:       make(chan wal.UploadTask, 1024),
		segmentCache:     make(map[string][]string),
		offsetBuf:        make(map[string]int64),
		committedByGroup: make(map[string]map[string]int64),
		retentionCfg:     retentionCfg,
		metadataCache:    NewMetadataCache(),
		dataCh:           make(chan struct{}),
	}

	onRoll := func(task wal.UploadTask) {
		task.Source = "fast-path"
		select {
		case se.uploadChan <- task:
		default:
			metrics.UploaderMissedEvents.Inc()
			log.Printf("Warning: Upload channel full, skipping fast-path for %s", task.Path)
		}
	}

	mgr, err := wal.NewManager(walDir, onRoll)
	if err != nil {
		return nil, err
	}
	se.walMgr = mgr

	// Load/Merge Cache from WAL
	if err := se.metadataCache.Load(walDir); err != nil {
		log.Printf("Warning: Failed to load metadata cache: %v", err)
	}

	// Start worker pool (8 workers)
	for i := 0; i < 8; i++ {
		se.wg.Add(1)
		go se.uploaderWorker()
	}

	// Start background uploader (reconciliation), offset flusher, retention, and checkpoint loop
	se.wg.Add(4)
	go se.uploaderLoop()
	go se.offsetFlusherLoop()
	go se.retentionLoop()
	go se.checkpointLoop()

	// Load consumer offsets from object storage so retention knows the log
	// start. Registered on the wait group so Close cannot return while it is
	// still using the object store.
	se.wg.Add(1)
	go func() {
		defer se.wg.Done()
		se.rehydrateCommittedOffsets(context.Background())
	}()

	return se, nil
}

// Append writes a batch and returns its base offset. When sync is true the
// data is fsynced before returning, so the offset may be acknowledged to the
// producer as durable. Callers should set sync from the request's acks value
// (acks=0 is fire-and-forget; acks>=1 promises durability).
func (s *StorageEngine) Append(topic string, partition int32, batch []byte, recordCount int, sync bool) (int64, error) {
	// Update Cache (Idempotent)
	s.metadataCache.AddPartition(topic, partition)
	offset, err := s.walMgr.Append(topic, partition, batch, recordCount, sync)
	if err == nil {
		// Wake any long-polling Fetch requests. Signalled after the write so
		// a woken reader is guaranteed to observe the new high watermark.
		s.signalData()
	}
	return offset, err
}

// signalData broadcasts that new data is available.
func (s *StorageEngine) signalData() {
	s.dataMu.Lock()
	close(s.dataCh)
	s.dataCh = make(chan struct{})
	s.dataMu.Unlock()
}

// DataSignal returns a channel closed on the next successful append. Callers
// must re-check their condition after it fires, since it is a broadcast and
// may be triggered by an append to a different partition.
func (s *StorageEngine) DataSignal() <-chan struct{} {
	s.dataMu.Lock()
	defer s.dataMu.Unlock()
	return s.dataCh
}

func (s *StorageEngine) Read(topic string, partition int32, offset int64) ([]byte, error) {
	// 1. Try WAL (Hot)
	data, err := s.walMgr.Read(topic, partition, offset)
	if err == nil {
		return data, nil
	}

	// 2. Try Object Store (Cold)
	prefix := fmt.Sprintf("%s/%d/", topic, partition)
	cacheKey := fmt.Sprintf("%s/%d", topic, partition)

	// Helper to find best key from a list of keys
	findBestKey := func(keys []string) string {
		var bKey string
		var bStartOffset int64 = -1
		for _, k := range keys {
			if !strings.HasSuffix(k, ".log") {
				continue
			}
			parts := strings.Split(k, "/")
			filename := parts[len(parts)-1]
			baseName := strings.TrimSuffix(filename, ".log")
			startOffset, err := strconv.ParseInt(baseName, 10, 64)
			if err != nil {
				continue
			}
			if startOffset <= offset {
				if startOffset > bStartOffset {
					bStartOffset = startOffset
					bKey = k
				}
			}
		}
		return bKey
	}

	// Try Cache First
	s.cacheMu.RLock()
	cachedKeys, hit := s.segmentCache[cacheKey]
	s.cacheMu.RUnlock()

	var bestKey string
	if hit {
		bestKey = findBestKey(cachedKeys)
	}

	// If miss or not found in cache, Refresh Cache
	if bestKey == "" {
		s.cacheMu.Lock()
		// Double check
		cachedKeys, hit = s.segmentCache[cacheKey]
		if hit {
			bestKey = findBestKey(cachedKeys)
		}

		if bestKey == "" {
			// Actually list S3
			ctx := context.Background()
			objects, err := s.objStore.List(ctx, prefix)
			if err != nil {
				s.cacheMu.Unlock()
				return nil, fmt.Errorf("failed to list s3: %v", err)
			}

			var keys []string
			for _, o := range objects {
				keys = append(keys, o.Key)
			}

			s.segmentCache[cacheKey] = keys
			bestKey = findBestKey(keys)
		}
		s.cacheMu.Unlock()
	}

	if bestKey == "" {
		return nil, fmt.Errorf("offset %d not found in verified segments", offset)
	}

	// Indexed Read Logic
	ctx := context.Background()
	indexKey := strings.TrimSuffix(bestKey, ".log") + ".index"

	// Try to get index
	// Optimization: Cache index? For MVP, just fetch it. It's small.
	indexReader, err := s.objStore.Get(ctx, indexKey)
	var shouldUseIndex = false
	var indexBytes []byte

	if err == nil {
		indexBytes, err = io.ReadAll(indexReader)
		indexReader.Close()
		if err == nil && len(indexBytes) > 0 {
			shouldUseIndex = true
		}
	}

	if shouldUseIndex {
		// Lookup Position
		// Need baseOffset from filename
		parts := strings.Split(bestKey, "/")
		filename := parts[len(parts)-1]
		baseName := strings.TrimSuffix(filename, ".log")
		baseOffset, _ := strconv.ParseInt(baseName, 10, 64)

		pos, err := index.Lookup(indexBytes, offset, baseOffset)
		if err == nil {
			// Range Read!
			// We read from pos to end (or a chunk).
			// If we don't know end, we can read to end of object.
			// But ObjectStore.GetRange needs length.
			// Ideally we know size. `Get` might return size or we Listed it.
			// s.segmentCache could store size? For now, we don't have size easily.
			// Let's assume we read 1MB or similar, or just use GetRange with large length if supported?
			// Standard S3 Range: bytes=X- (to end).
			// Our interface GetRange takes length.
			// If we pass -1 as length? Or very large?
			// Let's use GetRange with a reasonable chunk (e.g. 10MB) or just standard Get if interface limits.
			// Wait, I designed GetRange(start, length).
			// If I don't know size, I can't effectively fetch "rest of file".
			// But wait, `List` returned metadata including Size!
			// `segmentCache` stores KEYS.
			// We could enhance segmentCache to store Metadata.
			// OR for MVP: Just fallback to full read if we don't know size, OR just guess large.

			// Actually, let's use a large number. S3 ignores out of range.
			// "bytes=X-Y". If Y > size, S3 returns up to size.
			const FetchSize = 10 * 1024 * 1024 // 10MB

			rc, err := s.objStore.GetRange(ctx, bestKey, pos, FetchSize)
			if err == nil {
				defer rc.Close()
				// We need to scan from the start of this range (which corresponds to `pos` in file)
				// `scanStreamForOffset` assumes it's reading a stream of messages.
				// Since `pos` points to start of a message (guaranteed by index), it should work perfectly.
				return scanStreamForOffset(rc, offset)
			}
		}
	}

	// Fallback to Full Download
	rc, err := s.objStore.Get(ctx, bestKey)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return scanStreamForOffset(rc, offset)
}

func (s *StorageEngine) GetTopics() ([]string, error) {
	return s.metadataCache.GetTopics(), nil
}

func (s *StorageEngine) GetPartitions(topic string) ([]int32, error) {
	return s.walMgr.ListPartitions(topic)
}

func (s *StorageEngine) HighWaterMark(topic string, partition int32) int64 {
	return s.walMgr.HighWaterMark(topic, partition)
}

func (s *StorageEngine) CreateTopic(topic string, partitions int32) error {
	err := s.walMgr.CreateTopic(topic, partitions)
	if err == nil {
		s.metadataCache.AddTopic(topic)
		for i := int32(0); i < partitions; i++ {
			s.metadataCache.AddPartition(topic, i)
		}
	}
	return err
}

func (s *StorageEngine) DeleteTopic(topic string) error {
	err := s.walMgr.DeleteTopic(topic)
	if err == nil {
		s.metadataCache.RemoveTopic(topic)
		// Drop retention bookkeeping for the topic across every group.
		s.committedMu.Lock()
		for _, byGroup := range s.committedByGroup {
			for k := range byGroup {
				if strings.HasPrefix(k, topic+"/") {
					delete(byGroup, k)
				}
			}
		}
		s.committedMu.Unlock()
		// Delete cold segments from Object Storage asynchronously in the background
		go s.asyncDeleteTopicFromS3(topic)
	}
	return err
}

func (s *StorageEngine) asyncDeleteTopicFromS3(topic string) {
	ctx := context.Background()
	prefix := topic + "/"
	objects, err := s.objStore.List(ctx, prefix)
	if err != nil {
		log.Printf("Async S3 cleanup: Failed to list S3 objects for topic %s: %v", topic, err)
		return
	}

	for _, obj := range objects {
		if strings.HasPrefix(obj.Key, prefix) {
			if err := s.objStore.Delete(ctx, obj.Key); err != nil {
				log.Printf("Async S3 cleanup: Failed to delete cold object %s: %v", obj.Key, err)
			} else {
				log.Printf("Async S3 cleanup: Deleted cold object %s", obj.Key)
			}
		}
	}
}

func scanStreamForOffset(r io.Reader, targetOffset int64) ([]byte, error) {
	// Format: [Offset (8)][Size (4)][Data...]
	// We scan until we find targetOffset.

	bufHeader := make([]byte, 12)

	for {
		_, err := io.ReadFull(r, bufHeader)
		if err == io.EOF {
			return nil, fmt.Errorf("offset %d not found in segment (EOF)", targetOffset)
		}
		if err != nil {
			return nil, err
		}

		msgOffset := int64(binary.BigEndian.Uint64(bufHeader[0:8]))
		msgSize := binary.BigEndian.Uint32(bufHeader[8:12])

		// Optimization: if msgOffset > targetOffset, we overshot.
		// (Assuming sorted)
		if msgOffset > targetOffset {
			return nil, fmt.Errorf("offset %d passed (found %d)", targetOffset, msgOffset)
		}

		// Read data to check if it covers the range (handling compressed batches)
		// We can't skip simply because msgOffset < targetOffset, because the batch might be large.
		data := make([]byte, msgSize)
		_, err = io.ReadFull(r, data)
		if err != nil {
			return nil, err
		}

		// Use CountMessageSet to get total record count in this entry (v0/v1/v2/compressed)
		count := wal.CountMessageSet(data)
		if count == 0 {
			count = 1
		}
		endOffset := msgOffset + int64(count)

		if targetOffset < endOffset {
			// Found it (target is within [msgOffset, endOffset))

			// PATCH: Rewrite the offsets in the MessageSet to match the WAL sequence.
			// We rewrite starting from the WAL's stored msgOffset.
			pos := 0
			currentOff := msgOffset
			for pos <= len(data)-12 {
				// data[pos : pos+8] is offset
				size := binary.BigEndian.Uint32(data[pos+8 : pos+12])
				totalLen := 12 + int(size)

				if pos+totalLen > len(data) {
					break
				}

				// Rewrite offset
				binary.BigEndian.PutUint64(data[pos:pos+8], uint64(currentOff))

				// Handle V2 RecordBatch specifically: baseOffset is at msgStart (pos+12)
				// Wait, the 12-byte header is ALREADY the baseOffset.
				// But V2 RecordBatch also has a duplicate baseOffset at pos+12?
				// Actually, V2 layout: Offset(8), Length(4), PartitionLeaderEpoch(4), Magic(1)...
				// The Offset(8) is the BaseOffset.

				// However, if we are rewriting, we must also increment currentOff
				// by the record count of THIS entry.
				entryData := data[pos : pos+totalLen]
				// We need to know the count of just THIS entry.
				// wal.CountMessageSet on entryData should work (since it includes the 12-byte header).
				entryCount := wal.CountMessageSet(entryData)
				if entryCount == 0 {
					entryCount = 1
				}

				pos += totalLen
				currentOff += int64(entryCount)
			}

			return data, err
		}
	}
}

func (s *StorageEngine) Close() error {
	close(s.quit)
	s.wg.Wait()
	return s.walMgr.Close()
}

func (s *StorageEngine) uploaderWorker() {
	defer s.wg.Done()

	for {
		select {
		case <-s.quit:
			return
		case task := <-s.uploadChan:
			s.handleUpload(task)
		}
	}
}

func (s *StorageEngine) handleUpload(task wal.UploadTask) {
	// 1. Check/Set In-Flight
	if _, loaded := s.inFlightUploads.LoadOrStore(task.Path, struct{}{}); loaded {
		return // Already being handled
	}
	defer s.inFlightUploads.Delete(task.Path)

	metrics.UploaderInFlight.Inc()
	defer metrics.UploaderInFlight.Dec()

	// Verify file still exists (might have been uploaded by someone else just now)
	info, err := os.Stat(task.Path)
	if err != nil {
		return
	}

	key := fmt.Sprintf("%s/%d/%s", task.Topic, task.Partition, filepath.Base(task.Path))
	indexKey := strings.TrimSuffix(key, ".log") + ".index"

	log.Printf("Uploader: Processing %s (size: %d, source: %s)", key, info.Size(), task.Source)

	// 2. Generate Index
	indexData, err := index.GenerateIndex(task.Path, task.BaseOffset)
	if err != nil {
		log.Printf("Error generating index for %s: %v", task.Path, err)
	} else if len(indexData) > 0 {
		if err := s.objStore.Put(context.Background(), indexKey, bytes.NewReader(indexData)); err != nil {
			log.Printf("Failed to upload index %s: %v", indexKey, err)
		}
	}

	// 3. Upload Log
	f, err := os.Open(task.Path)
	if err != nil {
		return
	}
	defer f.Close()

	if err := s.objStore.Put(context.Background(), key, f); err != nil {
		metrics.UploaderTaskCount.WithLabelValues("error", task.Source).Inc()
		log.Printf("Failed to upload %s: %v", key, err)
		return
	}

	// 4. Remove Local
	if err := os.Remove(task.Path); err != nil {
		log.Printf("Failed to remove %s: %v", task.Path, err)
	} else {
		log.Printf("Uploaded and trimmed %s", key)
	}

	// 5. Update Metrics & Cache
	metrics.UploaderTaskCount.WithLabelValues("success", task.Source).Inc()
	metrics.UploaderBytesUploaded.Add(float64(info.Size()))

	cacheKey := fmt.Sprintf("%s/%d", task.Topic, task.Partition)
	s.cacheMu.Lock()
	if list, ok := s.segmentCache[cacheKey]; ok {
		s.segmentCache[cacheKey] = append(list, key)
	}
	s.cacheMu.Unlock()
}

func (s *StorageEngine) uploaderLoop() {
	defer s.wg.Done()

	// Initial scan
	s.uploadSegments()

	ticker := time.NewTicker(1 * time.Minute) // Reconciliation every minute
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			s.uploadSegments()
		}
	}
}

func (s *StorageEngine) uploadSegments() {
	err := filepath.Walk(s.walDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if info.Name() == "active.log" {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".log") {
			return nil
		}

		// path is .../wal/<topic>/<partition>/<offset>.log
		// Extract topic and partition
		rel, _ := filepath.Rel(s.walDir, path)
		parts := strings.Split(rel, string(os.PathSeparator))
		if len(parts) != 3 {
			return nil
		}

		topic := parts[0]
		pID, _ := strconv.ParseInt(parts[1], 10, 32)
		filename := parts[2]
		baseOffset, _ := strconv.ParseInt(strings.TrimSuffix(filename, ".log"), 10, 64)

		task := wal.UploadTask{
			Topic:      topic,
			Partition:  int32(pID),
			Path:       path,
			BaseOffset: baseOffset,
			Source:     "reconciliation",
		}

		select {
		case s.uploadChan <- task:
		default:
			// Queue full, will be picked up in next reconciliation
		}

		return nil
	})

	if err != nil {
		log.Printf("Error walking WAL dir: %v", err)
	}
}
func (s *StorageEngine) SaveOffset(groupID, topic string, partition int32, offset int64) error {
	// Buffer the offset save
	key := fmt.Sprintf("%s/%s/%d", groupID, topic, partition)
	s.offsetBufMu.Lock()
	s.offsetBuf[key] = offset
	s.offsetBufMu.Unlock()

	s.recordCommitted(groupID, topic, partition, offset)
	return nil
}

// recordCommitted notes a group's latest committed offset. Retention derives
// its log start from the minimum across groups, so this must track each group
// independently.
func (s *StorageEngine) recordCommitted(groupID, topic string, partition int32, offset int64) {
	tp := topic + "/" + strconv.Itoa(int(partition))

	s.committedMu.Lock()
	defer s.committedMu.Unlock()
	if s.committedByGroup[groupID] == nil {
		s.committedByGroup[groupID] = make(map[string]int64)
	}
	s.committedByGroup[groupID][tp] = offset
}

func (s *StorageEngine) offsetFlusherLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			// Shutting down: the client was told these commits succeeded, so
			// give the object store a few chances before we exit rather than
			// dropping them on a transient error.
			s.flushOffsetsOnShutdown()
			return
		case <-ticker.C:
			s.flushOffsets()
		}
	}
}

// flushOffsetsOnShutdown retries the offset flush a bounded number of times so
// a brief object-store hiccup during termination does not lose acked commits.
func (s *StorageEngine) flushOffsetsOnShutdown() {
	const attempts = 3
	for i := 1; i <= attempts; i++ {
		s.flushOffsets()

		s.offsetBufMu.Lock()
		remaining := len(s.offsetBuf)
		s.offsetBufMu.Unlock()
		if remaining == 0 {
			return
		}
		if i < attempts {
			log.Printf("Shutdown: %d offset(s) still unflushed, retrying (%d/%d)", remaining, i+1, attempts)
			time.Sleep(time.Duration(i) * 250 * time.Millisecond)
		}
	}

	s.offsetBufMu.Lock()
	remaining := len(s.offsetBuf)
	s.offsetBufMu.Unlock()
	if remaining > 0 {
		log.Printf("Shutdown: giving up on %d offset(s) that could not be written to object storage; "+
			"consumers will resume from the last persisted position", remaining)
	}
}

func (s *StorageEngine) flushOffsets() {
	s.offsetBufMu.Lock()
	if len(s.offsetBuf) == 0 {
		s.offsetBufMu.Unlock()
		return
	}
	// Snapshot under the lock, then release it before doing network I/O.
	// Entries are NOT removed here: a commit was already acknowledged to the
	// client, so it stays in the buffer until we know it is safely in S3.
	todo := make(map[string]int64, len(s.offsetBuf))
	for k, v := range s.offsetBuf {
		todo[k] = v
	}
	s.offsetBufMu.Unlock()

	ctx := context.Background()
	for k, offset := range todo {
		// k is "groupID/topic/partition"
		s3Key := fmt.Sprintf("_offsets/%s", k)
		data := []byte(strconv.FormatInt(offset, 10))
		if err := s.objStore.Put(ctx, s3Key, bytes.NewReader(data)); err != nil {
			// Leave the entry in the buffer so the next tick retries it.
			// Dropping it here would silently discard a commit the client
			// was already told had succeeded.
			metrics.OffsetFlushFailures.Inc()
			log.Printf("Failed to flush offset %s (%d): %v -- will retry", s3Key, offset, err)
			continue
		}
		metrics.OffsetsFlushed.Inc()
		// Only clear the entry if it has not been superseded by a newer
		// commit while this write was in flight.
		s.offsetBufMu.Lock()
		if cur, ok := s.offsetBuf[k]; ok && cur == offset {
			delete(s.offsetBuf, k)
		}
		s.offsetBufMu.Unlock()
	}
}

func (s *StorageEngine) checkpointLoop() {
	defer s.wg.Done()
	// Checkpoint every 30 seconds
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {

		select {
		case <-s.quit:
			s.SaveCheckpoint()
			return
		case <-ticker.C:
			s.SaveCheckpoint()
		}
	}
}

func (s *StorageEngine) SetCoordinator(c *coordinator.Coordinator) {
	s.coordinator = c
	// Load Cache from Checkpoint now that coordinator is linked
	if err := s.LoadCheckpoint(); err != nil {
		log.Printf("Info: No checkpoint found or failed to load: %v (will rely on WAL)", err)
	}
}

func (s *StorageEngine) SaveCheckpoint() error {
	if s.coordinator != nil {
		state := s.coordinator.ToState()
		s.metadataCache.Coordinator = &state
	}

	// Carry consumer offsets into the checkpoint so a graceful restart does
	// not have to re-read every offset object before retention may run.
	s.committedMu.Lock()
	s.metadataCache.Committed = cloneCommitted(s.committedByGroup)
	s.committedMu.Unlock()

	data, err := s.metadataCache.ToJSON()
	if err != nil {
		return err
	}
	key := "_meta/checkpoint.json"

	ctx := context.Background()
	// Use bytes reader
	r := strings.NewReader(string(data))
	if err := s.objStore.Put(ctx, key, r); err != nil {
		log.Printf("Failed to save checkpoint: %v", err)
		return err
	}
	log.Printf("Saved metadata checkpoint to %s (%d bytes)", key, len(data))
	return nil
}

func (s *StorageEngine) LoadCheckpoint() error {
	key := "_meta/checkpoint.json"
	ctx := context.Background()

	r, err := s.objStore.Get(ctx, key)
	if err != nil {
		return err
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if err := s.metadataCache.FromJSON(data); err != nil {
		return err
	}

	if s.coordinator != nil && s.metadataCache.Coordinator != nil {
		s.coordinator.FromState(*s.metadataCache.Coordinator)
		log.Printf("Restored coordinator state from checkpoint")
	}

	// Merge any offsets carried in the checkpoint. Rehydration from object
	// storage is authoritative and runs separately, so a checkpoint that is
	// missing or stale cannot understate what consumers need.
	if len(s.metadataCache.Committed) > 0 {
		s.committedMu.Lock()
		groups := 0
		for group, byPartition := range s.metadataCache.Committed {
			for tp, off := range byPartition {
				s.recordLocked(group, tp, off)
			}
			groups++
		}
		s.committedMu.Unlock()
		log.Printf("Restored committed offsets for %d group(s) from checkpoint", groups)
	}

	log.Printf("Loaded metadata cache from checkpoint %s (%d bytes)", key, len(data))
	return nil
}

func cloneCommitted(in map[string]map[string]int64) map[string]map[string]int64 {
	if in == nil {
		return nil
	}
	out := make(map[string]map[string]int64, len(in))
	for group, byPartition := range in {
		cp := make(map[string]int64, len(byPartition))
		for k, v := range byPartition {
			cp[k] = v
		}
		out[group] = cp
	}
	return out
}

// recordLocked stores a commit. Callers must hold committedMu.
func (s *StorageEngine) recordLocked(group, topicPartition string, offset int64) {
	if s.committedByGroup[group] == nil {
		s.committedByGroup[group] = make(map[string]int64)
	}
	s.committedByGroup[group][topicPartition] = offset
}

// offsetsPrefix is the object-store location of consumer group offsets.
const offsetsPrefix = "_offsets/"

// rehydrateCommittedOffsets rebuilds per-group committed offsets from object
// storage, which is the authoritative record of what consumers have claimed.
//
// The checkpoint only covers up to 30s of commits, and may be missing or
// stale entirely. Retention must not treat "no offsets known" as "no
// consumers", so it stays disabled until this has run. The engine fails
// closed: if the offsets cannot be read, retention does not run at all,
// because it cannot prove a segment is safe to delete.
func (s *StorageEngine) rehydrateCommittedOffsets(ctx context.Context) {
	objects, err := s.objStore.List(ctx, offsetsPrefix)
	if err != nil {
		log.Printf("Retention: cannot read consumer offsets from %s: %v -- "+
			"retention is disabled until offsets can be read", offsetsPrefix, err)
		return
	}

	restored, skipped := 0, 0
	for _, obj := range objects {
		group, topicPartition, ok := parseOffsetKey(obj.Key)
		if !ok {
			skipped++
			continue
		}

		rc, err := s.objStore.Get(ctx, obj.Key)
		if err != nil {
			skipped++
			continue
		}
		raw, readErr := io.ReadAll(rc)
		rc.Close()
		if readErr != nil {
			skipped++
			continue
		}
		off, convErr := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if convErr != nil {
			skipped++
			continue
		}

		s.committedMu.Lock()
		s.recordLocked(group, topicPartition, off)
		s.committedMu.Unlock()
		restored++
	}

	s.committedMu.Lock()
	s.committedAuthoritative = true
	groups := len(s.committedByGroup)
	s.committedMu.Unlock()

	if skipped > 0 {
		log.Printf("Retention: %d committed offset object(s) could not be read and were skipped; "+
			"retention will over-protect rather than risk deleting live data", skipped)
	}
	log.Printf("Retention: rehydrated %d committed offset(s) across %d group(s) from %s", restored, groups, offsetsPrefix)
}

// parseOffsetKey splits "_offsets/<group>/<topic>/<partition>" into its parts.
// The partition is the final segment and the topic the one before it, so a
// group name containing slashes still parses correctly.
func parseOffsetKey(key string) (group, topicPartition string, ok bool) {
	rest := strings.TrimPrefix(key, offsetsPrefix)
	if rest == key {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) < 3 {
		return "", "", false
	}
	partition := parts[len(parts)-1]
	topic := parts[len(parts)-2]
	group = strings.Join(parts[:len(parts)-2], "/")
	if group == "" || topic == "" || partition == "" {
		return "", "", false
	}
	return group, topic + "/" + partition, true
}

func (s *StorageEngine) LoadOffset(groupID, topic string, partition int32) (int64, error) {
	key := fmt.Sprintf("_offsets/%s/%s/%d", groupID, topic, partition)

	ctx := context.Background()
	rc, err := s.objStore.Get(ctx, key)
	if err != nil {
		// Assume not found if error (simplified)
		return -1, nil
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return -1, err
	}

	offset, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return -1, err
	}
	// A commit read back from storage also constrains retention.
	s.recordCommitted(groupID, topic, partition, offset)
	return offset, nil
}

func (e *StorageEngine) GetTopicCount() int {
	return e.metadataCache.GetTopicCount()
}

func (e *StorageEngine) GetPartitionCount() int {
	return e.metadataCache.GetPartitionCount()
}
