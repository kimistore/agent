package storage

import (
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

	"go-stream/internal/storage/wal"
)

type StorageEngine struct {
	walMgr   *wal.Manager
	objStore ObjectStore
	bucket   string
	walDir   string

	quit chan struct{}
	wg   sync.WaitGroup

	// Cache for S3 List results (topic/partition -> []keys)
	segmentCache map[string][]string
	cacheMu      sync.RWMutex

	// Buffer for offsets (groupID/topic/partition -> offset)
	offsetBuf   map[string]int64
	offsetBufMu sync.Mutex
}

func NewStorageEngine(walDir string, objStore ObjectStore, bucket string) (*StorageEngine, error) {
	mgr, err := wal.NewManager(walDir)
	if err != nil {
		return nil, err
	}

	se := &StorageEngine{
		walMgr:       mgr,
		objStore:     objStore,
		bucket:       bucket,
		walDir:       walDir,
		quit:         make(chan struct{}),
		segmentCache: make(map[string][]string),
		offsetBuf:    make(map[string]int64),
	}

	// Start background uploader and offset flusher
	se.wg.Add(2)
	go se.uploaderLoop()
	go se.offsetFlusherLoop()

	return se, nil
}

func (s *StorageEngine) Append(topic string, partition int32, batch []byte, recordCount int) (int64, error) {
	return s.walMgr.Append(topic, partition, batch, recordCount)
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
			ctx := context.TODO()
			keys, err := s.objStore.List(ctx, prefix)
			if err != nil {
				s.cacheMu.Unlock()
				return nil, fmt.Errorf("failed to list s3: %v", err)
			}
			s.segmentCache[cacheKey] = keys
			bestKey = findBestKey(keys)
		}
		s.cacheMu.Unlock()
	}

	if bestKey == "" {
		return nil, fmt.Errorf("offset %d not found in verified segments", offset)
	}

	// Download and Scan
	ctx := context.TODO()
	rc, err := s.objStore.Get(ctx, bestKey)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return scanStreamForOffset(rc, offset)
}

func (s *StorageEngine) GetPartitions(topic string) ([]int32, error) {
	return s.walMgr.ListPartitions(topic)
}

func (s *StorageEngine) HighWaterMark(topic string, partition int32) int64 {
	return s.walMgr.HighWaterMark(topic, partition)
}

func (s *StorageEngine) CreateTopic(topic string, partitions int32) error {
	return s.walMgr.CreateTopic(topic, partitions)
}

func (s *StorageEngine) DeleteTopic(topic string) error {
	// Also remove from S3?
	// For MVP, deleting local WAL is mostly what we control.
	// Deleting from S3 needs LIST + DELETE which is heavy.
	// We'll leave S3 cleanup for later or async process.
	return s.walMgr.DeleteTopic(topic)
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

		if msgOffset == targetOffset {
			// Found it. Read data.
			data := make([]byte, msgSize)
			_, err := io.ReadFull(r, data)

			// PATCH: Rewrite the offset in the MessageSet to match the WAL offset.
			// The stored data is a MessageSet (Offset+Size+Msg).
			// The producer sent it with relative/zero offset. We must serve it with the actual log offset.
			if len(data) >= 8 {
				binary.BigEndian.PutUint64(data[0:8], uint64(targetOffset))
			}

			return data, err
		}

		// Skip data
		// Use Seek if possible? No, generic Reader.
		// CopyN to discard?
		if msgOffset < targetOffset {
			// Skip
			// We can use io.CopyN(io.Discard, r, int64(msgSize))
			_, err := io.CopyN(io.Discard, r, int64(msgSize))
			if err != nil {
				return nil, err
			}
		} else {
			// msgOffset > targetOffset
			// This shouldn't happen if we found the correct segment and segments are sorted/contiguous
			// But if it does, it means target doesn't exist?
			return nil, fmt.Errorf("offset %d passed (found %d)", targetOffset, msgOffset)
		}
	}
}

func (s *StorageEngine) Close() error {
	close(s.quit)
	s.wg.Wait()
	return s.walMgr.Close()
}

func (s *StorageEngine) uploaderLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(2 * time.Second)
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
		partition := parts[1]
		filename := parts[2]

		key := fmt.Sprintf("%s/%s/%s", topic, partition, filename)

		log.Printf("Uploading segment %s -> s3://%s/%s", rel, s.bucket, key)

		f, err := os.Open(path)
		if err != nil {
			return nil
		}
		// Don't defer f.Close() here inside loop easily, handle explicitly

		ctx := context.Background()
		if err := s.objStore.Put(ctx, key, f); err != nil {
			log.Printf("Failed to upload %s: %v", key, err)
			f.Close()
			return nil
		}
		f.Close()

		if err := os.Remove(path); err != nil {
			log.Printf("Failed to remove %s: %v", path, err)
		} else {
			log.Printf("Uploaded and trimmed %s", key)
		}

		// Update Cache
		cacheKey := fmt.Sprintf("%s/%s", topic, partition)
		s.cacheMu.Lock()
		if list, ok := s.segmentCache[cacheKey]; ok {
			s.segmentCache[cacheKey] = append(list, key)
		}
		s.cacheMu.Unlock()

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
	return nil
}

func (s *StorageEngine) offsetFlusherLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			s.flushOffsets()
			return
		case <-ticker.C:
			s.flushOffsets()
		}
	}
}

func (s *StorageEngine) flushOffsets() {
	s.offsetBufMu.Lock()
	if len(s.offsetBuf) == 0 {
		s.offsetBufMu.Unlock()
		return
	}
	// Copy buffer to release lock quickly
	todo := make(map[string]int64)
	for k, v := range s.offsetBuf {
		todo[k] = v
	}
	// Clear buffer (assume we will succeed or retry in next call if we failed? MVP: simple clear)
	// Actually better to clear only what we process. But for offsets, last write wins.
	// We can just clear.
	s.offsetBuf = make(map[string]int64)
	s.offsetBufMu.Unlock()

	ctx := context.Background()
	for k, offset := range todo {
		// k is "groupID/topic/partition"
		s3Key := fmt.Sprintf("_offsets/%s", k)
		data := []byte(fmt.Sprintf("%d", offset))
		if err := s.objStore.Put(ctx, s3Key, strings.NewReader(string(data))); err != nil {
			log.Printf("Failed to flush offset %s: %v", s3Key, err)
		}
	}
}

func (s *StorageEngine) LoadOffset(groupID, topic string, partition int32) (int64, error) {
	key := fmt.Sprintf("_offsets/%s/%s/%d", groupID, topic, partition)

	ctx := context.TODO()
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
	return offset, nil
}
