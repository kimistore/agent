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
}

func NewStorageEngine(walDir string, objStore ObjectStore, bucket string) (*StorageEngine, error) {
	mgr, err := wal.NewManager(walDir)
	if err != nil {
		return nil, err
	}

	se := &StorageEngine{
		walMgr:   mgr,
		objStore: objStore,
		bucket:   bucket,
		walDir:   walDir,
		quit:     make(chan struct{}),
	}

	// Start background uploader
	se.wg.Add(1)
	go se.uploaderLoop()

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
	// Construct prefix: topic/partition/
	prefix := fmt.Sprintf("%s/%d/", topic, partition)

	// Note: For MVP, Listing every read is inefficient.
	// Ideally we cache the segment list or use an index.
	ctx := context.TODO()
	keys, err := s.objStore.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list s3: %v", err)
	}

	// Find the segment that SHOULD contain the offset.
	// Format: <offset>.log
	// We want max(start_offset) where start_offset <= requested_offset

	var bestKey string
	var bestStartOffset int64 = -1

	for _, k := range keys {
		if !strings.HasSuffix(k, ".log") {
			continue
		}
		// key: topic/partition/000.log
		parts := strings.Split(k, "/")
		filename := parts[len(parts)-1]
		baseName := strings.TrimSuffix(filename, ".log")
		startOffset, err := strconv.ParseInt(baseName, 10, 64)
		if err != nil {
			continue
		}

		if startOffset <= offset {
			if startOffset > bestStartOffset {
				bestStartOffset = startOffset
				bestKey = k
			}
		}
	}

	if bestKey == "" {
		return nil, fmt.Errorf("offset %d not found in verified segments", offset)
	}

	// Download and Scan
	rc, err := s.objStore.Get(ctx, bestKey)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return scanStreamForOffset(rc, offset)
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
		return nil
	})

	if err != nil {
		log.Printf("Error walking WAL dir: %v", err)
	}
}
