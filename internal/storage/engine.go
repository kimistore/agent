package storage

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
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

func (s *StorageEngine) Append(topic string, partition int32, batch []byte) (int64, error) {
	return s.walMgr.Append(topic, partition, batch)
}

func (s *StorageEngine) Read(topic string, partition int32, offset int64) ([]byte, error) {
	// 1. Try WAL (Hot)
	data, err := s.walMgr.Read(topic, partition, offset)
	if err == nil {
		return data, nil
	}

	// 2. Try Object Store (Cold)
	// For MVP, implement naive fetch if not in WAL?
	// Let's rely on failing for now or implement later.
	return nil, fmt.Errorf("read failed (checking s3 not implemented): %v", err)
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
