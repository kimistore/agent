package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"go-stream/internal/storage"
)

// MockObjectStore
type MockObjectStore struct {
	data map[string][]byte
	mu   sync.Mutex
}

func NewMockObjectStore() *MockObjectStore {
	return &MockObjectStore{
		data: make(map[string][]byte),
	}
}

func (m *MockObjectStore) Put(ctx context.Context, key string, r io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.data[key] = b
	fmt.Printf("[MockS3] Uploaded %s (%d bytes)\n", key, len(b))
	return nil
}

func (m *MockObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func main() {
	walDir := "./tmp-storage-test-wal"
	os.RemoveAll(walDir) // cleanup

	store := NewMockObjectStore()

	engine, err := storage.NewStorageEngine(walDir, store, "test-bucket")
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	topic := "topic-1"
	partition := int32(0)

	// 1. Write small messages
	fmt.Println("Writing messages...")
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("msg-%d", i))
		if _, err := engine.Append(topic, partition, msg); err != nil {
			log.Fatal(err)
		}
	}

	// 2. Write a large message to force roll (our limit is 1MB, so write 1.1MB)
	largeMsg := make([]byte, 1153434) // ~1.1MB
	if _, err := engine.Append(topic, partition, largeMsg); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Wrote large message to trigger roll.")

	// The roll happens inside Append *before* writing if currentSize > Max.
	// Wait, my logic checks `p.currentSize > MaxSegmentSize`.
	// After writing 10 small messages, size is small.
	// We write large message, size becomes 1.1MB+.
	// NEXT Append checks size and rolls.

	// So we need ONE MORE append to trigger the roll of the large segment.
	if _, err := engine.Append(topic, partition, []byte("trigger-roll")); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Wrote trigger message.")

	// Now the segment containing the large message (and the small ones) should be sealed as `000...00.log`
	// (Check activeBaseOffset logic: first append set it to 0).

	// Wait for uploader
	fmt.Println("Waiting for uploader...")
	time.Sleep(5 * time.Second)

	// Check MockStore
	store.mu.Lock()
	count := len(store.data)
	store.mu.Unlock()

	fmt.Printf("MockStore has %d objects.\n", count)

	if count > 0 {
		fmt.Println("SUCCESS: Segments uploaded.")
		// Check keys
		store.mu.Lock()
		for k := range store.data {
			fmt.Println("Key:", k)
			if strings.HasPrefix(k, fmt.Sprintf("%s/%d/", topic, partition)) {
				fmt.Println("Key structure valid.")
			} else {
				log.Fatal("Invalid key structure")
			}
		}
		store.mu.Unlock()
	} else {
		log.Fatal("FAILURE: No segments uploaded.")
	}

	// Verify local file deleted?
	// The uploader deletes it.
	// Check directory.
	files, _ := os.ReadDir(walDir + "/" + topic + "/0")
	fmt.Println("Local files remaining:")
	for _, f := range files {
		fmt.Println("- " + f.Name())
	}
	// Should only have active.log (and maybe new sealed if I triggered another roll)
	// If upload was successful, sealed files should be gone.
}
