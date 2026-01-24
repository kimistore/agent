package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

type MockStore struct {
	Data map[string][]byte
	Meta map[string]ObjectMetadata
	Mu   sync.Mutex
}

func NewMockStore() *MockStore {
	return &MockStore{
		Data: make(map[string][]byte),
		Meta: make(map[string]ObjectMetadata),
	}
}

func (m *MockStore) Put(ctx context.Context, key string, r io.Reader) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	b, _ := io.ReadAll(r)
	m.Data[key] = b
	m.Meta[key] = ObjectMetadata{
		Key:          key,
		Size:         int64(len(b)),
		LastModified: time.Now().Unix(),
	}
	return nil
}

func (m *MockStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}

func (m *MockStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	var res []ObjectMetadata
	for _, v := range m.Meta {
		res = append(res, v)
	}
	return res, nil
}

func (m *MockStore) Delete(ctx context.Context, key string) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	delete(m.Data, key)
	delete(m.Meta, key)
	return nil
}

func (m *MockStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return nil, nil // TODO: Implement if needed for tests
}

func TestRetention_Size(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "retention_size")
	defer os.RemoveAll(tmpDir)

	maxSize := int64(100)
	cfg := RetentionConfig{
		RetentionBytes: maxSize,
	}

	mockStore := NewMockStore()

	// Create Engine
	// We need walDir to exist
	se, err := NewStorageEngine(tmpDir, mockStore, "bucket", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer se.Close()

	// Manually inject segments into S3
	topic := "topic1"
	partition := "0"

	// Segment 1: 60 bytes. Offset 0.
	key1 := fmt.Sprintf("%s/%s/%020d.log", topic, partition, 0)
	mockStore.Put(context.Background(), key1, io.LimitReader(&zeroReader{}, 60))

	// Segment 2: 60 bytes. Offset 100.
	key2 := fmt.Sprintf("%s/%s/%020d.log", topic, partition, 100)
	mockStore.Put(context.Background(), key2, io.LimitReader(&zeroReader{}, 60))

	// Total size = 120. Limit = 100.
	// Check retention
	se.applyRetention()

	// Should delete segment 1 (oldest).
	mockStore.Mu.Lock()
	defer mockStore.Mu.Unlock()

	if _, ok := mockStore.Data[key1]; ok {
		t.Errorf("Segment 1 should be deleted. Size 120 > 100.")
	}
	if _, ok := mockStore.Data[key2]; !ok {
		t.Errorf("Segment 2 should remain.")
	}
}

func TestRetention_Time(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "retention_time")
	defer os.RemoveAll(tmpDir)

	retentionTime := 1 * time.Hour
	cfg := RetentionConfig{
		RetentionTime: retentionTime,
	}

	mockStore := NewMockStore()
	se, _ := NewStorageEngine(tmpDir, mockStore, "bucket", cfg)
	defer se.Close()

	topic := "topic1"
	partition := "0"

	// Segment 1: Old (2 hours ago)
	key1 := fmt.Sprintf("%s/%s/%020d.log", topic, partition, 0)
	mockStore.Put(context.Background(), key1, io.LimitReader(&zeroReader{}, 10))
	// Hack: Modify LastModified
	meta1 := mockStore.Meta[key1]
	meta1.LastModified = time.Now().Add(-2 * time.Hour).Unix()
	mockStore.Meta[key1] = meta1

	// Segment 2: New (30 mins ago)
	key2 := fmt.Sprintf("%s/%s/%020d.log", topic, partition, 10)
	mockStore.Put(context.Background(), key2, io.LimitReader(&zeroReader{}, 10))
	meta2 := mockStore.Meta[key2]
	meta2.LastModified = time.Now().Add(-30 * time.Minute).Unix()
	mockStore.Meta[key2] = meta2

	se.applyRetention()

	mockStore.Mu.Lock()
	defer mockStore.Mu.Unlock()

	if _, ok := mockStore.Data[key1]; ok {
		t.Errorf("Segment 1 should be deleted (too old).")
	}
	if _, ok := mockStore.Data[key2]; !ok {
		t.Errorf("Segment 2 should remain.")
	}
}

type zeroReader struct{}

func (z *zeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
