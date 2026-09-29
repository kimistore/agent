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
	"fmt"
	"io"
	"os"
	"strings"
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
	m.Mu.Lock()
	defer m.Mu.Unlock()
	b, ok := m.Data[key]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *MockStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	var res []ObjectMetadata
	for k, v := range m.Meta {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			res = append(res, v)
		}
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

	// The test controls its own store and knows no groups need protecting.
	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()

	// Manually inject segments into S3
	topic := "topic1"
	partition := "0"

	// Retention walks the topic/partition pairs known to the metadata cache.
	se.metadataCache.AddTopic(topic)
	se.metadataCache.AddPartition(topic, 0)

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

	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()

	topic := "topic1"
	partition := "0"

	se.metadataCache.AddTopic(topic)
	se.metadataCache.AddPartition(topic, 0)

	// Segment 1: Old (2 hours ago)
	key1 := fmt.Sprintf("%s/%s/%020d.log", topic, partition, 0)
	mockStore.Put(context.Background(), key1, io.LimitReader(&zeroReader{}, 10))
	// Hack: Modify LastModified under the lock so background rehydration
	// does not race with it.
	mockStore.Mu.Lock()
	meta1 := mockStore.Meta[key1]
	meta1.LastModified = time.Now().Add(-2 * time.Hour).Unix()
	mockStore.Meta[key1] = meta1
	mockStore.Mu.Unlock()

	// Segment 2: New (30 mins ago)
	key2 := fmt.Sprintf("%s/%s/%020d.log", topic, partition, 10)
	mockStore.Put(context.Background(), key2, io.LimitReader(&zeroReader{}, 10))
	mockStore.Mu.Lock()
	meta2 := mockStore.Meta[key2]
	meta2.LastModified = time.Now().Add(-30 * time.Minute).Unix()
	mockStore.Meta[key2] = meta2
	mockStore.Mu.Unlock()

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

func TestDeleteTopicS3Cleanup(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "delete_topic_cleanup")
	defer os.RemoveAll(tmpDir)

	mockStore := NewMockStore()

	// Create engine
	se, err := NewStorageEngine(tmpDir, mockStore, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer se.Close()

	topic := "topic-to-delete"
	partition := int32(0)

	// Create topic
	err = se.CreateTopic(topic, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Append some messages to trigger WAL file creation
	_, err = se.Append(topic, partition, []byte("msg1"), 1, true)
	if err != nil {
		t.Fatal(err)
	}

	// Put some mocked objects directly into S3 for this topic
	key1 := fmt.Sprintf("%s/%d/00000000000000000000.log", topic, partition)
	key2 := fmt.Sprintf("%s/%d/00000000000000000000.index", topic, partition)
	mockStore.Put(context.Background(), key1, strings.NewReader("log-data"))
	mockStore.Put(context.Background(), key2, strings.NewReader("index-data"))

	// Put an object for another topic that shouldn't be deleted
	otherKey := "other-topic/0/00000000000000000000.log"
	mockStore.Put(context.Background(), otherKey, strings.NewReader("other-log-data"))

	// Verify they are in MockStore
	mockStore.Mu.Lock()
	if len(mockStore.Data) != 3 {
		mockStore.Mu.Unlock()
		t.Fatalf("Expected 3 objects in mock S3 store, got %d", len(mockStore.Data))
	}
	mockStore.Mu.Unlock()

	// Call DeleteTopic
	err = se.DeleteTopic(topic)
	if err != nil {
		t.Fatal(err)
	}

	// Since S3 deletion is asynchronous, wait for it with a timeout
	start := time.Now()
	for time.Since(start) < 2*time.Second {
		mockStore.Mu.Lock()
		_, hasKey1 := mockStore.Data[key1]
		_, hasKey2 := mockStore.Data[key2]
		mockStore.Mu.Unlock()

		if !hasKey1 && !hasKey2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Assert S3 objects for deleted topic are cleaned up
	mockStore.Mu.Lock()
	defer mockStore.Mu.Unlock()

	if _, ok := mockStore.Data[key1]; ok {
		t.Errorf("Expected cold log object %s to be deleted from S3", key1)
	}
	if _, ok := mockStore.Data[key2]; ok {
		t.Errorf("Expected cold index object %s to be deleted from S3", key2)
	}

	// Assert other topic object remains
	if _, ok := mockStore.Data[otherKey]; !ok {
		t.Errorf("Expected cold object of other topic %s to remain in S3", otherKey)
	}
}
