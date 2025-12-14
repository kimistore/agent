package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// WALManager manages the write-ahead logs for multiple partitions.
type Manager struct {
	baseDir    string
	partitions map[string]*PartitionWAL // key: "topic/partition"
	mu         sync.RWMutex
}

func NewManager(baseDir string) (*Manager, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	return &Manager{
		baseDir:    baseDir,
		partitions: make(map[string]*PartitionWAL),
	}, nil
}

func (m *Manager) getPartitionWAL(topic string, partition int32) (*PartitionWAL, error) {
	key := fmt.Sprintf("%s/%d", topic, partition)

	m.mu.RLock()
	p, ok := m.partitions[key]
	m.mu.RUnlock()
	if ok {
		return p, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double check
	if p, ok := m.partitions[key]; ok {
		return p, nil
	}

	dir := filepath.Join(m.baseDir, topic, fmt.Sprintf("%d", partition))
	p, err := NewPartitionWAL(dir)
	if err != nil {
		return nil, err
	}
	m.partitions[key] = p
	return p, nil
}

func (m *Manager) Append(topic string, partition int32, batch []byte, recordCount int) (int64, error) {
	p, err := m.getPartitionWAL(topic, partition)
	if err != nil {
		return 0, err
	}
	return p.Append(batch, recordCount)
}

func (m *Manager) Read(topic string, partition int32, offset int64) ([]byte, error) {
	p, err := m.getPartitionWAL(topic, partition)
	if err != nil {
		return nil, err
	}
	return p.Read(offset)
}

func (m *Manager) ListPartitions(topic string) ([]int32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Scan loaded partitions
	var partitions []int32
	prefix := topic + "/"
	for k := range m.partitions {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			var pid int32
			fmt.Sscanf(k[len(prefix):], "%d", &pid)
			partitions = append(partitions, pid)
		}
	}

	// Also scan directory because not all might be loaded in memory
	topicDir := filepath.Join(m.baseDir, topic)
	entries, err := os.ReadDir(topicDir)
	if err == nil {
		for _, e := range entries {
			if e.IsDir() {
				var pid int32
				if _, err := fmt.Sscanf(e.Name(), "%d", &pid); err == nil {
					// Dedup
					found := false
					for _, p := range partitions {
						if p == pid {
							found = true
							break
						}
					}
					if !found {
						partitions = append(partitions, pid)
					}
				}
			}
		}
	}

	return partitions, nil
}

func (m *Manager) HighWaterMark(topic string, partition int32) int64 {
	p, err := m.getPartitionWAL(topic, partition)
	if err != nil {
		return 0
	}
	return p.HighWaterMark()
}

func (m *Manager) CreateTopic(topic string, partitions int32) error {
	for i := int32(0); i < partitions; i++ {
		// Just accessing it creates it
		_, err := m.getPartitionWAL(topic, i)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) DeleteTopic(topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close and remove from memory
	prefix := topic + "/"
	for k, p := range m.partitions {
		if k == topic || (len(k) > len(prefix) && k[:len(prefix)] == prefix) {
			_ = p.Close()
			delete(m.partitions, k)
		}
	}

	// Remove from disk
	topicDir := filepath.Join(m.baseDir, topic)
	return os.RemoveAll(topicDir)
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.partitions {
		_ = p.Close()
	}
	return nil
}
