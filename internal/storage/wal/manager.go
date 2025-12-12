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

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.partitions {
		_ = p.Close()
	}
	return nil
}
