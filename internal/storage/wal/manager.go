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
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// WALManager manages the write-ahead logs for multiple partitions.
type Manager struct {
	baseDir    string
	partitions map[string]*PartitionWAL // key: "topic/partition"
	onRoll     func(UploadTask)
	mu         sync.RWMutex

	// seeds holds the log end offset recovered from durable metadata, keyed
	// the same way as partitions. It is consulted when a partition WAL is
	// opened, so the first write after a restart continues the log rather
	// than starting it over.
	seeds map[string]int64
}

func NewManager(baseDir string, onRoll func(UploadTask)) (*Manager, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	return &Manager{
		baseDir:    baseDir,
		partitions: make(map[string]*PartitionWAL),
		onRoll:     onRoll,
		seeds:      make(map[string]int64),
	}, nil
}

func partitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

// SetSeed records the durable log end offset for a partition. It is a no-op
// for a lower value, so a stale or empty manifest can never rewind a log.
func (m *Manager) SetSeed(topic string, partition int32, logEndOffset int64) {
	key := partitionKey(topic, partition)

	m.mu.Lock()
	if cur, ok := m.seeds[key]; !ok || logEndOffset > cur {
		m.seeds[key] = logEndOffset
	}
	m.mu.Unlock()

	// If the partition is already open, move it forward immediately.
	m.mu.RLock()
	p, ok := m.partitions[key]
	m.mu.RUnlock()
	if ok {
		p.SeedPartition(m.seeds[key])
	}
}

// Seed restores a whole partition set in one call, used at startup before
// any partition has been opened.
func (m *Manager) Seed(seeds map[string]int64) {
	m.mu.Lock()
	for key, off := range seeds {
		if cur, ok := m.seeds[key]; !ok || off > cur {
			m.seeds[key] = off
		}
	}
	open := make(map[*PartitionWAL]int64, len(m.partitions))
	for key, p := range m.partitions {
		if off, ok := m.seeds[key]; ok {
			open[p] = off
		}
	}
	m.mu.Unlock()

	for p, off := range open {
		p.SeedPartition(off)
	}
}

func (m *Manager) getPartitionWAL(topic string, partition int32) (*PartitionWAL, error) {
	key := partitionKey(topic, partition)

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
	p, err := NewPartitionWAL(dir, topic, partition, m.seeds[key], m.onRoll)
	if err != nil {
		return nil, err
	}
	m.partitions[key] = p
	return p, nil
}

// Append writes a batch to the partition's WAL. sync=true fsyncs before
// returning, making the returned offset safe to acknowledge to the client.
func (m *Manager) Append(topic string, partition int32, batch []byte, recordCount int, sync bool) (int64, error) {
	p, err := m.getPartitionWAL(topic, partition)
	if err != nil {
		return 0, err
	}
	return p.Append(batch, recordCount, sync)
}

func (m *Manager) Read(topic string, partition int32, offset int64) ([]byte, error) {
	p, err := m.getPartitionWAL(topic, partition)
	if err != nil {
		return nil, err
	}
	return p.Read(offset)
}

// ReadBatch serves consecutive stored blobs starting at offset, bounded by
// maxBytes, and reports the offset to fetch next.
func (m *Manager) ReadBatch(topic string, partition int32, offset, maxBytes int64) ([]byte, int64, error) {
	p, err := m.getPartitionWAL(topic, partition)
	if err != nil {
		return nil, 0, err
	}
	return p.ReadBatch(offset, maxBytes)
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
			if _, err := fmt.Sscanf(k[len(prefix):], "%d", &pid); err != nil {
				continue // not a partition directory
			}
			partitions = append(partitions, pid)
		}
	}
	// Seeds describe partitions known from durable metadata even when no
	// local directory or in-memory entry exists yet.
	for k := range m.seeds {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			var pid int32
			if _, err := fmt.Sscanf(k[len(prefix):], "%d", &pid); err != nil {
				continue // not a partition entry
			}
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

	// Close and remove from memory. Discard rather than Close: sealing here
	// would upload the segment, and the upload can complete after the
	// caller's delete and resurrect the objects it removed.
	prefix := topic + "/"
	for k, p := range m.partitions {
		if k == topic || (len(k) > len(prefix) && k[:len(prefix)] == prefix) {
			_ = p.Discard()
			delete(m.partitions, k)
			delete(m.seeds, k)
		}
	}

	// Remove from disk
	topicDir := filepath.Join(m.baseDir, topic)
	return os.RemoveAll(topicDir)
}

// FlushPartition seals a partition's active segment so the uploader can store
// it. It is a no-op when the partition has nothing buffered or does not exist
// yet, so the durability loop can call it freely.
func (m *Manager) FlushPartition(topic string, partition int32) {
	p, err := m.getPartitionWAL(topic, partition)
	if err != nil {
		return
	}
	if err := p.Flush(); err != nil {
		log.Printf("WAL: flushing %s/%d: %v", topic, partition, err)
	}
}

// SealAll turns every partition's active segment into a sealed one, which
// hands it to the uploader. Used on shutdown so nothing is stranded on local
// disk.
func (m *Manager) SealAll() {
	m.mu.RLock()
	parts := make([]*PartitionWAL, 0, len(m.partitions))
	for _, p := range m.partitions {
		parts = append(parts, p)
	}
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, p := range parts {
		wg.Add(1)
		go func(p *PartitionWAL) {
			defer wg.Done()
			if err := p.Close(); err != nil {
				log.Printf("WAL: sealing %s on shutdown: %v", p.dir, err)
			}
		}(p)
	}
	wg.Wait()
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.partitions {
		_ = p.Close()
	}
	return nil
}
