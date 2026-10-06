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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestWALManager_AppendRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	mgr, err := NewManager(tempDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	topic := "test-topic"
	partition := int32(0)

	// Append 1
	data1 := []byte("hello")
	off1, err := mgr.Append(topic, partition, data1, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if off1 != 0 {
		t.Errorf("expected offset 0, got %d", off1)
	}

	// Append 2
	data2 := []byte("world")
	off2, err := mgr.Append(topic, partition, data2, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if off2 != 1 {
		t.Errorf("expected offset 1, got %d", off2)
	}

	// Read 1
	read1, err := mgr.Read(topic, partition, off1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read1, data1) {
		t.Errorf("expected %s, got %s", data1, read1)
	}

	// Read 2
	read2, err := mgr.Read(topic, partition, off2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read2, data2) {
		t.Errorf("expected %s, got %s", data2, read2)
	}
}

func TestPartitionWAL_Roll(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_roll_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	pDir := filepath.Join(tempDir, "topic", "0")
	p, err := NewPartitionWAL(pDir, "topic", 0, 0, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Override MaxSegmentSize for test
	originalSize := MaxSegmentSize
	MaxSegmentSize = 1024 * 1024
	defer func() { MaxSegmentSize = originalSize }()

	// Fill up close to 1MB
	// MaxSegmentSize is 1MB.
	// Let's write 500KB chunks.
	chunk := make([]byte, 500*1024)

	// Write 1
	if _, err := p.Append(chunk, 1, true); err != nil {
		t.Fatal(err)
	}

	// Write 2 (Should fit, total 1MB + headers)
	// Header is 12 bytes.
	// 500KB + 12 = 512012.
	// 2 * 512012 = 1024024. Just over 1MB?
	// MaxSegmentSize = 1 * 1024 * 1024 = 1048576.
	// So 2 chunks fit? 1024024 < 1048576. Yes.
	if _, err := p.Append(chunk, 1, true); err != nil {
		t.Fatal(err)
	}

	// Check directory: should only have active.log
	files, _ := os.ReadDir(pDir)
	if len(files) != 1 || files[0].Name() != "active.log" {
		t.Errorf("expected only active.log, got %v", files)
	}

	// Write 3 (Should exceed max size but roll happens on NEXT write)
	if _, err := p.Append(chunk, 1, true); err != nil {
		t.Fatal(err)
	}

	// Write 4 (Should trigger roll)
	if _, err := p.Append(chunk, 1, true); err != nil {
		t.Fatal(err)
	}

	// Check directory: active.log and 00000.log
	files, _ = os.ReadDir(pDir)
	hasSealed := false
	hasActive := false
	for _, f := range files {
		if f.Name() == "active.log" {
			hasActive = true
		} else if f.Name() == fmt.Sprintf("%020d.log", 0) {
			hasSealed = true
		}
	}

	if !hasActive || !hasSealed {
		t.Errorf("expected active and sealed logs, got %v", files)
	}
}
