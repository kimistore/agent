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
	"sync"
	"testing"
)

// TestAppend_ConcurrentSyncIsContiguous is the main stress test for group
// commit. Many writers append concurrently with sync=true, and every returned
// offset must be unique and contiguous. A commit implementation that certified
// a write against a flush that predated its bytes would show up here as a
// duplicate offset or a gap.
func TestAppend_ConcurrentSyncIsContiguous(t *testing.T) {
	dir := t.TempDir()
	pw, err := NewPartitionWAL(dir, "concurrent", 0, 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pw.Close()

	const (
		writers   = 8
		perWriter = 40
		total     = writers * perWriter
	)

	var mu sync.Mutex
	seen := make(map[int64]string, total)
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				payload := fmt.Sprintf("w%d-i%d", id, i)
				off, err := pw.Append(mkRecord(payload), 1, true)
				if err != nil {
					t.Errorf("append %s: %v", payload, err)
					return
				}
				mu.Lock()
				if prev, dup := seen[off]; dup {
					t.Errorf("offset %d handed out twice (%q and %q)", off, prev, payload)
				}
				seen[off] = payload
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if len(seen) != total {
		t.Fatalf("recorded %d distinct offsets, want %d", len(seen), total)
	}
	// Offsets must be dense: 0..total-1, each written exactly once.
	for off := int64(0); off < int64(total); off++ {
		if _, ok := seen[off]; !ok {
			t.Errorf("gap at offset %d (offsets are not contiguous)", off)
		}
	}
	if got := pw.HighWaterMark(); got != int64(total) {
		t.Errorf("HighWaterMark() = %d, want %d", got, total)
	}

	// Every record must be readable back with its own payload.
	for off := int64(0); off < int64(total); off++ {
		body, err := pw.Read(off)
		if err != nil {
			t.Errorf("Read(%d): %v", off, err)
			continue
		}
		if got, want := recordPayload(t, body), seen[off]; got != want {
			t.Errorf("Read(%d) = %q, want %q", off, got, want)
		}
	}
}

// TestAppend_ConcurrentSyncThenRecover checks that everything a concurrent
// writer was told was durable is actually present after reopening the
// partition, i.e. the group-commit bookkeeping survives a restart.
func TestAppend_ConcurrentSyncThenRecover(t *testing.T) {
	dir := t.TempDir()

	pw, err := NewPartitionWAL(dir, "recover", 0, 0, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const (
		writers   = 6
		perWriter = 25
		total     = writers * perWriter
	)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if _, err := pw.Append(mkRecord(fmt.Sprintf("w%d-i%d", id, i)), 1, true); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	pw.Close()

	// Reopen: everything acknowledged must be recoverable.
	pw2, err := NewPartitionWAL(dir, "recover", 0, 0, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer pw2.Close()

	if got := pw2.HighWaterMark(); got != int64(total) {
		t.Errorf("HighWaterMark() after recovery = %d, want %d (acked writes were lost)", got, total)
	}
	for off := int64(0); off < int64(total); off++ {
		if _, err := pw2.Read(off); err != nil {
			t.Errorf("Read(%d) after recovery: %v", off, err)
		}
	}
}

// TestAppend_ConcurrentSyncWithRolls mixes the durability barrier with segment
// rolls, which is where the file handle is swapped underneath the commit path.
// It is the case most likely to surface a handle-reuse or ordering bug.
func TestAppend_ConcurrentSyncWithRolls(t *testing.T) {
	dir := t.TempDir()

	// Shrink the roll threshold so several segments are produced quickly.
	orig := MaxSegmentSize
	MaxSegmentSize = 8 * 1024
	defer func() { MaxSegmentSize = orig }()

	pw, err := NewPartitionWAL(dir, "rolls", 0, 0, func(UploadTask) {})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pw.Close()

	const (
		writers   = 4
		perWriter = 30
	)

	var mu sync.Mutex
	seen := make(map[int64]bool)
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := make([]byte, 512)
			for i := range payload {
				payload[i] = byte(id)
			}
			for i := 0; i < perWriter; i++ {
				off, err := pw.Append(payload, 1, true)
				if err != nil {
					t.Errorf("append: %v", err)
					return
				}
				mu.Lock()
				if seen[off] {
					t.Errorf("duplicate offset %d", off)
				}
				seen[off] = true
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if got := pw.HighWaterMark(); got != int64(writers*perWriter) {
		t.Errorf("HighWaterMark() = %d, want %d", got, writers*perWriter)
	}
	if got := len(seen); got != writers*perWriter {
		t.Errorf("distinct offsets = %d, want %d", got, writers*perWriter)
	}
}
