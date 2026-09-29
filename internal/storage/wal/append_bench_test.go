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
	"testing"
)

// These benchmarks quantify the cost of the acks-aware durability barrier, so
// the trade-off can be re-measured on the target hardware rather than guessed
// at. Run with:
//
//	go test ./internal/storage/wal/ -bench=Append -benchtime=2000x
//
// Expect the acks=1 (sync) numbers to be materially slower and to scale with
// the storage device's fsync latency. The acks=0 path is unchanged from before
// the barrier was introduced.

func benchAppend(b *testing.B, sync bool) {
	dir := b.TempDir()
	pw, err := NewPartitionWAL(dir, "bench", 0, nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer pw.Close()

	// 1 KiB records, a realistic mid-size payload.
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := pw.Append(payload, 1, sync); err != nil {
			b.Fatalf("append %d: %v", i, err)
		}
	}
}

func BenchmarkAppendAcks0_NoSync(b *testing.B) { benchAppend(b, false) }
func BenchmarkAppendAcks1_Sync(b *testing.B)   { benchAppend(b, true) }
func BenchmarkAppendAcksAll_Sync(b *testing.B) { benchAppend(b, true) }

// BenchmarkAppendRoundTrip covers the mixed workload a real broker sees:
// a produce followed by a consume of the same offset, which is the pattern
// that used to corrupt the position index.
func BenchmarkAppendRoundTrip(b *testing.B) {
	dir := b.TempDir()
	pw, err := NewPartitionWAL(dir, "bench", 0, nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer pw.Close()

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off, err := pw.Append(payload, 1, true)
		if err != nil {
			b.Fatalf("append: %v", err)
		}
		if _, err := pw.Read(off); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}

// BenchmarkAppendConcurrentSync is the case group commit exists for.
//
// A single sequential writer fundamentally cannot avoid one fsync per record,
// so the acks=1 number there is a hard floor. Real producers arrive on many
// connections at once, and there the appends that pile up behind an in-flight
// fsync are all covered by that one flush. This benchmark is the evidence
// that the barrier is affordable under concurrent load.
func BenchmarkAppendConcurrentSync(b *testing.B) {
	dir := b.TempDir()
	pw, err := NewPartitionWAL(dir, "bench", 0, nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer pw.Close()

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := pw.Append(payload, 1, true); err != nil {
				b.Errorf("append: %v", err)
				return
			}
		}
	})
}

// BenchmarkAppendConcurrentNoSync is the control for the benchmark above.
func BenchmarkAppendConcurrentNoSync(b *testing.B) {
	dir := b.TempDir()
	pw, err := NewPartitionWAL(dir, "bench", 0, nil)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer pw.Close()

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i)
	}

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := pw.Append(payload, 1, false); err != nil {
				b.Errorf("append: %v", err)
				return
			}
		}
	})
}

var _ = fmt.Sprintf // keep fmt import if benchmarks are trimmed
