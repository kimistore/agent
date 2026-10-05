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
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// idempotentBatch builds a real RecordBatch carrying a producer id, epoch and
// base sequence. recordBatch wraps the batch in a message set entry, so the
// producer fields live 12 bytes into the returned blob.
func idempotentBatch(baseOffset int64, n int32, pid int64, epoch int16, baseSeq int32) []byte {
	b := recordBatch(baseOffset, n)
	off := 12
	binary.BigEndian.PutUint64(b[off+43:off+51], uint64(pid))
	binary.BigEndian.PutUint16(b[off+51:off+53], uint16(epoch))
	binary.BigEndian.PutUint32(b[off+53:off+57], uint32(baseSeq))
	return b
}

func newProducerTestEngine(t *testing.T) *StorageEngine {
	t.Helper()
	se, err := NewStorageEngine(t.TempDir(), NewMockStore(), "bucket", RetentionConfig{}, WithAgentID("producer-test"))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if err := se.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	return se
}

// TestProducer_DeduplicatesRetry is the property D2 depends on: a batch that
// is retried after a timed-out ack must be recognised and answered with the
// offset it already occupies, not appended a second time.
func TestProducer_DeduplicatesRetry(t *testing.T) {
	se := newProducerTestEngine(t)
	defer se.Close()

	batch := idempotentBatch(0, 3, 7, 0, 0)

	off, dup, err := se.AppendIdempotentContext(context.Background(), "orders", 0, batch, 3, true, 7, 0, 0)
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if dup || off != 0 {
		t.Fatalf("first append: offset=%d duplicate=%v, want 0/false", off, dup)
	}

	off, dup, err = se.AppendIdempotentContext(context.Background(), "orders", 0, batch, 3, true, 7, 0, 0)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !dup || off != 0 {
		t.Fatalf("retry: offset=%d duplicate=%v, want 0/true", off, dup)
	}

	if got := se.HighWaterMark("orders", 0); got != 3 {
		t.Errorf("log end offset after a deduplicated retry = %d, want 3", got)
	}
}

// TestProducer_RejectsBadSequences covers the three ways a batch can be
// invalid for its producer, which a client must be told about rather than
// having its records silently misplaced.
func TestProducer_RejectsBadSequences(t *testing.T) {
	se := newProducerTestEngine(t)
	defer se.Close()
	ctx := context.Background()

	if _, _, err := se.AppendIdempotentContext(ctx, "orders", 0, idempotentBatch(0, 3, 7, 0, 0), 3, true, 7, 0, 0); err != nil {
		t.Fatalf("seed append: %v", err)
	}

	// An unknown producer whose sequence is not zero cannot be placed.
	if _, _, err := se.AppendIdempotentContext(ctx, "orders", 0, idempotentBatch(0, 1, 99, 0, 1), 1, true, 99, 0, 1); !errors.Is(err, ErrUnknownProducer) {
		t.Errorf("unknown producer error = %v, want ErrUnknownProducer", err)
	}

	// A sequence ahead of the expected one is out of order.
	if _, _, err := se.AppendIdempotentContext(ctx, "orders", 0, idempotentBatch(0, 1, 7, 0, 5), 1, true, 7, 0, 5); !errors.Is(err, ErrOutOfOrderSequence) {
		t.Errorf("out-of-order error = %v, want ErrOutOfOrderSequence", err)
	}

	// A newer epoch may open at sequence zero, and then fences the old one.
	if _, _, err := se.AppendIdempotentContext(ctx, "orders", 0, idempotentBatch(3, 1, 7, 1, 0), 1, true, 7, 1, 0); err != nil {
		t.Fatalf("new epoch append: %v", err)
	}
	if _, _, err := se.AppendIdempotentContext(ctx, "orders", 0, idempotentBatch(4, 1, 7, 0, 1), 1, true, 7, 0, 1); !errors.Is(err, ErrFencedProducerEpoch) {
		t.Errorf("fenced epoch error = %v, want ErrFencedProducerEpoch", err)
	}
}

// TestProducer_AllocatorSurvivesRestart checks producer ids are not reissued
// after a restart. A recycled id would let a new producer be mistaken for the
// old one and its first batch dropped as a duplicate.
func TestProducer_AllocatorSurvivesRestart(t *testing.T) {
	store := NewMockStore()
	dir := t.TempDir()
	ctx := context.Background()

	se1, err := NewStorageEngine(dir, store, "bucket", RetentionConfig{}, WithAgentID("p"))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	first, err := se1.AllocateProducerID(ctx)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	second, err := se1.AllocateProducerID(ctx)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	se1.Close()

	se2, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{}, WithAgentID("p"))
	if err != nil {
		t.Fatalf("engine 2: %v", err)
	}
	defer se2.Close()

	third, err := se2.AllocateProducerID(ctx)
	if err != nil {
		t.Fatalf("allocate after restart: %v", err)
	}
	if third <= second || second <= first {
		t.Errorf("producer ids %d, %d, %d are not strictly increasing across the restart", first, second, third)
	}
}

// TestProducer_RecoversStateFromLocalWAL pins the restart path: the local
// segment a crash leaves behind is the one that must be scanned to recognise a
// retried batch.
func TestProducer_RecoversStateFromLocalWAL(t *testing.T) {
	dir := t.TempDir()
	partDir := filepath.Join(dir, "orders", "0")
	if err := os.MkdirAll(partDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	f, err := os.Create(filepath.Join(partDir, "active.log"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Two batches from producer 7: sequences [0,3) at offset 0 and [3,5) at
	// offset 3.
	for _, e := range []struct {
		offset int64
		body   []byte
	}{
		{0, idempotentBatch(0, 3, 7, 0, 0)},
		{3, idempotentBatch(3, 2, 7, 0, 3)},
	} {
		hdr := make([]byte, 12)
		binary.BigEndian.PutUint64(hdr[0:8], uint64(e.offset))
		binary.BigEndian.PutUint32(hdr[8:12], uint32(len(e.body)))
		if _, err := f.Write(hdr); err != nil {
			t.Fatalf("write header: %v", err)
		}
		if _, err := f.Write(e.body); err != nil {
			t.Fatalf("write body: %v", err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	se := &StorageEngine{
		walDir:         dir,
		producerStates: make(map[string]*partitionProducerState),
		nextProducerID: 1,
	}
	se.recoverProducerState(context.Background())

	ps := se.producerStates["orders/0"]
	if ps == nil {
		t.Fatal("no producer state recovered")
	}
	e := ps.entries[7]
	if e == nil {
		t.Fatal("producer 7 was not recovered")
	}
	if e.nextSeq != 5 || e.lastOffset != 3 {
		t.Errorf("recovered producer 7: nextSeq=%d lastOffset=%d, want 5/3", e.nextSeq, e.lastOffset)
	}
	if off, ok := e.lookup(0); !ok || off != 0 {
		t.Errorf("recovered batch sequence 0 at offset %d (found=%v), want 0", off, ok)
	}
}
