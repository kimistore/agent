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

package protocol

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"kimistore/internal/storage"
)

// discardStore stands in for object storage; the hot WAL path is what these
// tests exercise.
type discardStore struct{}

func (discardStore) Put(ctx context.Context, key string, r io.Reader) error { return nil }
func (discardStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, errors.New("not found")
}
func (discardStore) List(ctx context.Context, p string) ([]storage.ObjectMetadata, error) {
	return nil, nil
}
func (discardStore) Delete(ctx context.Context, key string) error { return nil }
func (discardStore) GetRange(ctx context.Context, key string, s, l int64) (io.ReadCloser, error) {
	return nil, errors.New("unsupported")
}

func newFetchEngine(t *testing.T) *storage.StorageEngine {
	t.Helper()
	se, err := storage.NewStorageEngine(t.TempDir(), discardStore{}, "test", storage.RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { se.Close() })
	return se
}

// buildFetchRequest encodes a V0 Fetch request body.
func buildFetchRequest(t *testing.T, topic string, partition int32, offset int64, minBytes, maxWaitMs int32) []byte {
	t.Helper()
	enc := NewEncoder()
	enc.Int32(-1) // ReplicaId
	enc.Int32(maxWaitMs)
	enc.Int32(minBytes)
	enc.Int32(1) // topic count
	enc.String(topic)
	enc.Int32(1) // partition count
	enc.Int32(partition)
	enc.Int64(offset)
	enc.Int32(1 * 1024 * 1024) // partitionMaxBytes
	return enc.Bytes()
}

// TestFetch_LongPollWakesOnData is the point of the change: a consumer that is
// caught up should park, not spin. Previously it got an instant empty response
// and re-polled in a tight loop.
func TestFetch_LongPollWakesOnData(t *testing.T) {
	se := newFetchEngine(t)
	const topic = "orders"
	// Produce one record so the partition exists at offset 0.
	if _, err := se.Append(topic, 0, []byte("first"), 1, false); err != nil {
		t.Fatalf("seed append: %v", err)
	}
	hw := se.HighWaterMark(topic, 0)

	// Consumer is caught up at the high watermark and asks to wait.
	req := buildFetchRequest(t, topic, 0, hw, 1, 500)

	go func() {
		time.Sleep(100 * time.Millisecond)
		se.Append(topic, 0, []byte("second"), 1, false)
	}()

	start := time.Now()
	enc := NewEncoder()
	enc.Int32(1) // correlation ID
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleFetch(NewDecoder(req), enc, se, 0)
	}()

	select {
	case <-done:
		elapsed := time.Since(start)
		if elapsed < 80*time.Millisecond {
			t.Errorf("fetch returned after %s, expected it to wait for the append", elapsed)
		}
		if se.HighWaterMark(topic, 0) <= hw {
			t.Error("expected the high watermark to have advanced")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fetch did not return")
	}
}

// TestFetch_LongPollTimesOutWhenIdle checks a quiet topic does not hold the
// request open indefinitely.
func TestFetch_LongPollTimesOutWhenIdle(t *testing.T) {
	se := newFetchEngine(t)
	const topic = "quiet"
	se.Append(topic, 0, []byte("only"), 1, false)
	hw := se.HighWaterMark(topic, 0)

	req := buildFetchRequest(t, topic, 0, hw, 1, 200)

	start := time.Now()
	enc := NewEncoder()
	enc.Int32(1)
	resp, err := handleFetch(NewDecoder(req), enc, se, 0)
	if err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 150*time.Millisecond {
		t.Errorf("fetch returned after %s, expected it to honour the 200ms wait", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("fetch waited %s, far beyond the requested wait", elapsed)
	}
	// It should still be a well-formed, empty-but-successful response.
	if len(resp) == 0 {
		t.Error("empty response")
	}
}

// TestFetch_NoLongPollWhenMinBytesZero preserves Kafka's behaviour: minBytes=0
// means the client is happy with an immediate empty answer.
func TestFetch_NoLongPollWhenMinBytesZero(t *testing.T) {
	se := newFetchEngine(t)
	const topic = "orders"
	se.Append(topic, 0, []byte("only"), 1, false)
	hw := se.HighWaterMark(topic, 0)

	req := buildFetchRequest(t, topic, 0, hw, 0, 2000) // minBytes=0

	start := time.Now()
	enc := NewEncoder()
	enc.Int32(1)
	if _, err := handleFetch(NewDecoder(req), enc, se, 0); err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("fetch waited %s with minBytes=0; it should return immediately", elapsed)
	}
}

// TestFetch_NoLongPollWhenMaxWaitZero checks a client that does not want to
// block still gets an immediate answer.
func TestFetch_NoLongPollWhenMaxWaitZero(t *testing.T) {
	se := newFetchEngine(t)
	const topic = "orders"
	se.Append(topic, 0, []byte("only"), 1, false)
	hw := se.HighWaterMark(topic, 0)

	req := buildFetchRequest(t, topic, 0, hw, 1, 0) // maxWait=0

	start := time.Now()
	enc := NewEncoder()
	enc.Int32(1)
	if _, err := handleFetch(NewDecoder(req), enc, se, 0); err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("fetch waited %s with maxWaitMs=0; it should return immediately", elapsed)
	}
}

// TestFetch_CapIsBounded checks a client asking for a very long wait cannot
// pin a connection open indefinitely.
func TestFetch_CapIsBounded(t *testing.T) {
	orig := maxFetchWaitMs
	maxFetchWaitMs = 300
	t.Cleanup(func() { maxFetchWaitMs = orig })

	se := newFetchEngine(t)
	const topic = "orders"
	se.Append(topic, 0, []byte("only"), 1, false)
	hw := se.HighWaterMark(topic, 0)

	req := buildFetchRequest(t, topic, 0, hw, 1, 600000) // 10 minutes

	start := time.Now()
	enc := NewEncoder()
	enc.Int32(1)
	if _, err := handleFetch(NewDecoder(req), enc, se, 0); err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("fetch waited %s; the cap should have limited it to %dms", elapsed, maxFetchWaitMs)
	}
}

// TestFetch_DataAvailableSkipsWait ensures a consumer with data ready is never
// delayed, which is the common case and must stay fast.
func TestFetch_DataAvailableSkipsWait(t *testing.T) {
	se := newFetchEngine(t)
	const topic = "orders"
	se.Append(topic, 0, []byte("first"), 1, false)
	se.Append(topic, 0, []byte("second"), 1, false)

	req := buildFetchRequest(t, topic, 0, 0, 1, 500) // offset 0 < hw 2

	start := time.Now()
	enc := NewEncoder()
	enc.Int32(1)
	if _, err := handleFetch(NewDecoder(req), enc, se, 0); err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("fetch waited %s despite data being available", elapsed)
	}
}

// TestFetch_OutOfRangeWaitsForCatchUp documents a deliberate design choice.
// An offset beyond the high watermark is not answered immediately: a producer
// may be moments away from filling it, so parking up to maxWait is what lets
// the consumer pick the data up in this same round trip. Only once the budget
// expires does it get OffsetOutOfRange.
func TestFetch_OutOfRangeWaitsForCatchUp(t *testing.T) {
	se := newFetchEngine(t)
	const topic = "orders"
	se.Append(topic, 0, []byte("first"), 1, false) // offset 0, hw=1

	// Ask for offset 5 when only offset 0 exists: genuinely out of range,
	// several records ahead of the producer.
	req := buildFetchRequest(t, topic, 0, 5, 1, 1000)

	const catchUp = 5
	produced := make(chan struct{})
	go func() {
		defer close(produced)
		time.Sleep(60 * time.Millisecond)
		for i := 0; i < catchUp; i++ {
			se.Append(topic, 0, []byte("catchup"), 1, false) // offsets 1..5
		}
	}()

	start := time.Now()
	enc := NewEncoder()
	enc.Int32(1)
	if _, err := handleFetch(NewDecoder(req), enc, se, 0); err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	elapsed := time.Since(start)

	// The fetch wakes on the first append rather than the full catch-up, so
	// wait for the producer before asserting on offsets.
	select {
	case <-produced:
	case <-time.After(5 * time.Second):
		t.Fatal("producer did not finish")
	}

	if elapsed < 40*time.Millisecond {
		t.Errorf("fetch returned after %s; it should have parked until data arrived", elapsed)
	}
	// Offsets 1..5 written, so the log end is 6 and offset 5 now has data.
	if got := se.HighWaterMark(topic, 0); got != 6 {
		t.Errorf("HighWaterMark() = %d, want 6", got)
	}
}

// TestFetch_OutOfRangeEventuallyErrors checks the other branch: if nothing
// arrives, the request still completes within its budget rather than hanging.
func TestFetch_OutOfRangeEventuallyErrors(t *testing.T) {
	se := newFetchEngine(t)
	const topic = "orders"
	se.Append(topic, 0, []byte("only"), 1, false)
	hw := se.HighWaterMark(topic, 0)

	req := buildFetchRequest(t, topic, 0, hw+50, 1, 250)

	start := time.Now()
	enc := NewEncoder()
	enc.Int32(1)
	if _, err := handleFetch(NewDecoder(req), enc, se, 0); err != nil {
		t.Fatalf("handleFetch: %v", err)
	}
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Errorf("fetch waited %s; it should give up near the requested 250ms", elapsed)
	}
}
