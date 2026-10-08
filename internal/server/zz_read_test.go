package server

import (
	"testing"
	"time"

	"kimistore/internal/storage"
)

// Does the broker's own read path serve a record that is still in the active
// segment? This is the gap between "hw says 1" and "the fetch returned nothing".
func TestProbe_ReadActiveSegment(t *testing.T) {
	engine, err := storage.NewStorageEngine(t.TempDir(), newMemoryStore(), "test", storage.RetentionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	if err := engine.CreateTopic("probe", 1); err != nil {
		t.Fatal(err)
	}
	off, err := engine.Append("probe", 0, []byte("hello"), 1, true)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	t.Logf("appended at offset %d; hw=%d logStart=%d", off, engine.HighWaterMark("probe", 0), engine.LogStartOffset("probe", 0))

	// Read immediately: the record is in the active, unsealed segment.
	recs, high, err := engine.ReadBatch("probe", 0, off, 1<<20)
	t.Logf("immediate ReadBatch: n=%d hw=%d err=%v", len(recs), high, err)

	time.Sleep(2 * time.Second) // well past the flush interval
	recs2, high2, err2 := engine.ReadBatch("probe", 0, off, 1<<20)
	t.Logf("after 2s ReadBatch: n=%d hw=%d err=%v", len(recs2), high2, err2)

	if len(recs) == 0 && len(recs2) == 0 {
		t.Error("record is unreadable from both the active and the sealed segment")
	}
}
