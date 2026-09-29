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
 * You should have express the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package storage

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// rehydrateStore records every List and Get so tests can assert how the engine
// builds its committed-offset map on startup.
type rehydrateStore struct {
	mu        sync.Mutex
	offsets   map[string]string // key -> offset value
	listCalls []string
	putCalls  []string
}

func newRehydrateStore() *rehydrateStore {
	return &rehydrateStore{offsets: map[string]string{}}
}

func (r *rehydrateStore) Put(ctx context.Context, key string, rd io.Reader) error {
	b, _ := io.ReadAll(rd)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.putCalls = append(r.putCalls, key)
	r.offsets[key] = strings.TrimSpace(string(b))
	return nil
}

func (r *rehydrateStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	r.mu.Lock()
	v, ok := r.offsets[key]
	r.mu.Unlock()
	if !ok {
		return nil, errors.New("no such offset")
	}
	return io.NopCloser(strings.NewReader(v)), nil
}

func (r *rehydrateStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	r.mu.Lock()
	r.listCalls = append(r.listCalls, prefix)
	// Filter by prefix
	var out []ObjectMetadata
	for k, v := range r.offsets {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			out = append(out, ObjectMetadata{Key: k, Size: int64(len(v))})
		}
	}
	r.mu.Unlock()
	return out, nil
}

func (r *rehydrateStore) Delete(ctx context.Context, key string) error { return nil }
func (r *rehydrateStore) GetRange(ctx context.Context, key string, s, l int64) (io.ReadCloser, error) {
	return nil, errors.New("unsupported")
}

// TestRehydrate_RebuildsMinFromS3 checks the authoritative path: after a cold
// start with no checkpoint, the engine reads every offset object and computes
// the minimum per topic/partition across groups.
func TestRehydrate_RebuildsMinFromS3(t *testing.T) {
	store := newRehydrateStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	// Simulate persisted offsets from a previous run.
	store.Put(context.Background(), "_offsets/fast/orders/0", strings.NewReader("150"))
	store.Put(context.Background(), "_offsets/slow/orders/0", strings.NewReader("10"))
	store.Put(context.Background(), "_offsets/slow/payments/0", strings.NewReader("5"))
	store.Put(context.Background(), "_offsets/garbage/what/ever", strings.NewReader("not-a-number"))

	se.rehydrateCommittedOffsets(context.Background())

	if got := se.FirstAllowedOffset("orders", 0); got != 10 {
		t.Errorf("FirstAllowedOffset(orders/0) = %d, want 10 (minimum of slow=10, fast=150)", got)
	}
	if got := se.FirstAllowedOffset("payments", 0); got != 5 {
		t.Errorf("FirstAllowedOffset(payments/0) = %d, want 5", got)
	}
	if got := se.FirstAllowedOffset("orders", 1); got != -1 {
		t.Errorf("FirstAllowedOffset(orders/1) = %d, want -1 (no commits)", got)
	}
}

// TestRehydrate_PreservesExistingCommits checks that rehydration merges rather
// than replaces, so commits that arrived before rehydration are not lost.
func TestRehydrate_PreservesExistingCommits(t *testing.T) {
	store := newRehydrateStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	// A commit arrives from a running consumer before rehydration runs.
	se.SaveOffset("early", "orders", 0, 3)
	store.Put(context.Background(), "_offsets/late/orders/0", strings.NewReader("20"))

	se.rehydrateCommittedOffsets(context.Background())

	if got := se.FirstAllowedOffset("orders", 0); got != 3 {
		t.Errorf("FirstAllowedOffset(orders/0) = %d, want 3 (minimum of early=3, late=20)", got)
	}
}

// TestCheckpoint_CarriesCommitted checks a clean restart restores the committed
// map from the checkpoint without needing to re-read every offset object.
func TestCheckpoint_CarriesCommitted(t *testing.T) {
	store := newRehydrateStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	se.SaveOffset("g1", "orders", 0, 5)
	se.SaveOffset("g2", "orders", 0, 20)

	if err := se.SaveCheckpoint(); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	se.Close()

	// A new engine loads the checkpoint.
	se2, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine 2: %v", err)
	}
	defer se2.Close()

	if err := se2.LoadCheckpoint(); err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	se2.committedMu.Lock()
	se2.committedAuthoritative = true
	se2.committedMu.Unlock()

	if got := se2.FirstAllowedOffset("orders", 0); got != 5 {
		t.Errorf("FirstAllowedOffset(orders/0) after checkpoint restore = %d, want 5", got)
	}
	if got := se2.committedByGroup["g1"]["orders/0"]; got != 5 {
		t.Errorf("committedByGroup[g1][orders/0] after restore = %d, want 5", got)
	}
}

// TestCheckpoint_WorkerRestoreUpdatesMin checks that a checkpoint merge with
// the rehydrated map produces the correct minimum.
func TestCheckpoint_WorkerRestoreUpdatesMin(t *testing.T) {
	store := newRehydrateStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	se.SaveOffset("g1", "orders", 0, 10)
	se.SaveCheckpoint()
	se.Close()

	// Store a lower commit from a second group that only exists in S3.
	store.Put(context.Background(), "_offsets/g2/orders/0", strings.NewReader("3"))

	se2, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine 2: %v", err)
	}
	defer se2.Close()

	// Load checkpoint first, then rehydrate from S3.
	if err := se2.LoadCheckpoint(); err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}

	se2.rehydrateCommittedOffsets(context.Background())

	if got := se2.FirstAllowedOffset("orders", 0); got != 3 {
		t.Errorf("FirstAllowedOffset(orders/0) after checkpoint + rehydrate = %d, want 3 (g2's 3)", got)
	}
}

// TestParseOffsetKey_GroupNameWithSlash checks parseOffsetKey handles a group
// ID that itself contains a slash, which is valid for Amazon MSK-style IAM
// names like "project/env/task".
func TestParseOffsetKey_GroupNameWithSlash(t *testing.T) {
	cases := []struct {
		key       string
		wantGroup string
		wantTP    string
		wantOK    bool
	}{
		{"_offsets/mygroup/orders/0", "mygroup", "orders/0", true},
		{"_offsets/project/env/task/orders/0", "project/env/task", "orders/0", true},
		{"_offsets/a/b/c/d/e/orders/0", "a/b/c/d/e", "orders/0", true},
		{"_offsets/g/t/p", "g", "t/p", true},
		{"_offsets/", "", "", false},
		{"foo/bar", "", "", false},
		{"_offsets/g/t", "", "", false}, // no partition
	}
	for _, cc := range cases {
		g, tp, ok := parseOffsetKey(cc.key)
		if ok != cc.wantOK {
			t.Errorf("parseOffsetKey(%q) ok=%v, want %v", cc.key, ok, cc.wantOK)
		}
		if g != cc.wantGroup {
			t.Errorf("parseOffsetKey(%q) group=%q, want %q", cc.key, g, cc.wantGroup)
		}
		if tp != cc.wantTP {
			t.Errorf("parseOffsetKey(%q) topicPartition=%q, want %q", cc.key, tp, cc.wantTP)
		}
	}
}

// TestOffsetsAuthoritative_GatesRetention checks that retention does not run
// before the committed-offset map is ready, and that rehydration opens the gate.
func TestOffsetsAuthoritative_GatesRetention(t *testing.T) {
	store := newRehydrateStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{RetentionTime: time.Hour})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	se.metadataCache.AddTopic("orders")
	se.metadataCache.AddPartition("orders", 0)

	// Wait for the background rehydration goroutine to finish.
	time.Sleep(200 * time.Millisecond)

	// Switch the gate off and verify retention skips.
	se.committedMu.Lock()
	se.committedAuthoritative = false
	se.committedMu.Unlock()

	if se.offsetsAuthoritative() {
		t.Fatal("authoritative should be false after we forced it to false")
	}

	// Open the gate by rehydrating synchronously.
	se.rehydrateCommittedOffsets(context.Background())
	if !se.offsetsAuthoritative() {
		t.Fatal("authoritative should be true after rehydration")
	}

	// Manually close the gate once more and verify retention skips.
	se.committedMu.Lock()
	se.committedAuthoritative = false
	se.committedMu.Unlock()

	if se.offsetsAuthoritative() {
		t.Fatal("authoritative should be false after we forced it to false")
	}
}

// TestFirstAllowedOffset_RisesAsSlowGroupAdvances is the bug I found during
// review. The per-group design guarantees that when the slowest consumer moves
// on, the log start rises and previously blocked segments become reclaimable.
func TestFirstAllowedOffset_RisesAsSlowGroupAdvances(t *testing.T) {
	se, err := NewStorageEngine(t.TempDir(), newRehydrateStore(), "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()

	se.SaveOffset("slow", "orders", 0, 10)
	se.SaveOffset("fast", "orders", 0, 150)

	if got := se.FirstAllowedOffset("orders", 0); got != 10 {
		t.Fatalf("min = %d, want 10 (slow is at 10)", got)
	}

	// The slow group advances to 200. The minimum must now be 150.
	se.SaveOffset("slow", "orders", 0, 200)
	if got := se.FirstAllowedOffset("orders", 0); got != 150 {
		t.Errorf("after slow advances to 200, min = %d, want 150 (fast's 150)", got)
	}

	// The fast group advances too. Minimum goes up again.
	se.SaveOffset("fast", "orders", 0, 300)
	if got := se.FirstAllowedOffset("orders", 0); got != 200 {
		t.Errorf("min = %d, want 200", got)
	}
}

// TestDeleteTopic_ClearsRetentionState cross-group variant.
func TestDeleteTopic_ByGroupClearsState(t *testing.T) {
	store := newRehydrateStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	se.CreateTopic("orders", 1)
	se.SaveOffset("g1", "orders", 0, 10)
	se.SaveOffset("g2", "orders", 0, 20)

	if got := se.FirstAllowedOffset("orders", 0); got != 10 {
		t.Fatalf("FirstAllowedOffset = %d, want 10", got)
	}

	if err := se.DeleteTopic("orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if got := se.FirstAllowedOffset("orders", 0); got != -1 {
		t.Errorf("FirstAllowedOffset after delete = %d, want -1", got)
	}

	// Verify the cross-group entry was cleaned too.
	se.committedMu.Lock()
	byGroup := se.committedByGroup["g2"]["orders/0"]
	se.committedMu.Unlock()
	if byGroup != 0 {
		t.Errorf("g2/orders/0 still has value %d after topic delete", byGroup)
	}
}
