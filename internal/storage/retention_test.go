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
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// recordingStore records every List prefix so we can assert retention scopes
// its work, rather than listing the entire bucket.
type recordingStore struct {
	mu        sync.Mutex
	data      map[string][]byte
	meta      map[string]ObjectMetadata
	listCalls []string
	deleted   []string
	failLists bool
}

func newRecordingStore() *recordingStore {
	return &recordingStore{
		data: map[string][]byte{},
		meta: map[string]ObjectMetadata{},
	}
}

func (r *recordingStore) Put(ctx context.Context, key string, rd io.Reader) error {
	b, _ := io.ReadAll(rd)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data[key] = b
	r.meta[key] = ObjectMetadata{Key: key, Size: int64(len(b)), LastModified: time.Now().Unix()}
	return nil
}

func (r *recordingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, errors.New("not found")
}

func (r *recordingStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	r.mu.Lock()
	r.listCalls = append(r.listCalls, prefix)
	fail := r.failLists
	r.mu.Unlock()

	if fail {
		return nil, errors.New("simulated listing failure")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	var out []ObjectMetadata
	for k, m := range r.meta {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			m.Key = k
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (r *recordingStore) Delete(ctx context.Context, key string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted = append(r.deleted, key)
	delete(r.data, key)
	delete(r.meta, key)
	return nil
}

func (r *recordingStore) GetRange(ctx context.Context, key string, s, l int64) (io.ReadCloser, error) {
	return nil, errors.New("unsupported")
}

func (r *recordingStore) listCallsSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.listCalls))
	copy(out, r.listCalls)
	return out
}

func (r *recordingStore) deletedSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.deleted))
	copy(out, r.deleted)
	return out
}

func segKey(topic string, partition int32, start int64) string {
	return fmt.Sprintf("%s/%d/%020d.log", topic, partition, start)
}

func putSegment(t *testing.T, store *recordingStore, topic string, partition int32, start int64, size int) {
	t.Helper()
	key := segKey(topic, partition, start)
	if err := store.Put(context.Background(), key, strings.NewReader(strings.Repeat("x", size))); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

// TestRetention_ListsPerPartitionNotWholeBucket is the core efficiency fix.
// Retention used to call List("") every interval, which lists every object in
// the bucket regardless of how much was being reclaimed.
func TestRetention_ListsPerPartitionNotWholeBucket(t *testing.T) {
	store := newRecordingStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{
		RetentionTime: time.Hour,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()

	se.metadataCache.AddTopic("orders")
	se.metadataCache.AddPartition("orders", 0)
	se.metadataCache.AddPartition("orders", 1)
	se.metadataCache.AddTopic("payments")
	se.metadataCache.AddPartition("payments", 0)

	for i := int64(0); i < 3; i++ {
		putSegment(t, store, "orders", 0, i*100, 10)
		putSegment(t, store, "orders", 1, i*100, 10)
		putSegment(t, store, "payments", 0, i*100, 10)
	}

	se.applyRetention()

	calls := store.listCallsSnapshot()
	if len(calls) == 0 {
		t.Fatal("retention performed no listings")
	}
	for _, prefix := range calls {
		if prefix == "" {
			t.Fatalf("retention listed the entire bucket (prefix \"\"); calls=%v", calls)
		}
		if !strings.HasSuffix(prefix, "/") {
			t.Errorf("listing prefix %q is not partition-scoped", prefix)
		}
	}

	// Rehydration lists _offsets/ at startup; filter that out.
	seen := map[string]int{}
	for _, c := range calls {
		if c == "_offsets/" {
			continue
		}
		seen[c]++
	}
	for _, want := range []string{"orders/0/", "orders/1/", "payments/0/"} {
		if seen[want] != 1 {
			t.Errorf("prefix %q listed %d time(s), want 1 (all=%v)", want, seen[want], seen)
		}
	}
}

// TestRetention_SkipsUnknownTopics makes sure the sweep is bounded to what the
// agent actually knows about.
func TestRetention_SkipsUnknownTopics(t *testing.T) {
	store := newRecordingStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{
		RetentionTime: time.Hour,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	// A partition the metadata cache has no record of.
	putSegment(t, store, "ghost", 0, 0, 10)

	se.applyRetention()

	for _, c := range store.listCallsSnapshot() {
		if strings.HasPrefix(c, "ghost/") {
			t.Errorf("retention listed unknown topic %q", c)
		}
	}
}

// TestRetention_DisabledByDefault confirms the sweep is a no-op when no policy
// is configured, rather than listing the bucket every interval for nothing.
func TestRetention_DisabledByDefault(t *testing.T) {
	store := newRecordingStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()
	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()

	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()

	se.metadataCache.AddTopic("orders")
	se.metadataCache.AddPartition("orders", 0)
	putSegment(t, store, "orders", 0, 0, 10)

	se.applyRetention()

	// The background rehydration goroutine does list _offsets/ at startup, but
	// retention itself—with no policy configured—must not list anything.
	calls := store.listCallsSnapshot()
	for _, prefix := range calls {
		if prefix != "_offsets/" {
			t.Errorf("unexpected listing call %q when retention is disabled", prefix)
		}
	}
}

// TestRetention_PreservesSegmentsConsumersStillNeed is the safety property.
// A consumer sitting at offset 50 still needs every segment from 50 onward, so
// under maximum deletion pressure nothing may be reclaimed, however far over
// budget the partition is.
func TestRetention_PreservesSegmentsConsumersStillNeed(t *testing.T) {
	store := newRecordingStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{
		RetentionBytes: 1, // forces maximum pressure to delete
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()

	se.metadataCache.AddTopic("orders")
	se.metadataCache.AddPartition("orders", 0)

	putSegment(t, store, "orders", 0, 0, 1000)   // offsets [0,100)
	putSegment(t, store, "orders", 0, 100, 1000) // offsets [100,200)
	putSegment(t, store, "orders", 0, 200, 1000) // offsets [200, ...)

	// A consumer is still reading from offset 50, so it needs [50, ...).
	if err := se.SaveOffset("group-a", "orders", 0, 50); err != nil {
		t.Fatalf("SaveOffset: %v", err)
	}

	se.applyRetention()

	if deleted := store.deletedSnapshot(); len(deleted) != 0 {
		t.Errorf("retention deleted %v while a consumer still needed everything from offset 50", deleted)
	}
}

// TestRetention_ReclaimsSegmentsConsumersHavePassed checks the protection is
// not a blanket refusal: once a consumer has moved past a segment, that
// segment becomes reclaimable under the normal policies.
func TestRetention_ReclaimsSegmentsConsumersHavePassed(t *testing.T) {
	store := newRecordingStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{
		RetentionBytes: 1,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()

	se.metadataCache.AddTopic("orders")
	se.metadataCache.AddPartition("orders", 0)

	putSegment(t, store, "orders", 0, 0, 1000)   // [0,100)
	putSegment(t, store, "orders", 0, 100, 1000) // [100,200)
	putSegment(t, store, "orders", 0, 200, 1000) // [200, ...) newest

	// The consumer has read up to 250, so [0,100) and [100,200) are consumed.
	se.SaveOffset("group-a", "orders", 0, 250)

	se.applyRetention()

	deleted := map[string]bool{}
	for _, k := range store.deletedSnapshot() {
		deleted[k] = true
	}
	if !deleted[segKey("orders", 0, 0)] {
		t.Error("a fully consumed segment was not reclaimed")
	}
	if !deleted[segKey("orders", 0, 100)] {
		t.Error("a fully consumed segment was not reclaimed")
	}
	if deleted[segKey("orders", 0, 200)] {
		t.Error("retention deleted the newest segment, which cannot be proven consumed")
	}
}

// TestRetention_LowestCommitAcrossGroupsWins checks one group reading far
// behind is respected even when another group has raced ahead.
func TestRetention_LowestCommitAcrossGroupsWins(t *testing.T) {
	store := newRecordingStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{
		RetentionBytes: 1,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()

	se.metadataCache.AddTopic("orders")
	se.metadataCache.AddPartition("orders", 0)
	putSegment(t, store, "orders", 0, 0, 1000)
	putSegment(t, store, "orders", 0, 100, 1000)

	se.SaveOffset("fast-group", "orders", 0, 150) // caught up
	se.SaveOffset("slow-group", "orders", 0, 10)  // still catching up

	if got := se.FirstAllowedOffset("orders", 0); got != 10 {
		t.Errorf("FirstAllowedOffset() = %d, want 10 (the lowest across groups)", got)
	}

	se.applyRetention()
	for _, k := range store.deletedSnapshot() {
		if k == segKey("orders", 0, 0) {
			t.Error("retention deleted a segment the slow group still needs")
		}
	}
}

// TestRetention_UnknownPartitionHasNoProtection documents the -1 sentinel: with
// nothing committed, ordinary policies apply and old segments are reclaimed.
func TestRetention_UnknownPartitionHasNoProtection(t *testing.T) {
	se, err := NewStorageEngine(t.TempDir(), newRecordingStore(), "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	if got := se.FirstAllowedOffset("orders", 0); got != -1 {
		t.Errorf("FirstAllowedOffset() for an uncommitted partition = %d, want -1", got)
	}
}

// TestRetention_SurvivesListingFailure checks a transient object-store error on
// one partition does not abort the whole sweep.
func TestRetention_SurvivesListingFailure(t *testing.T) {
	store := newRecordingStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{
		RetentionTime: time.Hour,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()

	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()

	se.metadataCache.AddTopic("orders")
	se.metadataCache.AddPartition("orders", 0)
	se.metadataCache.AddTopic("payments")
	se.metadataCache.AddPartition("payments", 0)

	store.mu.Lock()
	store.failLists = true
	store.mu.Unlock()

	// Must not panic or block even though every listing fails.
	se.applyRetention()
}

// TestDeleteTopic_ClearsRetentionState checks committed-offset bookkeeping does
// not leak across a topic lifecycle.
func TestDeleteTopic_ClearsRetentionState(t *testing.T) {
	store := newRecordingStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer se.Close()
	se.committedMu.Lock()
	se.committedAuthoritative = true
	se.committedMu.Unlock()

	se.CreateTopic("doomed", 1)
	se.SaveOffset("g", "doomed", 0, 25)
	if got := se.FirstAllowedOffset("doomed", 0); got != 25 {
		t.Fatalf("FirstAllowedOffset() = %d, want 25", got)
	}

	if err := se.DeleteTopic("doomed"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if got := se.FirstAllowedOffset("doomed", 0); got != -1 {
		t.Errorf("FirstAllowedOffset() after delete = %d, want -1 (state should be cleared)", got)
	}
}
