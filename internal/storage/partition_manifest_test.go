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
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// putKeys returns a snapshot of the keys the store has been asked to write.
func putKeys(store *rehydrateStore) []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	out := make([]string, len(store.putCalls))
	copy(out, store.putCalls)
	return out
}

// TestManifest_PerPartitionKeys checks the Phase 0 split: the durable position
// of a partition lives in its own object under _topics/, not in one bucket-wide
// manifest that a second writer would clobber.
func TestManifest_PerPartitionKeys(t *testing.T) {
	store := newRehydrateStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{}, WithAgentID("test-agent"))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	if err := se.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := se.Append("orders", 0, batchOfN(4), 4, true); err != nil {
		t.Fatalf("append 0: %v", err)
	}
	if _, err := se.Append("orders", 1, batchOfN(7), 7, true); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := se.SaveManifest(); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	se.Close()

	for _, key := range []string{
		"_topics/orders/_manifest/0",
		"_topics/orders/_manifest/1",
	} {
		rc, err := store.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("expected per-partition manifest %s: %v", key, err)
		}
		rc.Close()
	}
	if _, err := store.Get(context.Background(), legacyManifestKey); err == nil {
		t.Errorf("a new agent must not write the bucket-global legacy manifest %s", legacyManifestKey)
	}

	// A fresh agent recovers both positions from the per-partition objects,
	// with no local WAL and no checkpoint to help it.
	se2, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{}, WithAgentID("test-agent"))
	if err != nil {
		t.Fatalf("engine 2: %v", err)
	}
	defer se2.Close()
	if got := se2.HighWaterMark("orders", 0); got != 4 {
		t.Errorf("recovered LEO for orders/0 = %d, want 4", got)
	}
	if got := se2.HighWaterMark("orders", 1); got != 7 {
		t.Errorf("recovered LEO for orders/1 = %d, want 7", got)
	}
}

// TestManifest_OnlyDirtyPartitionsRewritten pins the cost property: a
// checkpoint rewrites the partitions whose position moved, not every partition
// in the log. Rewriting all of them on every checkpoint is what would make the
// split a net loss.
func TestManifest_OnlyDirtyPartitionsRewritten(t *testing.T) {
	store := newRehydrateStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{}, WithAgentID("test-agent"))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer se.Close()

	if err := se.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Everything that existed before this point is already durable.
	before := len(putKeys(store))

	if _, err := se.Append("orders", 1, batchOfN(3), 3, true); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := se.SaveManifest(); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	var wrote []string
	for _, key := range putKeys(store)[before:] {
		if strings.HasPrefix(key, topicsMetadataPrefix) {
			wrote = append(wrote, key)
		}
	}
	if len(wrote) != 1 || wrote[0] != "_topics/orders/_manifest/1" {
		t.Fatalf("expected only the dirty partition 1 to be rewritten, got %v", wrote)
	}
}

// TestManifest_LegacyMigration checks an upgrade: a bucket written before
// Phase 0 has only the whole-log manifest, and the new agent must recover the
// same position from it and re-persist it per partition.
func TestManifest_LegacyMigration(t *testing.T) {
	store := newRehydrateStore()

	legacy, err := json.Marshal(manifest{
		WriterEpoch: 0,
		Writer:      "old-agent",
		Partitions: []manifestEntry{{
			Topic:        "orders",
			Partition:    0,
			LogEndOffset: 250,
		}},
	})
	if err != nil {
		t.Fatalf("marshal legacy manifest: %v", err)
	}
	if err := store.Put(context.Background(), legacyManifestKey, bytes.NewReader(legacy)); err != nil {
		t.Fatalf("seed legacy manifest: %v", err)
	}

	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{}, WithAgentID("new-agent"))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	if got := se.HighWaterMark("orders", 0); got != 250 {
		t.Fatalf("recovered LEO from legacy manifest = %d, want 250", got)
	}

	// The next save rewrites it in the new shape, so the dependency on the
	// old key is one-time.
	if err := se.SaveManifest(); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	se.Close()

	rc, err := store.Get(context.Background(), "_topics/orders/_manifest/0")
	if err != nil {
		t.Fatalf("legacy position was not migrated to a per-partition manifest: %v", err)
	}
	defer rc.Close()
}

// TestManifest_LegacyFromNewerEpochIsRefused checks that the fencing rule the
// checkpoint already had extends to the legacy manifest: a position written by
// an agent that held the log after us must not be served.
func TestManifest_LegacyFromNewerEpochIsRefused(t *testing.T) {
	store := newRehydrateStore()

	legacy, err := json.Marshal(manifest{WriterEpoch: 99, Partitions: []manifestEntry{{
		Topic:        "orders",
		Partition:    0,
		LogEndOffset: 5,
	}}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.Put(context.Background(), legacyManifestKey, bytes.NewReader(legacy)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err = NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{}, WithAgentID("new-agent"))
	if err == nil {
		t.Fatal("an agent must refuse to serve a log whose manifest is from a newer writer")
	}
	if !strings.Contains(err.Error(), "newer writer") {
		t.Fatalf("expected a superseded-writer refusal, got %v", err)
	}
}

// TestCheckpoint_NamespacedPerAgentAndLegacyFallback checks the checkpoint key
// is scoped to this agent, and that an older bucket-global checkpoint is still
// read once so an upgrade keeps its fast path.
func TestCheckpoint_NamespacedPerAgentAndLegacyFallback(t *testing.T) {
	store := newRehydrateStore()

	// A checkpoint as an older agent would have left it, carrying a commit.
	old := MetadataCache{Committed: map[string]map[string]int64{
		"g1": {"orders/0": 5},
	}}
	data, err := old.ToJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.Put(context.Background(), legacyCheckpointKey, bytes.NewReader(data)); err != nil {
		t.Fatalf("seed legacy checkpoint: %v", err)
	}

	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{}, WithAgentID("agent-a"))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer se.Close()

	if got := se.checkpointKey(); got != "_agents/agent-a/checkpoint.json" {
		t.Errorf("checkpoint key = %q, want it namespaced per agent", got)
	}
	if err := se.LoadCheckpoint(); err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}
	if got := se.FirstAllowedOffset("orders", 0); got != 5 {
		t.Errorf("committed offset restored from legacy checkpoint = %d, want 5", got)
	}

	// A subsequent save writes the namespaced object, not the legacy key.
	if err := se.SaveCheckpoint(); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	if _, err := store.Get(context.Background(), se.checkpointKey()); err != nil {
		t.Errorf("namespaced checkpoint was not written: %v", err)
	}
}

// TestDeleteTopic_RemovesPerPartitionManifests checks the cleanup path. The
// manifests live under _topics/, outside the topic's own data prefix, so the
// existing topic deletion had to learn about them or it would leave a deleted
// topic's positions behind to be recovered on the next start.
func TestDeleteTopic_RemovesPerPartitionManifests(t *testing.T) {
	store := NewMockStore()
	se, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{}, WithAgentID("test-agent"))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer se.Close()

	if err := se.CreateTopic("doomed", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := se.Append("doomed", 0, batchOfN(2), 2, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := se.SaveManifest(); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	if err := se.DeleteTopic("doomed"); err != nil {
		t.Fatalf("delete topic: %v", err)
	}

	// Deletion is detached from the request, so wait for it rather than
	// assuming it finished.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		store.Mu.Lock()
		_, exists := store.Data["_topics/doomed/_manifest/0"]
		store.Mu.Unlock()
		if !exists {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("per-partition manifest for a deleted topic was left behind")
}
