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
	"testing"
)

// TestManifestSave_RefusesAStaleWriter reproduces the failure the store-verified
// check exists to prevent.
//
// A takeover this agent never observed: the claim now names another agent and
// the epoch has moved on, while the local view still says this agent owns the
// partition and is 10 records ahead. Before the check, the manifest PUT was
// unconditional, so this agent overwrote the new owner's record with its own
// lower epoch and log end, and the new owner recovered from a position that
// belonged to the writer it had superseded.
func TestManifestSave_RefusesAStaleWriter(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")

	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(10), 10, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := engine.SaveManifestContext(context.Background()); err != nil {
		t.Fatalf("first manifest save: %v", err)
	}

	before := readManifest(t, store, "orders", 0)
	if before.Epoch == 0 || before.LogEndOffset != 10 {
		t.Fatalf("baseline manifest = epoch %d, log end %d; want a real record at 10",
			before.Epoch, before.LogEndOffset)
	}

	// Another agent takes the partition. The local view keeps saying we own it,
	// which is exactly the state the renewal loop has not yet corrected.
	stealPartitionClaim(t, store, "orders", 0, "agent-b", before.Epoch+1)

	// Write the new owner's manifest, as a real successor would.
	successorManifest := perPartitionManifest{
		Epoch:        before.Epoch + 1,
		Writer:       "agent-b",
		Topic:        "orders",
		Partition:    0,
		LogEndOffset: 4,
		Segments:     []*SegmentMetadata{},
	}
	writeManifestDirect(t, store, "orders", 0, successorManifest)

	// Mark the partition dirty with a newer log end, which is the state that
	// would overwrite the new owner's record if the save were not fenced.
	engine.markManifestDirty("orders", 0)

	if err := engine.SaveManifestContext(context.Background()); err != nil {
		t.Fatalf("manifest save: %v", err)
	}

	after := readManifest(t, store, "orders", 0)
	if after.Epoch != successorManifest.Epoch {
		t.Errorf("stale writer replaced the manifest: epoch is now %d, the owner wrote %d",
			after.Epoch, successorManifest.Epoch)
	}
	if after.LogEndOffset != successorManifest.LogEndOffset {
		t.Errorf("stale writer rewrote the log end: %d, the owner wrote %d",
			after.LogEndOffset, successorManifest.LogEndOffset)
	}
	if after.Writer != "agent-b" {
		t.Errorf("manifest writer = %q, want agent-b", after.Writer)
	}
}

// TestManifestSave_WritesWhenTheClaimStillHolds is the control for the test
// above. Without it, a check that refused every write would pass the test above.
func TestManifestSave_WritesWhenTheClaimStillHolds(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")

	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(10), 10, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := engine.SaveManifestContext(context.Background()); err != nil {
		t.Fatalf("manifest save: %v", err)
	}

	// Five more records, so the log end moves from 10 to 15.
	if _, err := engine.Append("orders", 0, batchOfN(5), 5, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := engine.SaveManifestContext(context.Background()); err != nil {
		t.Fatalf("second manifest save: %v", err)
	}

	after := readManifest(t, store, "orders", 0)
	if after.LogEndOffset != 15 {
		t.Errorf("log end = %d, want 15; the claim is still held so the write must go through",
			after.LogEndOffset)
	}
	if after.Writer != "agent-a" {
		t.Errorf("writer = %q, want agent-a", after.Writer)
	}
}

func writeManifestDirect(t *testing.T, store ObjectStore, topic string, partition int32, m perPartitionManifest) {
	t.Helper()

	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := store.Put(context.Background(), partitionManifestKey(topic, partition), bytes.NewReader(body)); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
}

// TestManifestSave_DropsTheDirtyMarkOfARefusedPartition checks that a refused
// write is not retried forever.
//
// The partition is no longer ours to publish, so leaving it marked dirty would
// make every subsequent save re-read a claim it does not hold, for as long as
// this agent runs.
func TestManifestSave_DropsTheDirtyMarkOfARefusedPartition(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")

	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(4), 4, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := engine.SaveManifestContext(context.Background()); err != nil {
		t.Fatalf("first save: %v", err)
	}

	stealPartitionClaim(t, store, "orders", 0, "agent-b", 99)
	engine.markManifestDirty("orders", 0)

	if err := engine.SaveManifestContext(context.Background()); err != nil {
		t.Fatalf("save after takeover: %v", err)
	}

	engine.manifestMu.Lock()
	_, stillDirty := engine.manifestDirty["orders/0"]
	engine.manifestMu.Unlock()

	if stillDirty {
		t.Error("the refused partition is still marked dirty, so every save will retry it")
	}
}
