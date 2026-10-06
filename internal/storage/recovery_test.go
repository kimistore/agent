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
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

// appendedThrough reads a partition's manifest and returns the log end it
// records.
func manifestLogEnd(t *testing.T, store *casStore, topic string, partition int32) int64 {
	t.Helper()
	rc, err := store.Get(context.Background(), partitionManifestKey(topic, partition))
	if err != nil {
		t.Fatalf("no manifest for %s/%d: %v", topic, partition, err)
	}
	defer func() { _ = rc.Close() }()
	buf := make([]byte, 64*1024)
	n, _ := rc.Read(buf)
	var m perPartitionManifest
	if err := json.Unmarshal(buf[:n], &m); err != nil {
		t.Fatalf("manifest does not parse: %v", err)
	}
	return m.LogEndOffset
}

// A crashing owner leaves a manifest behind the object storage it wrote. If
// recovery trusts that manifest the next owner appends over records the previous
// owner had already acknowledged -- which D2 releases as soon as the segment
// lands, not when a checkpoint catches up. This is the bug the handover ordering
// exists to prevent, and it is silent: no error, just a log missing records.
func TestRecovery_CrashLeavesAManifestBehindTheObjects(t *testing.T) {
	store := newCASStore()

	// agent-a appends to 10 and checkpoints.
	crashed := newOwningEngine(t, store, "agent-a")
	if err := crashed.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := crashed.Append("orders", 0, batchOfN(10), 10, true); err != nil {
		t.Fatalf("append to 10: %v", err)
	}
	if err := crashed.SaveManifest(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// Then it appends to 20 and uploads that segment, and dies before the next
	// checkpoint. The manifest still says 10.
	if _, err := crashed.Append("orders", 0, batchOfN(10), 10, true); err != nil {
		t.Fatalf("append to 20: %v", err)
	}
	crashed.walMgr.FlushPartition("orders", 0)
	waitForClaimedSegment(t, store, "orders/0/"+walSegmentName(0, 1))

	if got := manifestLogEnd(t, store, "orders", 0); got != 10 {
		t.Fatalf("test setup: manifest log end = %d, want the stale 10", got)
	}

	// agent-a's claim expires; agent-b takes the partition over.
	store.expireClaim(t, "orders", 0)
	next := newOwningEngine(t, store, "agent-b")
	defer next.Close()

	if !next.Owns("orders", 0) {
		t.Fatal("agent-b should have taken the expired claim")
	}
	if got := next.HighWaterMark("orders", 0); got != 20 {
		t.Fatalf("recovered log end = %d, want 20: at %d the next append would overwrite records %d-20",
			got, got, got)
	}

	// And the records really are still readable at the offsets that were acked.
	if _, _, err := next.ReadBatch("orders", 0, 15, 0); err != nil {
		t.Errorf("records 15-20 are unreadable after takeover: %v", err)
	}
}

// The safe direction matters too: a manifest that is *ahead* of object storage
// describes data that was never durable. Nothing is lost by trusting it -- the
// records are simply not there yet -- and recovery must not walk the log end
// backwards to match what it can see.
func TestRecovery_DoesNotRewindAFlushlessManifest(t *testing.T) {
	store := newCASStore()

	crashed := newOwningEngine(t, store, "agent-a")
	if err := crashed.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	// Appended and checkpointed, but never flushed: acks=1, so the producer was
	// told the offset with only a local fsync behind it.
	if _, err := crashed.Append("orders", 0, batchOfN(10), 10, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := crashed.SaveManifest(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	store.expireClaim(t, "orders", 0)
	next := newOwningEngine(t, store, "agent-b")
	defer next.Close()

	if got := next.HighWaterMark("orders", 0); got != 10 {
		t.Errorf("recovered log end = %d, want 10: rewinding would reissue offsets to an acks=1 producer", got)
	}
}

// A graceful handover is the same sequence with a final checkpoint, so the next
// owner finds a manifest that already agrees with object storage and can take
// the partition with nothing to reconcile.
func TestRecovery_GracefulHandoverLeavesAConsistentManifest(t *testing.T) {
	store := newCASStore()

	first := newOwningEngine(t, store, "agent-a")
	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := first.Append("orders", 0, batchOfN(20), 20, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	// A graceful close: seal, upload, checkpoint, then release the claim.
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got := manifestLogEnd(t, store, "orders", 0); got != 20 {
		t.Errorf("manifest after a graceful close = %d, want the flushed 20", got)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("second close must be a no-op: %v", err)
	}

	next := newOwningEngine(t, store, "agent-b")
	defer next.Close()
	if got := next.HighWaterMark("orders", 0); got != 20 {
		t.Errorf("recovered log end after handover = %d, want 20", got)
	}
}

// A partition with several segments has to reconcile against the newest one, and
// a superseded owner must not define where the log ends.
func TestRecovery_ReconcilesAgainstTheNewestLiveSegment(t *testing.T) {
	store := newCASStore()

	crashed := newOwningEngine(t, store, "agent-a")
	if err := crashed.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := crashed.Append("orders", 0, batchOfN(10), 10, true); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		crashed.walMgr.FlushPartition("orders", 0)
	}
	// A stale checkpoint that predates the last two segments.
	if err := crashed.SaveManifest(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	store.expireClaim(t, "orders", 0)
	next := newOwningEngine(t, store, "agent-b")
	defer next.Close()

	if got := next.HighWaterMark("orders", 0); got != 30 {
		t.Errorf("recovered log end = %d, want 30 from the newest segment", got)
	}
}

// One LIST per topic is what keeps unconditional reconciliation affordable, so
// the batching itself is worth pinning: a topic's partitions must all be found,
// including partition 0, which shares its base offset with every other
// partition in the topic.
func TestRecovery_ReconciliationFindsEveryPartition(t *testing.T) {
	store := newCASStore()
	ctx := context.Background()

	owner := newOwningEngine(t, store, "agent-a")
	if err := owner.CreateTopic("orders", 4); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for pid := int32(0); pid < 4; pid++ {
		if _, err := owner.Append("orders", pid, batchOfN(5), 5, true); err != nil {
			t.Fatalf("append %d: %v", pid, err)
		}
		owner.walMgr.FlushPartition("orders", pid)
	}
	for pid := int32(0); pid < 4; pid++ {
		waitForClaimedSegment(t, store, "orders/"+itoa(pid)+"/"+walSegmentName(0, 1))
	}
	owner.Close()

	byPartition, err := owner.objectSegmentsByPartition(ctx, "orders")
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if len(byPartition) != 4 {
		t.Fatalf("found %d partition(s), want 4: %v", len(byPartition), byPartition)
	}
	for pid := int32(0); pid < 4; pid++ {
		segs := byPartition[pid]
		if len(segs) != 1 {
			t.Errorf("partition %d has %d segment(s), want 1", pid, len(segs))
			continue
		}
		if !strings.Contains(segs[0].S3Key, "orders/"+itoa(pid)+"/") {
			t.Errorf("partition %d mapped to %q, which belongs to another partition", pid, segs[0].S3Key)
		}
	}
}

func itoa(v int32) string { return strconv.Itoa(int(v)) }

// A manifest is only a snapshot, and several things can write one that is behind
// reality: a crashed owner, a handover that raced a stolen claim, an operator
// restoring an old backup. None of them may rewind the log, so recovery takes the
// higher of what the manifest claims and what the objects hold. This is the
// belt to the epoch check's braces -- the epoch check only catches a manifest from
// a *newer* owner.
func TestRecovery_StaleEpochManifestCannotRewindTheLog(t *testing.T) {
	store := newCASStore()

	owner := newOwningEngine(t, store, "agent-a")
	if err := owner.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := owner.Append("orders", 0, batchOfN(30), 30, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	owner.walMgr.FlushPartition("orders", 0)
	waitForClaimedSegment(t, store, "orders/0/"+walSegmentName(0, 1))

	// A manifest that claims the log ends at 10, stamped with an epoch below the
	// one that wrote 30.
	clobber(t, store, partitionManifestKey("orders", 0), perPartitionManifest{
		Epoch: 1, Writer: "agent-a", Topic: "orders", Partition: 0,
		LogEndOffset: 10, LogStartOffset: 0,
	})

	store.expireClaim(t, "orders", 0)
	next := newOwningEngine(t, store, "agent-b")
	defer next.Close()

	if got := next.HighWaterMark("orders", 0); got != 30 {
		t.Fatalf("recovered log end = %d, want 30: a stale manifest rewound the log", got)
	}
	// And the records past the stale manifest's claim are still readable, which is
	// what would have been lost.
	if _, _, err := next.ReadBatch("orders", 0, 25, 0); err != nil {
		t.Errorf("records past the stale manifest's log end are unreadable: %v", err)
	}
}

// clobber writes an object directly, standing in for anything that leaves a
// manifest behind that the writer of it did not mean to leave.
func clobber(t *testing.T, store *casStore, key string, v any) {
	t.Helper()
	body, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshal %s: %v", key, err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.data[key] = body
	store.version[key]++
}

// ListOffsets("earliest") must report the oldest segment that is actually present,
// not the newest. A log start that is too high is invisible: the data is still
// there, the broker reports no error, and every consumer that resets to the start
// of the log -- which is what a restarted Mimir does -- skips it silently.
func TestRecovery_LogStartComesFromTheOldestSegmentNotTheNewest(t *testing.T) {
	store := newCASStore()

	owner := newOwningEngine(t, store, "agent-a")
	if err := owner.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	// Three segments, so the oldest and the newest are different offsets.
	for i := 0; i < 3; i++ {
		if _, err := owner.Append("orders", 0, batchOfN(10), 10, true); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		owner.walMgr.FlushPartition("orders", 0)
	}
	if err := owner.SaveManifest(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	owner.Close()

	if got := manifestLogEnd(t, store, "orders", 0); got != 30 {
		t.Fatalf("setup: manifest log end = %d, want 30", got)
	}

	// Corrupt only the log start: the log end stays right, which is exactly the
	// case where the old code had no reason to look at it.
	stored := readManifest(t, store, "orders", 0)
	stored.LogStartOffset = 20
	clobber(t, store, partitionManifestKey("orders", 0), stored)

	// Reconciliation only covers a partition this agent holds.
	store.expireClaim(t, "orders", 0)
	next := newOwningEngine(t, store, "agent-b")
	defer next.Close()

	if got := next.HighWaterMark("orders", 0); got != 30 {
		t.Errorf("recovered log end = %d, want 30", got)
	}
	if got := next.LogStartOffset("orders", 0); got != 0 {
		t.Errorf("recovered log start = %d, want 0: segments from offset 0 are present and readable", got)
	}
}

// The same reasoning without any corruption: reconciliation must not *introduce* a
// bad log start. Deriving it from the newest segment -- which is what the segment
// walk in endOffsetsFromObject naturally produces -- points consumers past every
// older segment the moment recovery runs at all.
func TestRecovery_ReconciliationKeepsTheLogStartAtTheOldestSegment(t *testing.T) {
	store := newCASStore()

	crashed := newOwningEngine(t, store, "agent-a")
	if err := crashed.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := crashed.Append("orders", 0, batchOfN(10), 10, true); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		crashed.walMgr.FlushPartition("orders", 0)
	}
	// The upload is asynchronous: sealing a segment does not mean it is in the
	// store yet. Without this the last segment is still in flight and recovery
	// legitimately finds only the earlier ones.
	waitForClaimedSegment(t, store, "orders/0/"+walSegmentName(20, 1))

	// No checkpoint, so recovery has to reconcile three segments' worth from the
	// objects alone.
	store.expireClaim(t, "orders", 0)

	next := newOwningEngine(t, store, "agent-b")
	defer next.Close()

	if got := next.HighWaterMark("orders", 0); got != 30 {
		t.Errorf("recovered log end = %d, want 30", got)
	}
	if got := next.LogStartOffset("orders", 0); got != 0 {
		t.Errorf("recovered log start = %d, want 0: the oldest segment is at offset 0", got)
	}
	// And the records at the start of the log are actually reachable, which is the
	// property the offset is supposed to describe.
	if _, _, err := next.ReadBatch("orders", 0, 0, 0); err != nil {
		t.Errorf("the first record is not readable: %v", err)
	}
}

// readManifest returns a partition's manifest as stored.
func readManifest(t *testing.T, store *casStore, topic string, partition int32) perPartitionManifest {
	t.Helper()
	rc, err := store.Get(context.Background(), partitionManifestKey(topic, partition))
	if err != nil {
		t.Fatalf("no manifest for %s/%d: %v", topic, partition, err)
	}
	defer func() { _ = rc.Close() }()
	buf := make([]byte, 256*1024)
	n, _ := rc.Read(buf)
	var m perPartitionManifest
	if err := json.Unmarshal(buf[:n], &m); err != nil {
		t.Fatalf("manifest does not parse: %v", err)
	}
	return m
}
