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
	"strings"
	"testing"
	"time"
)

// A handover must leave the partition in a state a peer can pick up cold: the
// log end recorded, the records readable, and nothing left behind that would let
// this agent serve the partition again.
func TestDrainPartition_HandsOverReadableDurableState(t *testing.T) {
	store := newCASStore()

	first := newOwningEngine(t, store, "agent-a")
	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := first.Append("orders", 0, batchOfN(40), 40, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	report, err := first.DrainPartition(context.Background(), "orders", 0, DefaultHandoverBudget)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !report.Released {
		t.Error("report says the partition was not released")
	}
	if !report.Durable {
		t.Error("report says the tail was not durable")
	}
	if report.Tail != 40 {
		t.Errorf("report tail = %d, want 40", report.Tail)
	}
	if report.Elapsed <= 0 {
		t.Error("report has no elapsed time, which is the number this exists to produce")
	}

	// The claim is gone and the agent stops serving it immediately, not at its
	// next renewal tick.
	if first.Owns("orders", 0) {
		t.Error("engine still reports ownership after a handover")
	}
	// Writes stop. Reads deliberately do not: a non-owner reads from object
	// storage, which is authoritative, and refusing them would break consumers
	// during a handover for no safety gain. What must not happen is the old owner
	// serving its own pre-handover WAL, and that is covered by the read-side fence
	// tests in the ownership suite.
	if _, err := first.Append("orders", 0, batchOfN(1), 1, false); !refusedForOwnership(err) {
		t.Errorf("append after handover = %v, want an ownership refusal", err)
	}
	if _, _, err := first.ReadBatch("orders", 0, 0, 0); err != nil {
		t.Errorf("a consumer should still be able to read a handed-over partition: %v", err)
	}

	// The next owner finds the position already recorded, and the data intact.
	if got := manifestLogEnd(t, store, "orders", 0); got != 40 {
		t.Errorf("manifest after handover = %d, want 40", got)
	}
	second := newOwningEngine(t, store, "agent-b")
	defer second.Close()
	if got := second.HighWaterMark("orders", 0); got != 40 {
		t.Errorf("next owner recovered log end = %d, want 40", got)
	}
	if _, _, err := second.ReadBatch("orders", 0, 30, 0); err != nil {
		t.Errorf("records unreadable after handover: %v", err)
	}
}

// The whole point of the ordering is that the release comes last, so this pins the
// step that makes it safe: a handover waits for the upload rather than releasing
// and flushing afterwards, which would let a peer take the partition mid-write.
func TestDrainPartition_WaitsForTheUploadBeforeReleasing(t *testing.T) {
	store := newCASStore()

	engine := newOwningEngine(t, store, "agent-a")
	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(30), 30, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	// Deliberately not flushed: the whole handover has to do this.

	report, err := engine.DrainPartition(context.Background(), "orders", 0, DefaultHandoverBudget)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !report.Durable {
		t.Fatal("handover reported a non-durable tail")
	}

	// Durable means durable: the segment is in the object store before the claim
	// was let go.
	waitForClaimedSegment(t, store, "orders/0/"+walSegmentName(0, 1))
	if got := manifestLogEnd(t, store, "orders", 0); got != 30 {
		t.Errorf("manifest = %d, want 30", got)
	}
}

// Releasing a partition whose tail is not durable would make the next owner
// rewind past records an acks=all producer was told were written. When the budget
// runs out the claim stays put, and the caller is told why.
func TestDrainPartition_KeepsTheClaimWhenTheTailCannotBeMadeDurable(t *testing.T) {
	store := newCASStore()

	engine := newOwningEngine(t, store, "agent-a")
	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(10), 10, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Break the uploader so the tail can never be confirmed.
	store.failPut = errors.New("object store is unavailable")

	report, err := engine.DrainPartition(context.Background(), "orders", 0, 250*time.Millisecond)
	if err == nil {
		t.Fatal("drain reported success with an unsatisfiable durability requirement")
	}
	if report.Released {
		t.Error("report says the partition was released; it must not have been")
	}
	if !strings.Contains(err.Error(), "orders/0") {
		t.Errorf("error does not name the partition: %v", err)
	}

	// Still ours, still writable: a slow object store degrades to slow recovery
	// rather than to lost records.
	if !engine.Owns("orders", 0) {
		t.Error("engine gave up a partition it could not hand over safely")
	}
	if _, err := engine.Append("orders", 0, batchOfN(1), 1, false); err != nil {
		t.Errorf("append after an aborted handover = %v, want the partition to still be writable", err)
	}
}

// Draining a partition this agent does not hold is a caller error, and must not be
// mistaken for a handover of something else.
func TestDrainPartition_RefusesPartitionsItDoesNotOwn(t *testing.T) {
	store := newCASStore()

	engine := newOwningEngine(t, store, "agent-a")
	if err := engine.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Partition 1's claim ages out and agent-b takes it, leaving agent-a holding
	// a partition it has already lost.
	store.expireClaim(t, "orders", 1)
	other := newOwningEngine(t, store, "agent-b")
	defer other.Close()
	if !other.Owns("orders", 1) {
		t.Fatal("agent-b did not take partition 1; the test proves nothing")
	}

	report, err := engine.DrainPartition(context.Background(), "orders", 1, DefaultHandoverBudget)
	if !errors.Is(err, ErrPartitionNotOwned) {
		t.Errorf("drain of a partition owned elsewhere = %v, want %v", err, ErrPartitionNotOwned)
	}
	if report.Released {
		t.Error("report claims a partition this agent no longer held was released")
	}
	// And agent-b still holds it: a handover must never release a claim on
	// somebody else's behalf, which would be a way to steal a partition.
	if !other.Owns("orders", 1) {
		t.Error("a failed handover on agent-a disturbed agent-b's claim")
	}
}

// Without ownership there is no epoch to release against and no fence to prove the
// partition is still ours, so the handover refuses rather than doing something
// unsafe that looks successful.
func TestDrainPartition_RequiresOwnership(t *testing.T) {
	store := newCASStore()

	engine := newOwningEngine(t, store, "agent-a")
	engine.ownership = nil

	_, err := engine.DrainPartition(context.Background(), "orders", 0, DefaultHandoverBudget)
	if err == nil {
		t.Fatal("drain without ownership succeeded")
	}
	if !strings.Contains(err.Error(), "KIMISTORE_PARTITION_OWNERSHIP") {
		t.Errorf("error does not name the missing setting: %v", err)
	}
}

// A batch handover stops at the first partition it cannot release, and keeps what
// it already did. Rolling a released partition back would mean re-claiming one a
// peer may already have taken.
func TestDrainPartitions_StopsAtTheFirstUnsafeHandover(t *testing.T) {
	store := newCASStore()

	engine := newOwningEngine(t, store, "agent-a")
	if err := engine.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for pid := int32(0); pid < 2; pid++ {
		if _, err := engine.Append("orders", pid, batchOfN(10), 10, true); err != nil {
			t.Fatalf("append %d: %v", pid, err)
		}
	}
	if _, err := engine.DrainPartition(context.Background(), "orders", 0, DefaultHandoverBudget); err != nil {
		t.Fatalf("handover of partition 0: %v", err)
	}

	// Now the uploader breaks, so partition 1's tail can never be confirmed.
	store.failPut = errors.New("object store is unavailable")

	reports, err := engine.DrainPartitions(context.Background(),
		[]PartitionRef{{"orders", 1}}, 250*time.Millisecond)
	if err == nil {
		t.Fatal("batch drain reported success despite a partition that could not be released")
	}
	if len(reports) != 1 {
		t.Fatalf("got %d report(s), want one per attempted partition", len(reports))
	}
	if reports[0].Released {
		t.Error("partition 1 was released even though its tail was not durable")
	}
	if !engine.Owns("orders", 1) {
		t.Error("a handover that could not make the tail durable gave the partition up anyway")
	}

	// The partition already handed over stays handed over: re-claiming it would
	// mean racing a peer that may have taken it in the meantime.
	if engine.Owns("orders", 0) {
		t.Error("partition 0 came back after the batch failed")
	}
}

// refusedForOwnership reports whether an error is the write path declining because
// this agent does not hold the partition, in any of the three ways that can
// happen. A released claim reads as "lost", which is the interesting one here.
func refusedForOwnership(err error) bool {
	return errors.Is(err, ErrPartitionNotOwned) ||
		errors.Is(err, ErrPartitionLost) ||
		errors.Is(err, ErrPartitionHeld)
}

// The read-side twin of the write-side claim, and the one thing the stale-read
// path must never get wrong. A partition's local WAL can hold records written
// before the partition moved away; serving those to a consumer would hand it data
// the current owner has since replaced. The same records are also the ones an
// engine would otherwise report as its log end.
//
// This is at the engine level rather than the protocol level because the hazard is
// a property of ReadBatch, and it needs a genuine loss of ownership -- a claim that
// has aged out and been taken by a peer -- not merely a local view of one.
func TestNonOwnerReads_NeverServeTheLocalWAL(t *testing.T) {
	store := newCASStore()
	// Claim expiry is stored in whole seconds, so a one-second TTL rounds to
	// nothing and the two agents simply take the partition back from each other.
	// Three seconds keeps the renewal interval at its one-second floor while
	// leaving the claim long enough to lose.
	const shortTTL = 3 * time.Second

	first, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOwnership(OwnershipConfig{Enabled: true, Agent: "agent-a", TTL: shortTTL}))
	if err != nil {
		t.Fatalf("agent-a: %v", err)
	}
	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	// Written locally and deliberately not flushed: three records that exist only
	// in agent-a's WAL.
	for i := 0; i < 3; i++ {
		if _, err := first.Append("orders", 0, batchOfN(1), 1, true); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	// agent-a's claim ages out and agent-b takes the partition over.
	store.expireClaim(t, "orders", 0)
	second, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOwnership(OwnershipConfig{Enabled: true, Agent: "agent-b", TTL: shortTTL}))
	if err != nil {
		t.Fatalf("agent-b: %v", err)
	}
	defer second.Close()
	if !second.Owns("orders", 0) {
		t.Fatal("agent-b did not take the partition")
	}
	// agent-b writes one record and gets it into object storage, so there is a
	// legitimate answer for agent-a to fall back to.
	if _, err := second.Append("orders", 0, batchOfN(1), 1, true); err != nil {
		t.Fatalf("agent-b append: %v", err)
	}
	second.walMgr.FlushPartition("orders", 0)
	// The segment carries the takeover epoch, which is whatever the claim
	// advanced to -- not necessarily 1.
	waitForClaimedSegment(t, store, "orders/0/"+walSegmentName(0, store.claim(t, "orders", 0).Epoch))

	// Wait for agent-a to notice it lost the partition. It renews every TTL/3, so
	// this is bounded by a couple of seconds rather than instant.
	deadline := time.Now().Add(20 * time.Second)
	for first.Owns("orders", 0) && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if first.Owns("orders", 0) {
		t.Fatal("agent-a never noticed it had lost the partition")
	}

	// The log end agent-a reports must be the durable one, never the three
	// records that only its own WAL can justify.
	if got := first.HighWaterMark("orders", 0); got > 1 {
		t.Errorf("non-owner log end = %d, want at most 1: the extra records exist only in its local WAL", got)
	}
	if got, _, ok := first.NonOwnerPosition(context.Background(), "orders", 0); !ok || got > 1 {
		t.Errorf("NonOwnerPosition = (%d, ok=%v), want at most 1 and ok", got, ok)
	}

	// And the read itself must come from object storage. agent-a's three local
	// records are longer than agent-b's one, so a local read is detectable by
	// size as well as by content.
	local, _, err := first.walMgr.ReadBatch("orders", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("reading the local WAL: %v", err)
	}
	data, _, err := first.ReadBatchContext(context.Background(), "orders", 0, 0, 1<<20)
	if err != nil {
		t.Fatalf("ReadBatchContext: %v", err)
	}
	if len(data) == len(local) && len(local) > 0 && len(data) > 0 {
		t.Errorf("ReadBatchContext returned %d bytes, the same as the local WAL: it served records object storage never received", len(data))
	}
}

// A non-owner's answer must survive a stale manifest: the log end it reports is
// what a consumer will use as its high watermark, so a manifest left behind by a
// crash must not hide records that are demonstrably in object storage.
func TestNonOwnerReads_ReconcileAgainstObjectStorageNotTheManifest(t *testing.T) {
	store := newCASStore()

	owner := newOwningEngine(t, store, "agent-a")
	if err := owner.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := owner.Append("orders", 0, batchOfN(10), 10, true); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		owner.walMgr.FlushPartition("orders", 0)
	}
	waitForClaimedSegment(t, store, "orders/0/"+walSegmentName(20, 1))
	if err := owner.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Rewind the manifest to an offset that is behind the objects, which is what
	// a crash between an upload and the next checkpoint leaves behind.
	stored := readManifest(t, store, "orders", 0)
	stored.LogEndOffset = 10
	clobber(t, store, partitionManifestKey("orders", 0), stored)

	reader, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOwnership(OwnershipConfig{Enabled: true, Agent: "agent-b", TTL: time.Minute}))
	if err != nil {
		t.Fatalf("agent-b: %v", err)
	}
	defer reader.Close()

	leo, _, ok := reader.NonOwnerPosition(context.Background(), "orders", 0)
	if !ok {
		t.Fatal("NonOwnerPosition found nothing in object storage")
	}
	if leo != 30 {
		t.Errorf("non-owner log end = %d, want 30: a stale manifest must not hide records that are in object storage", leo)
	}
}
