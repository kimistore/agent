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
	"errors"
	"strings"
	"testing"
	"time"
)

// TestTwoAgentsDistinctIDs_SplitThePartitions is the supported deployment, and
// this is the contract it relies on: one agent owns the partitions, the other
// serves nothing and refuses writes to them.
func TestTwoAgentsDistinctIDs_SplitThePartitions(t *testing.T) {
	store := newCASStore()

	first := newOwningEngine(t, store, "agent-a")
	if err := first.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := first.Append("orders", 0, batchOfN(10), 10, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	second := newOwningEngine(t, store, "agent-b")

	for p := 0; p < 2; p++ {
		if !first.Owns("orders", int32(p)) {
			t.Errorf("agent-a lost orders/%d to a second agent that never held it", p)
		}
		if second.Owns("orders", int32(p)) {
			t.Errorf("agent-b took orders/%d, which a live agent already held", p)
		}
	}

	// The loser refuses writes, and says which agent holds it, so an operator
	// reading the log does not have to guess.
	_, err := second.Append("orders", 0, batchOfN(1), 1, false)
	if !errors.Is(err, ErrPartitionHeld) {
		t.Errorf("agent-b append = %v, want an ownership refusal naming the holder", err)
	}
	if err != nil && !strings.Contains(err.Error(), "agent-a") {
		t.Errorf("refusal %q does not name the agent that holds the partition", err)
	}

	// The owner is unaffected.
	if _, err := first.Append("orders", 0, batchOfN(1), 1, false); err != nil {
		t.Errorf("agent-a append to its own partition: %v", err)
	}
	if got := first.HighWaterMark("orders", 0); got != 11 {
		t.Errorf("agent-a log end = %d, want 11", got)
	}
}

// TestTwoAgentsSharingAnID_RefuseRatherThanFight covers the misconfiguration
// this fence exists for.
//
// Two agents with the same agent id cannot tell each other apart from the claim
// record, because the record names the id both of them use. Before the check,
// each renewal bumped the epoch past the other: the epoch climbed without bound,
// both agents kept believing they owned the partition, and both acknowledged
// writes at diverged log end offsets. That is not a degraded read path, it is
// two writers on one log.
func TestTwoAgentsSharingAnID_RefuseRatherThanFight(t *testing.T) {
	store := newCASStore()

	first := newOwningEngine(t, store, "shared-id")
	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := first.Append("orders", 0, batchOfN(10), 10, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	epochBefore := first.partitionEpoch("orders", 0)

	second := newOwningEngine(t, store, "shared-id")
	if second.Owns("orders", 0) {
		t.Fatal("the second agent took a partition that a live agent holds; " +
			"a shared agent id must be refused rather than honoured")
	}

	// Renewals must not start a fight. The incumbent keeps the partition and its
	// epoch advances only on its own renewals.
	ctx := context.Background()
	for round := 0; round < 3; round++ {
		first.ownership.renewAll(ctx)
		second.ownership.renewAll(ctx)

		if !first.Owns("orders", 0) {
			t.Fatalf("round %d: the incumbent lost its own partition", round)
		}
		if second.Owns("orders", 0) {
			t.Fatalf("round %d: the second agent gained the partition", round)
		}
	}

	epochAfter := first.partitionEpoch("orders", 0)
	if epochAfter < epochBefore {
		t.Errorf("epoch went backwards: %d then %d", epochBefore, epochAfter)
	}
	// Three renewals by the incumbent plus the one at startup. The counter is
	// allowed to move, but it must not be doubling, which is the ping-pong.
	if delta := epochAfter - epochBefore; delta > 4 {
		t.Errorf("epoch advanced by %d across three renewals, which is the ping-pong the check prevents", delta)
	}

	// Only one writer is ever acknowledged.
	if _, err := first.Append("orders", 0, batchOfN(1), 1, false); err != nil {
		t.Errorf("incumbent append: %v", err)
	}
	if _, err := second.Append("orders", 0, batchOfN(1), 1, false); err == nil {
		t.Error("the second agent wrote to a partition it does not own")
	}
}

// TestVerifyClaim_RejectsASupersededEpoch pins the second half of the fence.
//
// verifyClaim reads the claim record, so it can see an epoch above the one this
// agent holds even though the record names the same agent. It is what the
// manifest save and the handover rely on, and it is the check that stops a
// shared id from writing durable state on the partition's behalf.
func TestVerifyClaim_RejectsASupersededEpoch(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "shared-id")

	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(4), 4, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := engine.ownership.verifyClaim(context.Background(), "orders", 0); err != nil {
		t.Fatalf("verifyClaim on a held claim: %v", err)
	}

	// Someone else moved the claim forward under our own id.
	stealPartitionClaim(t, store, "orders", 0, "shared-id", engine.partitionEpoch("orders", 0)+5)

	err := engine.ownership.verifyClaim(context.Background(), "orders", 0)
	if !errors.Is(err, ErrPartitionLost) {
		t.Fatalf("verifyClaim after a same-id takeover = %v, want ErrPartitionLost", err)
	}
	if !strings.Contains(err.Error(), "KIMISTORE_AGENT_ID") {
		t.Errorf("the refusal does not say what to fix: %v", err)
	}
}

// TestClaim_ExpiredEpochAboveOursIsStillTakeoverable guards the other side of
// the rule. An expired claim at a higher epoch is the normal failover case: the
// previous holder died, so taking it over is exactly what should happen.
func TestClaim_ExpiredEpochAboveOursIsStillTakeoverable(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")

	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(4), 4, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Seal and upload, so the partition has durable state in object storage for
	// the successor to recover. Without this the successor legitimately owns an
	// empty partition, and the assertion below would be testing nothing.
	engine.walMgr.FlushPartition("orders", 0)
	waitForClaimedSegment(t, store, "orders/0/"+walSegmentName(0, engine.partitionEpoch("orders", 0)))
	waitForPendingUploads(t, engine, 0)

	// The incumbent dies: its claim sits at a high epoch and expires.
	stealPartitionClaim(t, store, "orders", 0, "agent-a", engine.partitionEpoch("orders", 0)+9)
	expireAllClaims(t, store)

	successor := newOwningEngine(t, store, "agent-b")
	if !successor.Owns("orders", 0) {
		t.Fatal("a successor must take over a partition whose claim has expired, " +
			"even when the epoch it inherits is higher than its own")
	}
	if got := successor.HighWaterMark("orders", 0); got != 4 {
		t.Errorf("successor log end = %d, want 4 recovered from object storage; "+
			"a successor that took over an expired claim must be able to serve it", got)
	}
}

// expireAllClaims backdates every ownership record, standing in for an agent that
// died and let its claim lapse.
func expireAllClaims(t *testing.T, store *casStore) {
	t.Helper()

	store.mu.Lock()
	defer store.mu.Unlock()

	for key, raw := range store.data {
		if !strings.HasPrefix(key, "_owners/") {
			continue
		}
		record := PartitionOwner{}
		if json.Unmarshal(raw, &record) != nil {
			continue
		}
		record.Expires = time.Now().Add(-time.Hour).Unix()
		body, err := json.Marshal(record)
		if err != nil {
			t.Fatalf("marshal %s: %v", key, err)
		}
		store.data[key] = body
		store.version[key]++
	}
}
