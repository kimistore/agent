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
	"testing"
	"time"
)

// TestDrainOwned_ReleasesEveryPartitionItHolds is the shutdown path this exists
// for. A SIGTERM handler calls it, and the whole point is that every claim this
// agent holds leaves with a manifest written behind it.
func TestDrainOwned_ReleasesEveryPartitionItHolds(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")

	if err := engine.CreateTopic("orders", 3); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for p := 0; p < 3; p++ {
		if _, err := engine.Append("orders", int32(p), batchOfN(10), 10, true); err != nil {
			t.Fatalf("append %d: %v", p, err)
		}
	}

	reports, err := engine.DrainOwned(context.Background(), DrainShutdownBudget)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(reports) != 3 {
		t.Fatalf("got %d reports, want 3", len(reports))
	}

	for _, r := range reports {
		if !r.Released {
			t.Errorf("%s/%d: not released", r.Topic, r.Partition)
		}
		if !r.Durable {
			t.Errorf("%s/%d: tail not durable", r.Topic, r.Partition)
		}
	}

	// Every claim gone, not just the ones that happened to be first.
	for p := 0; p < 3; p++ {
		if engine.Owns("orders", int32(p)) {
			t.Errorf("still owns orders/%d after the drain", p)
		}
	}

	// And the successor can read every partition from what was left behind,
	// which is the property the manifest exists to provide.
	successor := newOwningEngine(t, store, "agent-b")
	for p := 0; p < 3; p++ {
		if !successor.Owns("orders", int32(p)) {
			t.Errorf("successor did not claim orders/%d", p)
		}
		if got := successor.HighWaterMark("orders", int32(p)); got != 10 {
			t.Errorf("successor log end for orders/%d = %d, want 10", p, got)
		}
	}
}

// TestDrainOwned_KeepsGoingPastAFailure is the reason DrainOwned exists rather
// than calling DrainPartition in a loop.
//
// DrainPartitions stops at the first error, which is the right behaviour while
// the agent keeps running and the caller may retry. During shutdown the process
// is leaving either way, so stopping early strands every remaining claim and
// costs the cluster a full TTL for each one.
func TestDrainOwned_KeepsGoingPastAFailure(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")

	if err := engine.CreateTopic("orders", 3); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for p := 0; p < 3; p++ {
		if _, err := engine.Append("orders", int32(p), batchOfN(10), 10, true); err != nil {
			t.Fatalf("append %d: %v", p, err)
		}
	}

	// Steal one partition out from under the agent. The drain must notice, via
	// verifyClaim, and must still drain the other two.
	stealPartitionClaim(t, store, "orders", 1, "agent-thief", 42)

	reports, err := engine.DrainOwned(context.Background(), DrainShutdownBudget)
	if err == nil {
		t.Error("drain reported success, want an error naming the partition it could not hand over")
	}

	byPartition := map[int32]HandoverReport{}
	for _, r := range reports {
		byPartition[r.Partition] = r
	}
	if len(byPartition) != 3 {
		t.Fatalf("got reports for %d partitions, want all 3 attempted", len(byPartition))
	}

	if !byPartition[0].Released || !byPartition[2].Released {
		t.Errorf("the partitions that could be handed over were not: %v", reports)
	}
	if byPartition[1].Released {
		t.Error("orders/1 was released even though another agent holds its claim")
	}
}

// TestDrainOwned_NoPartitionsIsNotAnError keeps a single-partition agent, or an
// agent that owns nothing, from logging a spurious failure at shutdown.
func TestDrainOwned_NoPartitionsIsNotAnError(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")

	reports, err := engine.DrainOwned(context.Background(), DrainShutdownBudget)
	if err != nil {
		t.Fatalf("drain with nothing owned: %v", err)
	}
	if len(reports) != 0 {
		t.Errorf("got %d reports, want none", len(reports))
	}
}

// TestOwnedPartitionRefs_ParsesKeys guards the parse that turns OwnedPartitions
// keys into refs. A topic containing a slash would parse into the wrong
// partition, so the split takes the last segment rather than the first.
func TestOwnedPartitionRefs_ParsesKeys(t *testing.T) {
	cases := []struct {
		key         string
		wantTopic   string
		wantPart    int32
		wantFailure bool
	}{
		{key: "orders/0", wantTopic: "orders", wantPart: 0},
		{key: "mimir-distributor/3", wantTopic: "mimir-distributor", wantPart: 3},
		{key: "team/orders/7", wantTopic: "team/orders", wantPart: 7},
		{key: "nodeless", wantFailure: true},
		{key: "orders/notanumber", wantFailure: true},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			topic, part, err := splitOwnedPartitionKey(tc.key)
			if tc.wantFailure {
				if err == nil {
					t.Fatalf("splitOwnedPartitionKey(%q) succeeded, want an error", tc.key)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitOwnedPartitionKey(%q): %v", tc.key, err)
			}
			if topic != tc.wantTopic || part != tc.wantPart {
				t.Errorf("got %q/%d, want %q/%d", topic, part, tc.wantTopic, tc.wantPart)
			}
		})
	}
}

// TestDrainOwned_RespectsItsDeadlineAcrossManyPartitions guards the arithmetic
// that a single-partition test cannot reach.
//
// Dividing a budget across N partitions does not bound the total. Once the share
// falls below the one-second floor, the run takes N seconds however small the
// budget, and a SIGTERM handler that overruns its grace period is killed
// mid-drain, losing the seal it was attempting. The context deadline is what
// stops that, so this asserts the whole run finishes inside the budget.
func TestDrainOwned_RespectsItsDeadlineAcrossManyPartitions(t *testing.T) {
	if testing.Short() {
		t.Skip("spins up an engine with many partitions")
	}

	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")

	const partitions = 30
	if err := engine.CreateTopic("orders", partitions); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for p := 0; p < partitions; p++ {
		if _, err := engine.Append("orders", int32(p), batchOfN(2), 2, true); err != nil {
			t.Fatalf("append %d: %v", p, err)
		}
	}

	const budget = 2 * time.Second
	start := time.Now()
	_, _ = engine.DrainOwned(context.Background(), budget)
	elapsed := time.Since(start)

	// Generous headroom, because the assertion is about a runaway loop rather
	// than about precise timing. The failure this replaces would have taken 30
	// seconds, one per partition.
	if elapsed > budget+3*time.Second {
		t.Errorf("drain of %d partitions took %s, budget was %s: the deadline is not bounding the run",
			partitions, elapsed.Truncate(time.Millisecond), budget)
	}
}

// TestDrainShutdownBudgetFitsInsideAGracePeriod documents the relationship
// between the shutdown budget and a per-partition drain.
//
// DrainShutdownBudget is a total for the whole run, so it is deliberately
// smaller than DefaultHandoverBudget, which is the budget for a single
// partition when a caller names none. The shutdown budget must also be positive,
// because a non-positive value makes DrainPartition substitute its own 30-second
// default and ignore the deadline.
func TestDrainShutdownBudgetFitsInsideAGracePeriod(t *testing.T) {
	if DrainShutdownBudget <= 0 {
		t.Fatalf("DrainShutdownBudget = %s, want a positive total", DrainShutdownBudget)
	}
	if DrainShutdownBudget > time.Minute {
		t.Errorf("DrainShutdownBudget = %s is long enough to be killed by a supervisor "+
			"before it finishes; a SIGTERM handler should finish well inside a grace period",
			DrainShutdownBudget)
	}
}

// stealPartitionClaim replaces another agent's claim, the way a peer taking over
// after a TTL expiry would. It writes the record directly rather than going
// through a second engine, because the point is that this engine never observes
// the takeover: its local view still says it owns the partition.
func stealPartitionClaim(t *testing.T, store *casStore, topic string, partition int32, agent string, epoch int64) {
	t.Helper()

	key := partitionOwnerKey(topic, partition)
	record := PartitionOwner{
		Agent:   agent,
		Epoch:   epoch,
		Expires: time.Now().Add(time.Hour).Unix(),
	}
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal claim: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	store.data[key] = body
	store.version[key]++
}
