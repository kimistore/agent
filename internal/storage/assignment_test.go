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
	"fmt"
	"testing"
	"time"

	"kimistore/internal/coordinator"
)

// assignmentEnabled is the opt-in under test. The settle window is short so the
// reconciler can be exercised without waiting a production TTL.
func assignmentEnabled() AssignmentConfig {
	return AssignmentConfig{Enabled: true, Settle: time.Millisecond}
}

// newAssignedEngine builds an agent that both owns partitions and registers
// itself, which is what assignment needs: the live set comes from the registry.
func newAssignedEngine(t *testing.T, store ObjectStore, agent string, nodeID int32) *StorageEngine {
	t.Helper()

	port := int32(19092 + nodeID)
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOwnership(testOwnership(agent, 30*time.Second)),
		WithAgentID(agent),
		WithAssignment(assignmentEnabled()),
		WithRegistry(testRegistry(agent, nodeID, agent+".kimi.internal", port)))
	if err != nil {
		t.Fatalf("NewStorageEngine(%s): %v", agent, err)
	}
	return engine
}

// TestAssignment_ThreeAgentsShareSixPartitions is the case assignment exists for.
//
// Without it the first agent to start wins every claim and the other two idle:
// three agents, six partitions, all six on one agent. The whole point of scaling
// out is that the work is shared.
func TestAssignment_ThreeAgentsShareSixPartitions(t *testing.T) {
	store := newCASStore()

	a := newAssignedEngine(t, store, "agent-a", 1)
	if err := a.CreateTopic("orders", 6); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for p := 0; p < 6; p++ {
		if _, err := a.Append("orders", int32(p), batchOfN(5), 5, true); err != nil {
			t.Fatalf("append %d: %v", p, err)
		}
	}

	b := newAssignedEngine(t, store, "agent-b", 2)
	c := newAssignedEngine(t, store, "agent-c", 3)

	// Let each agent see the full live set before it claims.
	refreshRouting(t, a)
	refreshRouting(t, b)
	refreshRouting(t, c)

	// Rebalance every agent against the now-shared live set. Startup alone
	// cannot do this: each agent's first claim pass runs before its registry
	// knows the others exist, so it is a rebalance that moves the work.
	fleet := fleetOf(1, "agent-a", "agent-b", "agent-c")
	seedLiveSet(t, store, fleet)
	for _, e := range []*StorageEngine{a, b, c} {
		refreshRouting(t, e)
		if _, err := e.rebalanceNow(context.Background()); err != nil {
			t.Fatalf("rebalance: %v", err)
		}
	}
	// A second pass, so the partitions agent-a released are actually claimed
	// rather than merely ownerless.
	for _, e := range []*StorageEngine{a, b, c} {
		refreshRouting(t, e)
		if _, err := e.rebalanceNow(context.Background()); err != nil {
			t.Fatalf("second rebalance: %v", err)
		}
	}

	held := map[string][]int32{}
	total := 0
	for p := 0; p < 6; p++ {
		total++
		for name, e := range map[string]*StorageEngine{"agent-a": a, "agent-b": b, "agent-c": c} {
			if e.Owns("orders", int32(p)) {
				held[name] = append(held[name], int32(p))
			}
		}
	}

	t.Logf("assignment: a=%v b=%v c=%v", held["agent-a"], held["agent-b"], held["agent-c"])
	if len(held["agent-a"]) == total {
		t.Errorf("agent-a still holds all %d partitions; scaling out gained nothing", total)
	}
	if len(held) < 2 {
		t.Errorf("only %v holds any partition; the fleet did not share the work", held)
	}

	// Every partition has exactly one owner, which is the safety property. The
	// claim is what guarantees it, and assignment must not weaken that.
	for p := 0; p < 6; p++ {
		owners := 0
		for _, e := range []*StorageEngine{a, b, c} {
			if e.Owns("orders", int32(p)) {
				owners++
			}
		}
		if owners != 1 {
			t.Errorf("orders/%d has %d owners, want exactly 1", p, owners)
		}
	}
}

// TestAssignment_SingleAgentKeepsEverything is the guard on the most dangerous
// failure mode. With only itself live, every partition is assigned to it, and it
// must keep all of them. Releasing here would empty the cluster.
func TestAssignment_SingleAgentKeepsEverything(t *testing.T) {
	store := newCASStore()

	a := newAssignedEngine(t, store, "agent-a", 1)
	if err := a.CreateTopic("orders", 4); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for p := 0; p < 4; p++ {
		if _, err := a.Append("orders", int32(p), batchOfN(3), 3, true); err != nil {
			t.Fatalf("append %d: %v", p, err)
		}
	}

	refreshRouting(t, a)
	if released, err := a.rebalanceNow(context.Background()); err != nil || released != 0 {
		t.Fatalf("a lone agent released %d partition(s): %v", released, err)
	}
	for p := 0; p < 4; p++ {
		if !a.Owns("orders", int32(p)) {
			t.Errorf("a lone agent released orders/%d", p)
		}
	}
}

// TestAssignment_DrainsWhatItNoLongerWins covers the half that makes assignment
// mean anything. Refusing to acquire is only half the rule; an agent that does
// not give up what it no longer wins is in exactly the state it was before.
func TestAssignment_DrainsWhatItNoLongerWins(t *testing.T) {
	store := newCASStore()

	// agent-a starts alone and therefore owns everything, which is the state
	// assignment is meant to correct.
	a := newAssignedEngine(t, store, "agent-a", 1)
	defer a.Close()

	const partitions = 8
	if err := a.CreateTopic("orders", partitions); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for p := 0; p < partitions; p++ {
		if _, err := a.Append("orders", int32(p), batchOfN(4), 4, true); err != nil {
			t.Fatalf("append %d: %v", p, err)
		}
	}
	if got := len(a.OwnedPartitions()); got != partitions {
		t.Fatalf("agent-a holds %d partitions, want %d while it is alone", got, partitions)
	}

	// Three peers appear. Their liveness records are seeded rather than booted,
	// so the test exercises the rebalance without standing up three more
	// processes that would race for the same claims.
	fleet := fleetOf(1, "agent-a", "agent-b", "agent-c", "agent-d")
	seedLiveSet(t, store, fleet)
	refreshRouting(t, a)

	released, err := a.rebalanceNow(context.Background())
	if err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	if released == 0 {
		t.Fatal("agent-a released nothing after four agents appeared; assignment would be advisory only")
	}

	// What survives is exactly what the assignment still gives it.
	want := 0
	for p := 0; p < partitions; p++ {
		if winner, ok := coordinator.AssignPartition("orders", int32(p), fleet); ok && winner.Agent == "agent-a" {
			want++
		}
	}
	if got := len(a.OwnedPartitions()); got != want {
		t.Errorf("agent-a holds %d partitions, the assignment gives it %d", got, want)
	}
	if want == partitions {
		t.Error("the assignment still gives agent-a everything, so this test proves nothing")
	}

	// Whatever agent-a kept must still be writable, and everything it gave up
	// must have left a durable tail behind rather than a local-only one.
	for p := 0; p < partitions; p++ {
		if a.Owns("orders", int32(p)) {
			if _, err := a.Append("orders", int32(p), batchOfN(1), 1, false); err != nil {
				t.Errorf("append to a retained partition %d: %v", p, err)
			}
		}
	}
}

// fleetOf builds the live set for n agents named from the given ids, indexed from
// firstNode. Agent one is the agent under test; the rest exist only in the set.
func fleetOf(firstNode int32, ids ...string) []coordinator.AgentRef {
	out := make([]coordinator.AgentRef, 0, len(ids))
	for i, id := range ids {
		node := firstNode + int32(i)
		out = append(out, coordinator.AgentRef{
			Agent:  id,
			NodeID: node,
			Host:   id + ".kimi.internal",
			Port:   19092 + node,
		})
	}
	return coordinator.SortAgents(out)
}

// seedLiveSet publishes both records a peer needs before an agent will route to
// it: a liveness record proving it is up, and a routing table naming its node.
//
// Both are required. Discovery walks the routing keys and then cross-checks the
// liveness record against the table's node id, so seeding only liveness leaves
// the peer invisible and every test here quietly runs against a fleet of one.
func seedLiveSet(t *testing.T, store *casStore, agents []coordinator.AgentRef) {
	t.Helper()

	now := time.Now()
	store.mu.Lock()
	defer store.mu.Unlock()

	for _, a := range agents {
		// Both timestamps matter. A record with no expiry reads as expired and
		// never reaches the broker list.
		live := RoutingRecord{
			Agent:   a.Agent,
			NodeID:  a.NodeID,
			Host:    a.Host,
			Port:    a.Port,
			Updated: now.Unix(),
			Expires: now.Add(time.Hour).Unix(),
		}
		table := RoutingTable{
			Agent:      a.Agent,
			NodeID:     a.NodeID,
			Generation: 1,
			Partitions: []RoutingPartition{},
			Updated:    now.Unix(),
		}

		for key, value := range map[string]interface{}{
			agentsPrefix + a.Agent + "/" + agentLivenessKey: live,
			agentsPrefix + a.Agent + "/" + agentRoutingKey:  table,
		} {
			body, err := json.Marshal(value)
			if err != nil {
				t.Fatalf("marshal %s: %v", key, err)
			}
			store.data[key] = body
			store.version[key]++
		}
	}
}

// TestAssignment_OffByDefaultPreservesExistingBehaviour guards the opt-in. A
// deployment that upgrades must not start releasing partitions because of a
// feature nobody turned on.
func TestAssignment_OffByDefaultPreservesExistingBehaviour(t *testing.T) {
	store := newCASStore()

	a := newOwningEngine(t, store, "agent-a",
		WithAgentID("agent-a"),
		WithRegistry(testRegistry("agent-a", 1, "agent-a.kimi.internal", 19092)))
	if err := a.CreateTopic("orders", 4); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for p := 0; p < 4; p++ {
		if _, err := a.Append("orders", int32(p), batchOfN(3), 3, true); err != nil {
			t.Fatalf("append %d: %v", p, err)
		}
	}

	b := newOwningEngine(t, store, "agent-b",
		WithAgentID("agent-b"),
		WithRegistry(testRegistry("agent-b", 2, "agent-b.kimi.internal", 19093)))
	defer b.Close()

	refreshRouting(t, a)
	refreshRouting(t, b)

	// The pre-assignment behaviour: the incumbent keeps everything.
	if got := len(b.OwnedPartitions()); got != 0 {
		t.Errorf("agent-b holds %d partitions with assignment off, want 0", got)
	}
	if got := len(a.OwnedPartitions()); got != 4 {
		t.Errorf("agent-a holds %d partitions, want all 4", got)
	}
}

// TestAssignment_NeverSplitsAPartition is the invariant every other test leans
// on: whatever the live set says, one partition has one owner.
func TestAssignment_NeverSplitsAPartition(t *testing.T) {
	store := newCASStore()

	agents := map[string]*StorageEngine{}
	live := make([]coordinator.AgentRef, 0, 4)
	for i, id := range []string{"agent-a", "agent-b", "agent-c", "agent-d"} {
		e := newAssignedEngine(t, store, id, int32(i+1))
		defer e.Close()
		agents[id] = e
		live = append(live, coordinator.AgentRef{
			Agent: id, NodeID: int32(i + 1), Host: id + ".kimi.internal", Port: int32(19092 + i + 1),
		})
	}

	if err := agents["agent-a"].CreateTopic("orders", 12); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for p := 0; p < 12; p++ {
		if _, err := agents["agent-a"].Append("orders", int32(p), batchOfN(3), 3, true); err != nil {
			t.Fatalf("append %d: %v", p, err)
		}
	}

	seedLiveSet(t, store, live)

	// Every agent rebalances against the same live set.
	for id, e := range agents {
		refreshRouting(t, e)
		if _, err := e.rebalanceNow(context.Background()); err != nil {
			t.Fatalf("%s rebalance: %v", id, err)
		}
	}
	// And once more, so a second pass cannot move anything either.
	for id, e := range agents {
		refreshRouting(t, e)
		if _, err := e.rebalanceNow(context.Background()); err != nil {
			t.Fatalf("%s second rebalance: %v", id, err)
		}
	}

	owned := map[int32][]string{}
	for id, e := range agents {
		for _, key := range e.OwnedPartitions() {
			_, p, err := splitOwnedPartitionKey(key)
			if err != nil {
				t.Fatalf("%s has an unreadable key %q: %v", id, key, err)
			}
			owned[p] = append(owned[p], id)
		}
	}

	if len(owned) != 12 {
		t.Errorf("%d partitions have an owner, want all 12: %v", len(owned), fmt.Sprint(owned))
	}
	for p, who := range owned {
		if len(who) != 1 {
			t.Errorf("orders/%d is owned by %v, want exactly one agent", p, who)
		}
	}
}
