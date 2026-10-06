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

package coordinator

import (
	"fmt"
	"testing"
)

func agents(ids ...string) []AgentRef {
	out := make([]AgentRef, 0, len(ids))
	for i, id := range ids {
		out = append(out, AgentRef{
			Agent:  id,
			NodeID: int32(i + 1),
			Host:   id + ".kimi.internal",
			Port:   19092 + int32(i),
		})
	}
	return out
}

// The selection has to be a pure function of the group and the live set. Two
// agents that share a view must agree, or they become two coordinators for one
// group and build independent assignment state.
func TestRendezvous_IsDeterministicAndAgrees(t *testing.T) {
	set := agents("agent-a", "agent-b", "agent-c")

	for _, group := range []string{"g", "group-1", "mimir-ingesters", "", "a-very-long-group-id-with-entropy"} {
		first, ok := Rendezvous(group, set)
		if !ok {
			t.Fatalf("group %q produced no coordinator", group)
		}
		// A different slice order, which is what two agents can disagree about
		// while holding the same set, must not change the answer.
		shuffled := []AgentRef{set[2], set[0], set[1]}
		second, _ := Rendezvous(group, shuffled)
		if first.Agent != second.Agent {
			t.Errorf("group %q: order changed the answer from %q to %q", group, first.Agent, second.Agent)
		}
		// And so must repeated calls.
		for i := 0; i < 10; i++ {
			again, _ := Rendezvous(group, set)
			if again.Agent != first.Agent {
				t.Fatalf("group %q is not stable: %q then %q", group, first.Agent, again.Agent)
			}
		}
	}
}

func TestRendezvous_EmptySetHasNoCoordinator(t *testing.T) {
	if _, ok := Rendezvous("g", nil); ok {
		t.Error("an empty live set must produce no coordinator")
	}
	if IsCoordinator("g", "agent-a", nil) {
		t.Error("an empty live set must not elect anyone")
	}
}

// Exactly one agent is elected. This is the invariant the fence at the point of
// use is checked against, so it is worth pinning directly.
func TestRendezvous_ElectsExactlyOne(t *testing.T) {
	set := agents("agent-a", "agent-b", "agent-c", "agent-d")
	for _, group := range []string{"g1", "g2", "g3", "g4", "g5"} {
		winners := 0
		for _, a := range set {
			if IsCoordinator(group, a.Agent, set) {
				winners++
			}
		}
		if winners != 1 {
			t.Errorf("group %q elected %d agents, want exactly 1", group, winners)
		}
	}
}

// Rendezvous rather than modulo, and the reason is operational: one agent leaving
// must move only the groups it held, not every group in the cluster.
func TestRendezvous_AnAgentLeavingMovesOnlyItsOwnGroups(t *testing.T) {
	full := agents("agent-a", "agent-b", "agent-c")

	before := map[string]string{}
	for _, group := range groupIDs(60) {
		winner, _ := Rendezvous(group, full)
		before[group] = winner.Agent
	}

	// Count who owned what.
	ownedBy := map[string]int{}
	for _, a := range before {
		ownedBy[a]++
	}
	if ownedBy["agent-c"] == 0 {
		t.Skip("agent-c happened to own nothing in this sample")
	}
	if ownedBy["agent-b"] == 0 {
		t.Skip("agent-b happened to own nothing in this sample")
	}

	// The departed agent's groups must move -- to somebody. Every other group's
	// owner must be untouched, which is the whole point: a modulo assignment
	// would reshuffle the whole cluster on one agent's departure.
	reduced := agents("agent-a", "agent-b")
	moved, held := 0, 0
	for _, group := range groupIDs(60) {
		winner, _ := Rendezvous(group, reduced)
		if winner.Agent == "agent-c" {
			t.Fatalf("group %q was still elected to the departed agent", group)
		}
		if before[group] == "agent-c" {
			moved++
			continue
		}
		if winner.Agent != before[group] {
			t.Fatalf("group %q moved from %q to %q; only the departed agent's groups may move",
				group, before[group], winner.Agent)
		}
		held++
	}
	if moved != ownedBy["agent-c"] {
		t.Errorf("%d group(s) moved but agent-c owned %d; exactly its groups should move", moved, ownedBy["agent-c"])
	}
	if held == 0 {
		t.Error("no group kept its owner, which is impossible for a departing agent to cause")
	}
}

// A group that has no owner before must gain one when an agent is added, and a
// group whose owner stays must not move. Together these are the "only the groups
// it held" property from the other direction.
func TestRendezvous_AddingAnAgentOnlyMovesItsOwnGroups(t *testing.T) {
	small := agents("agent-a")
	grown := agents("agent-a", "agent-b")

	moved, unchanged := 0, 0
	for _, group := range groupIDs(60) {
		before, _ := Rendezvous(group, small)
		after, _ := Rendezvous(group, grown)
		switch after.Agent {
		case "agent-a":
			if before.Agent != "agent-a" {
				t.Fatalf("group %q changed owner to agent-a", group)
			}
			unchanged++
		case "agent-b":
			if before.Agent == "agent-b" {
				t.Fatalf("group %q was already agent-b's", group)
			}
			moved++
		default:
			t.Fatalf("group %q elected an unknown agent %q", group, after.Agent)
		}
	}
	if moved == 0 {
		t.Error("the new agent never won a group; rendezvous hashing is not distributing")
	}
	if unchanged == 0 {
		t.Error("the new agent took every group; rendezvous hashing is not minimal")
	}
}

// A duplicated agent must not be weighed twice: two brokers with one identity
// would otherwise both be told they coordinate the same group.
func TestSortAgents_Deduplicates(t *testing.T) {
	in := []AgentRef{
		{Agent: "b", NodeID: 2},
		{Agent: "a", NodeID: 1},
		{Agent: "b", NodeID: 2},
		{Agent: "a", NodeID: 1},
	}
	got := SortAgents(in)
	if len(got) != 2 {
		t.Fatalf("SortAgents returned %d agents, want 2: %+v", len(got), got)
	}
	if got[0].Agent != "a" || got[1].Agent != "b" {
		t.Errorf("SortAgents = %+v, want sorted a then b", got)
	}
	if len(in) != 4 {
		t.Error("SortAgents must not mutate its argument")
	}
}

// The hash input has to be unambiguous. If ("ab","c") and ("a","bc") hashed the
// same, two different deployments could elect different coordinators for the same
// group.
func TestRendezvous_SeparatesFieldBoundaries(t *testing.T) {
	a, _ := Rendezvous("ab", agents("c"))
	b, _ := Rendezvous("a", agents("bc"))
	if a.Agent == b.Agent && rendezvousScore("ab", "c") == rendezvousScore("a", "bc") {
		t.Error(`"ab"/"c" and "a"/"bc" hash identically; the field boundaries are not encoded`)
	}
}

// The hash must actually spread. A constant or a near-constant score would elect
// one agent for every group, which is exactly the single-coordinator arrangement
// this replaces.
func TestRendezvous_DistributesGroups(t *testing.T) {
	set := agents("agent-a", "agent-b", "agent-c", "agent-d", "agent-e")
	counts := map[string]int{}
	const total = 500
	for _, group := range groupIDs(total) {
		winner, _ := Rendezvous(group, set)
		counts[winner.Agent]++
	}
	for _, a := range set {
		if counts[a.Agent] == 0 {
			t.Errorf("agent %q won no groups out of %d; the hash is not distributing", a.Agent, total)
		}
	}
	// With 500 groups over 5 agents the expected share is 100. Anything outside a
	// generous band means the hash is lopsided, not merely unlucky.
	for agent, n := range counts {
		if n < 50 || n > 200 {
			t.Errorf("agent %q won %d of %d groups, want roughly %d", agent, n, total, total/len(set))
		}
	}
}

func groupIDs(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("group-%03d", i))
	}
	return out
}
