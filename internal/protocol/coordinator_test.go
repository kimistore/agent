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

package protocol

import (
	"testing"
	"time"

	"kimistore/internal/storage"
)

// findCoordinatorNode runs a FindCoordinator and returns the node id it named.
func findCoordinatorNode(t *testing.T, se *storage.StorageEngine, cfg ServerConfig, group string) int32 {
	t.Helper()
	dec := dispatch(t, se, cfg, ApiKeyFindCoordinator, 0, coordinatorRequestFor(group))
	if code, _ := dec.Int16(); code != ErrNone {
		t.Fatalf("FindCoordinator error = %d", code)
	}
	node, err := dec.Int32()
	if err != nil {
		t.Fatalf("coordinator node id: %v", err)
	}
	host, _ := dec.String()
	port, _ := dec.Int32()
	if host == "" || port == 0 {
		t.Errorf("coordinator address = %q:%d, want something dialable", host, port)
	}
	return node
}

// joinGroupRequest encodes a JoinGroup v1 request.
func joinGroupRequest(group, member string) []byte {
	enc := NewEncoder()
	enc.String(group)
	enc.Int32(30_000) // session timeout
	enc.Int32(30_000) // rebalance timeout
	enc.String(member)
	enc.String("consumer")
	enc.Int32(1) // one protocol
	enc.String("range")
	enc.PutBytes(nil)
	return enc.Bytes()
}

// joinGroupCode runs a JoinGroup and returns the error code it answered with,
// along with the leader and member ids it assigned.
func joinGroup(t *testing.T, se *storage.StorageEngine, cfg ServerConfig, group, member string) (int16, string, string) {
	t.Helper()
	dec := dispatch(t, se, cfg, ApiKeyJoinGroup, 1, joinGroupRequest(group, member))
	code, err := dec.Int16()
	if err != nil {
		t.Fatalf("JoinGroup error code: %v", err)
	}
	_, _ = dec.Int32()  // generation id
	_, _ = dec.String() // selected protocol
	leader, _ := dec.String()
	newMember, _ := dec.String()
	return code, leader, newMember
}

// heartbeatRequest encodes a Heartbeat v0 request.
func heartbeatRequest(group, member string, generation int32) []byte {
	enc := NewEncoder()
	enc.String(group)
	enc.Int32(generation)
	enc.String(member)
	return enc.Bytes()
}

// With one agent, every group is coordinated by it. The regression this guards
// is a fence that refuses everything because the agent does not recognise itself.
func TestCoordinator_SingleAgentCoordinatesEverything(t *testing.T) {
	se := testStore(t)
	cfg := testConfig()
	cfg.NodeID = 4

	for _, group := range []string{"g1", "g2", "g3"} {
		if node := findCoordinatorNode(t, se, cfg, group); node != 4 {
			t.Errorf("FindCoordinator(%q) = node %d, want this agent (4)", group, node)
		}
		code, leader, member := joinGroup(t, se, cfg, group, "m1")
		if code != ErrNone {
			t.Errorf("JoinGroup(%q) = %d, want 0", group, code)
		}
		if leader == "" {
			t.Errorf("JoinGroup(%q) returned no leader", group)
		}
		if member != "m1" {
			t.Errorf("JoinGroup(%q) member id = %q, want m1", group, member)
		}
	}
}

// Two agents, one bucket: FindCoordinator must name the same coordinator on both
// for the same group, because both hold the same live set. If they disagreed,
// each would build its own group state and members would get different partitions.
func TestCoordinator_TwoAgentsAgreeOnTheSameGroup(t *testing.T) {
	store := newProtocolCASStore()

	first := startAgent(t, store, "agent-1", 1)
	defer first.Close()
	second := startAgent(t, store, "agent-2", 2)
	defer second.Close()

	waitForRouting(t, second, "both agents to appear", func(s storage.RoutingSnapshot) bool {
		return len(s.Brokers) == 2
	})
	waitForRouting(t, first, "both agents to appear", func(s storage.RoutingSnapshot) bool {
		return len(s.Brokers) == 2
	})

	cfg1, cfg2 := agentConfig(1), agentConfig(2)
	agree, disagree := 0, 0
	for _, group := range []string{"g1", "g2", "g3", "g4", "g5", "g6", "g7", "g8"} {
		a := findCoordinatorNode(t, first, cfg1, group)
		b := findCoordinatorNode(t, second, cfg2, group)
		if a != b {
			disagree++
			t.Errorf("group %q: agent-1 says coordinator is node %d, agent-2 says %d", group, a, b)
			continue
		}
		agree++

		// Whichever agent it named must accept the join; the other must refuse.
		owner, other := first, second
		if b == 2 {
			owner, other = second, first
		}
		ownerCfg, otherCfg := cfg1, cfg2
		if b == 2 {
			ownerCfg, otherCfg = cfg2, cfg1
		}

		code, _, _ := joinGroup(t, owner, ownerCfg, group, "m1")
		if code != ErrNone {
			t.Errorf("group %q: the elected coordinator refused a join with %d", group, code)
		}
		refused, _, _ := joinGroup(t, other, otherCfg, group, "m1")
		if refused != ErrNotCoordinator {
			t.Errorf("group %q: a non-coordinator answered JoinGroup with %d, want %d (NOT_COORDINATOR)",
				group, refused, ErrNotCoordinator)
		}
	}
	if disagree > 0 {
		t.Fatalf("%d group(s) were assigned two different coordinators", disagree)
	}
	if agree == 0 {
		t.Fatal("no groups were agreed on")
	}
}

// The fence must cover the group operations that hand out state, not just the
// one that starts them. A SyncGroup answered by a non-coordinator would return an
// assignment computed against a member set that agent knows nothing about.
func TestCoordinator_NonCoordinatorRefusesGroupOperations(t *testing.T) {
	store := newProtocolCASStore()

	first := startAgent(t, store, "agent-1", 1)
	defer first.Close()
	second := startAgent(t, store, "agent-2", 2)
	defer second.Close()

	waitForRouting(t, first, "both agents to appear", func(s storage.RoutingSnapshot) bool {
		return len(s.Brokers) == 2
	})
	waitForRouting(t, second, "both agents to appear", func(s storage.RoutingSnapshot) bool {
		return len(s.Brokers) == 2
	})

	// Find a group the second agent coordinates.
	var group string
	for _, candidate := range []string{"g1", "g2", "g3", "g4", "g5", "g6", "g7", "g8"} {
		if findCoordinatorNode(t, second, agentConfig(2), candidate) == 2 {
			group = candidate
			break
		}
	}
	if group == "" {
		t.Skip("the second agent won none of the sampled groups")
	}

	// It accepts a join, so the group exists there.
	if code, _, _ := joinGroup(t, second, agentConfig(2), group, "m1"); code != ErrNone {
		t.Fatalf("the coordinator refused its own join with %d", code)
	}

	// A heartbeat for that group, arriving at the wrong agent, must be refused.
	dec := dispatch(t, first, agentConfig(1), ApiKeyHeartbeat, 0, heartbeatRequest(group, "m1", 1))
	code, err := dec.Int16()
	if err != nil {
		t.Fatalf("heartbeat error code: %v", err)
	}
	if code != ErrNotCoordinator {
		t.Errorf("Heartbeat at a non-coordinator = %d, want %d (NOT_COORDINATOR)", code, ErrNotCoordinator)
	}

	// And a SyncGroup likewise.
	enc := NewEncoder()
	enc.String(group)
	enc.Int32(1) // generation
	enc.String("m1")
	enc.Int32(0) // no assignments
	dec = dispatch(t, first, agentConfig(1), ApiKeySyncGroup, 0, enc.Bytes())
	if code, err = dec.Int16(); err != nil {
		t.Fatalf("SyncGroup error code: %v", err)
	}
	if code != ErrNotCoordinator {
		t.Errorf("SyncGroup at a non-coordinator = %d, want %d (NOT_COORDINATOR)", code, ErrNotCoordinator)
	}
}

// When the coordinator's agent goes away, its groups move. The members rejoin
// against the new coordinator and their committed offsets survive, because those
// are durable in object storage rather than in group state.
func TestCoordinator_GroupMovesWhenTheCoordinatorLeaves(t *testing.T) {
	store := newProtocolCASStore()

	first := startAgent(t, store, "agent-1", 1)
	second := startAgent(t, store, "agent-2", 2)
	defer second.Close()

	waitForRouting(t, second, "both agents to appear", func(s storage.RoutingSnapshot) bool {
		return len(s.Brokers) == 2
	})
	waitForRouting(t, first, "both agents to appear", func(s storage.RoutingSnapshot) bool {
		return len(s.Brokers) == 2
	})

	// Find a group agent-1 coordinates.
	var group string
	for _, candidate := range []string{"g1", "g2", "g3", "g4", "g5", "g6", "g7", "g8"} {
		if findCoordinatorNode(t, first, agentConfig(1), candidate) == 1 {
			group = candidate
			break
		}
	}
	if group == "" {
		t.Skip("the first agent won none of the sampled groups")
	}

	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if code, _, _ := joinGroup(t, first, agentConfig(1), group, "m1"); code != ErrNone {
		t.Fatalf("join on the original coordinator: %d", code)
	}
	// A committed offset, which is the part of a group that survives.
	if err := first.SaveOffset(group, "orders", 0, 42); err != nil {
		t.Fatalf("commit offset: %v", err)
	}

	// Agent-1 goes away.
	if err := first.Close(); err != nil {
		t.Fatalf("close agent-1: %v", err)
	}

	waitForRouting(t, second, "the departed agent to leave the broker list", func(s storage.RoutingSnapshot) bool {
		for _, b := range s.Brokers {
			if b.Agent == "agent-1" {
				return false
			}
		}
		return len(s.Brokers) == 1
	})

	// With one agent left it necessarily coordinates the group.
	if node := findCoordinatorNode(t, second, agentConfig(2), group); node != 2 {
		t.Fatalf("FindCoordinator after the departure = node %d, want 2", node)
	}

	// The member rejoins against the new coordinator, which knows no members.
	code, _, member := joinGroup(t, second, agentConfig(2), group, "m1")
	if code != ErrNone {
		t.Fatalf("rejoin on the new coordinator = %d, want 0", code)
	}
	if member != "m1" {
		t.Errorf("member id after rejoin = %q, want m1", member)
	}

	// And the committed offset is still there, which is what stops the consumer
	// replaying the whole topic.
	offset, err := second.LoadOffset(group, "orders", 0)
	if err != nil {
		t.Fatalf("load offset: %v", err)
	}
	if offset != 42 {
		t.Errorf("committed offset after the coordinator changed = %d, want 42", offset)
	}
}

// An agent that cannot prove it is alive must stop coordinating immediately. A
// coordinator has no epoch to fence it with, so unlike partition ownership there
// is no grace window: it may already have been replaced, and two coordinators for
// one group assign different partitions to the same consumers.
func TestCoordinator_UnfencedAgentStopsCoordinating(t *testing.T) {
	store := newProtocolCASStore()
	se := startAgent(t, store, "agent-1", 5)
	defer se.Close()
	cfg := agentConfig(5)

	if !se.AgentLive() {
		t.Fatal("a freshly started agent should report itself live")
	}
	if code, _, _ := joinGroup(t, se, cfg, "g1", "m1"); code != ErrNone {
		t.Fatalf("a healthy agent refused a join with %d", code)
	}

	// Break the store so the liveness record can no longer be written.
	store.failPuts(true)

	waitFor(t, 5*time.Second, func() bool { return !se.AgentLive() },
		"the agent to notice it can no longer prove it is alive")

	if se.AgentLive() {
		t.Fatal("the agent still reports itself live after its liveness record could not be written")
	}
	if node := findCoordinatorNode(t, se, cfg, "g1"); node != 5 {
		t.Errorf("FindCoordinator while unfenced = node %d, want this agent: it is the only one left", node)
	}
	code, _, _ := joinGroup(t, se, cfg, "g2", "m1")
	if code != ErrNotCoordinator {
		t.Errorf("JoinGroup while unfenced = %d, want %d (NOT_COORDINATOR)", code, ErrNotCoordinator)
	}
}

// Consumer offsets are not group state: they live in object storage so a
// coordinator change cannot lose them. This is what makes the full rebalance on a
// coordinator change affordable.
func TestCoordinator_OffsetsAreDurableAcrossCoordinatorChanges(t *testing.T) {
	offsetStore := newProtocolCASStore()
	se, err := storage.NewStorageEngine(t.TempDir(), offsetStore, "bucket", storage.RetentionConfig{},
		storage.WithOwnership(storage.OwnershipConfig{Enabled: true, Agent: "agent-1", TTL: routingTTL}),
		storage.WithAgentID("agent-1"),
		storage.WithRegistry(storage.RegistryConfig{
			Enabled: true, AgentID: "agent-1", NodeID: 1,
			Host: "agent-1.kimi.internal", Port: 19092, TTL: routingTTL,
		}),
	)
	if err != nil {
		t.Fatalf("first engine: %v", err)
	}
	if err := se.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	const group = "mimir-ingesters"
	for p := int32(0); p < 2; p++ {
		if err := se.SaveOffset(group, "orders", p, int64(100+p)); err != nil {
			t.Fatalf("save offset: %v", err)
		}
	}

	// Close the first agent so its buffered offset commits are flushed, exactly
	// as they would be on a handover.
	if err := se.Close(); err != nil {
		t.Fatalf("close first engine: %v", err)
	}

	// A second engine over the same bucket is the cheapest stand-in for a
	// coordinator change: nothing about the offsets may depend on in-memory
	// group state, and nothing about them may depend on which agent serves them.
	restarted, err := storage.NewStorageEngine(t.TempDir(), offsetStore, "bucket", storage.RetentionConfig{},
		storage.WithOwnership(storage.OwnershipConfig{Enabled: true, Agent: "agent-2", TTL: routingTTL}),
		storage.WithAgentID("agent-2"),
		storage.WithRegistry(storage.RegistryConfig{
			Enabled: true, AgentID: "agent-2", NodeID: 2,
			Host: "agent-2.kimi.internal", Port: 19093, TTL: routingTTL,
		}),
	)
	if err != nil {
		t.Fatalf("second engine: %v", err)
	}
	defer restarted.Close()
	for p := int32(0); p < 2; p++ {
		got, lerr := restarted.LoadOffset(group, "orders", p)
		if lerr != nil {
			t.Fatalf("load offset for partition %d: %v", p, lerr)
		}
		if want := int64(100 + p); got != want {
			t.Errorf("offset for partition %d = %d, want %d", p, got, want)
		}
	}
}

// waitFor polls until cond holds or the budget runs out.
func waitFor(t *testing.T, budget time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
