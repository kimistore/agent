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
	"errors"
	"testing"
	"time"
)

// A routable agent needs a node id and an advertised address. Without the
// address there is nothing for a client to be redirected to, and the agent
// deliberately stays out of its own broker list rather than advertising an empty
// host that a client would try to dial.
func testRegistry(agent string, nodeID int32, host string, port int32) RegistryConfig {
	return RegistryConfig{
		Enabled: true,
		AgentID: agent,
		NodeID:  nodeID,
		Host:    host,
		Port:    port,
		TTL:     routingTTL,
	}
}

// routingTTL is short enough that the discovery loop runs at least once per
// test, and comfortably over a second because a claim's expiry is stored in whole
// seconds: at a TTL of 1.5s the record would round down to roughly one second and
// peers would see a healthy claim as expired between renewals.
const routingTTL = 3 * time.Second

// newRoutingEngine builds an owning agent that also joins the cluster.
func newRoutingEngine(t *testing.T, store ObjectStore, agent string, nodeID int32, opts ...Option) *StorageEngine {
	t.Helper()
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		append([]Option{
			WithOwnership(testOwnership(agent, 30*time.Second)),
			WithAgentID(agent),
			WithRegistry(testRegistry(agent, nodeID, agent+".kimi.internal", 19092)),
		}, opts...)...)
	if err != nil {
		t.Fatalf("NewStorageEngine(%s): %v", agent, err)
	}
	return engine
}

// refresh forces the routing view to be rebuilt now, rather than waiting for the
// discovery interval.
func refreshRouting(t *testing.T, engine *StorageEngine) {
	t.Helper()
	engine.registry.setView(engine.registry.buildView(context.Background(), engine.routingPartitions(), true))
}

// An agent has to advertise itself, or no client can ever be routed to it. This
// is the smallest thing phase 3 has to get right.
func TestRouting_AgentAdvertisesItself(t *testing.T) {
	store := newCASStore()
	engine := newRoutingEngine(t, store, "agent-a", 7)
	defer engine.Close()

	view := engine.Routing()
	if len(view.Brokers) != 1 {
		t.Fatalf("brokers = %d, want just this agent: %+v", len(view.Brokers), view.Brokers)
	}
	got := view.Brokers[0]
	if got.NodeID != 7 || got.Host != "agent-a.kimi.internal" || got.Port != 19092 {
		t.Errorf("broker = %+v, want node 7 at agent-a.kimi.internal:19092", got)
	}
	if view.SelfNodeID != 7 {
		t.Errorf("SelfNodeID = %d, want 7; Metadata's fallback broker must use the same id", view.SelfNodeID)
	}

	// The record has to be in object storage, not just in memory: another agent
	// can only find it there.
	body, err := store.Get(context.Background(), agentsPrefix+"agent-a/liveness")
	if err != nil {
		t.Fatalf("no liveness record published: %v", err)
	}
	var record RoutingRecord
	if err := json.NewDecoder(body).Decode(&record); err != nil {
		t.Fatalf("liveness record does not parse: %v", err)
	}
	body.Close()
	if record.NodeID != 7 || record.Agent != "agent-a" {
		t.Errorf("published liveness = %+v, want agent-a at node 7", record)
	}
	if record.Expired(time.Now()) {
		t.Error("a liveness record published now must not read as expired")
	}
}

// The point of phase 3: two agents on one bucket, each seeing the other, each
// owning a different partition. Before this, the second agent was invisible and
// its partitions unreachable.
func TestRouting_TwoAgentsSeeEachOther(t *testing.T) {
	store := newCASStore()

	first := newRoutingEngine(t, store, "agent-a", 1)
	defer first.Close()
	if err := first.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// agent-a holds both partitions; agent-b starts and takes none, because a
	// live claim is a live claim.
	second := newRoutingEngine(t, store, "agent-b", 2)
	defer second.Close()

	refreshRouting(t, first)
	refreshRouting(t, second)

	firstView := first.Routing()
	if len(firstView.Brokers) != 2 {
		t.Fatalf("agent-a sees %d broker(s), want 2: %+v", len(firstView.Brokers), firstView.Brokers)
	}
	if second.Owns("orders", 0) {
		t.Error("agent-b must not own a partition agent-a holds")
	}

	// Release agent-a's partitions so agent-b can take them over, which is the
	// ordinary handover and the only way a partition moves between agents.
	if err := first.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}
	refreshRouting(t, second)
	if _, err := second.claimPartition(context.Background(), "orders", 0); err != nil {
		t.Fatalf("agent-b should be able to claim a released partition: %v", err)
	}
	if _, err := second.claimPartition(context.Background(), "orders", 1); err != nil {
		t.Fatalf("agent-b should be able to claim a released partition: %v", err)
	}
	refreshRouting(t, second)

	view := second.Routing()
	if len(view.Brokers) != 1 || view.Brokers[0].Agent != "agent-b" {
		t.Fatalf("agent-b's broker list should be just itself once agent-a is gone: %+v", view.Brokers)
	}
	for _, pid := range []int32{0, 1} {
		route, ok := view.Owner("orders", pid)
		if !ok {
			t.Fatalf("orders/%d has no owner in agent-b's own view", pid)
		}
		if route.NodeID != 2 {
			t.Errorf("orders/%d leader = node %d, want 2", pid, route.NodeID)
		}
		if route.Epoch < 2 {
			t.Errorf("orders/%d epoch = %d, want the takeover epoch above 1", pid, route.Epoch)
		}
	}
}

// A dead agent has to leave the broker list, or clients keep being sent to an
// address that is not answering.
func TestRouting_DeadAgentLeavesTheBrokerList(t *testing.T) {
	store := newCASStore()

	alive := newRoutingEngine(t, store, "agent-a", 1)
	defer alive.Close()

	// An agent that publishes and then goes away without releasing its liveness,
	// which is what a crash looks like.
	dying, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOwnership(testOwnership("agent-b", 30*time.Second)),
		WithAgentID("agent-b"),
		WithRegistry(testRegistry("agent-b", 2, "agent-b.kimi.internal", 19092)))
	if err != nil {
		t.Fatalf("agent-b: %v", err)
	}

	refreshRouting(t, alive)
	if got := len(alive.Routing().Brokers); got != 2 {
		t.Fatalf("brokers = %d, want both agents while agent-b is alive", got)
	}

	// Expire agent-b's liveness record by hand rather than waiting out the TTL.
	expireAgent(t, store, "agent-b")

	refreshRouting(t, alive)
	view := alive.Routing()
	if len(view.Brokers) != 1 || view.Brokers[0].Agent != "agent-a" {
		t.Fatalf("a dead agent must leave the broker list: %+v", view.Brokers)
	}
	if err := dying.Close(); err != nil {
		t.Fatalf("close agent-b: %v", err)
	}
}

// A routing table older than the TTL means its writer is wedged even if its
// liveness record has time left. Serving its partitions from stale routing would
// send clients to an agent that is not renewing, so they are treated as unowned.
func TestRouting_StaleTableIsIgnoredEvenWhileTheAgentIsLive(t *testing.T) {
	store := newCASStore()

	alive := newRoutingEngine(t, store, "agent-a", 1)
	defer alive.Close()
	if err := alive.CreateTopic("orders", 3); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	// A wedged peer claiming orders/2, which this agent does not own.
	if _, err := alive.claimPartition(context.Background(), "orders", 2); err == nil {
		t.Skip("test setup could not create an unowned partition")
	}
	publishStaleAgent(t, store, "agent-b", 2, []RoutingPartition{
		{Topic: "orders", Partition: 0, Epoch: 5},
	}, -time.Hour)

	refreshRouting(t, alive)
	view := alive.Routing()
	if len(view.Brokers) != 1 {
		t.Errorf("brokers = %+v, want the wedged agent excluded", view.Brokers)
	}
	if route, ok := view.Owner("orders", 0); ok {
		t.Errorf("orders/0 is attributed to %+v, but its table is stale so it must be unowned", route)
	}
	// The partition still has to be reported as existing, so a client is told to
	// retry rather than that the topic has no such partition.
	parts := view.PartitionsOf("orders")
	if len(parts) != 3 {
		t.Errorf("orders partitions = %v, want all three: an unowned partition still exists", parts)
	}
}

// A routing table is only trustworthy if it agrees with the liveness record
// beside it. One that does not means an agent restarted with a different
// identity, and pairing its address with the other node id would send clients
// somewhere they cannot use.
func TestRouting_TableDisagreeingWithLivenessIsIgnored(t *testing.T) {
	store := newCASStore()

	alive := newRoutingEngine(t, store, "agent-a", 1)
	defer alive.Close()

	now := time.Now()
	put(t, store, agentsPrefix+"agent-b", mustJSON(t, RoutingRecord{
		Agent: "agent-b", NodeID: 2, Host: "agent-b.kimi.internal", Port: 19092,
		Updated: now.Unix(), Expires: now.Add(time.Minute).Unix(),
	}))
	put(t, store, agentsPrefix+"agent-b/routing", mustJSON(t, RoutingTable{
		Agent: "agent-b", NodeID: 99, Generation: 1, Updated: now.Unix(),
	}))

	refreshRouting(t, alive)
	view := alive.Routing()
	for _, b := range view.Brokers {
		if b.Agent == "agent-b" {
			t.Fatalf("an inconsistent agent was admitted as %+v", b)
		}
	}
}

// Two agents configured with the same node id would send a client to whichever
// answered last. The duplicate is dropped rather than emitted.
func TestRouting_DuplicateNodeIDsAreCollapsed(t *testing.T) {
	store := newCASStore()

	alive := newRoutingEngine(t, store, "agent-a", 5)
	defer alive.Close()

	now := time.Now()
	put(t, store, agentsPrefix+"agent-b", mustJSON(t, RoutingRecord{
		Agent: "agent-b", NodeID: 5, Host: "agent-b.kimi.internal", Port: 19092,
		Updated: now.Unix(), Expires: now.Add(time.Minute).Unix(),
	}))

	refreshRouting(t, alive)
	view := alive.Routing()
	if len(view.Brokers) != 1 {
		t.Fatalf("brokers = %+v, want the duplicate collapsed to one", view.Brokers)
	}
	if view.Brokers[0].Agent != "agent-a" {
		t.Errorf("kept %q; this agent's own entry should win the tie", view.Brokers[0].Agent)
	}
}

// A failed discovery must not empty the cluster. Reporting "no brokers" because
// one LIST timed out would take every client's metadata away at once.
func TestRouting_RefreshFailureKeepsThePreviousView(t *testing.T) {
	store := newCASStore()
	engine := newRoutingEngine(t, store, "agent-a", 1)
	defer engine.Close()

	if err := engine.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	refreshRouting(t, engine)
	before := engine.Routing()
	if len(before.Brokers) != 1 || len(before.PartitionsOf("orders")) != 2 {
		t.Fatalf("view not built: %+v", before)
	}

	store.failList(errors.New("injected list failure"))
	defer store.failList(nil)

	refreshRouting(t, engine)
	after := engine.Routing()
	if len(after.Brokers) != 1 {
		t.Errorf("brokers = %d after a failed refresh, want the previous view kept", len(after.Brokers))
	}
	if !after.Stale {
		t.Error("a view kept through a failed refresh must be marked stale")
	}
	if len(after.PartitionsOf("orders")) != 2 {
		t.Errorf("the inventory was lost: %v", after.PartitionsOf("orders"))
	}
}

// The routing table is republished only when the owned set actually changes. A
// PUT per agent per interval for an identical table is pure cost, and it makes
// every reader's table look fresh when nothing moved.
func TestRouting_TableIsRepublishedOnlyOnChange(t *testing.T) {
	store := newCASStore()
	engine := newRoutingEngine(t, store, "agent-a", 1)
	defer engine.Close()
	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Take ownership of the new partition so the published set changes.
	if _, err := engine.claimPartition(context.Background(), "orders", 0); err != nil {
		t.Fatalf("claim: %v", err)
	}

	ctx := context.Background()
	partitions := engine.routingPartitions()
	if err := engine.registry.publish(ctx, partitions); err != nil {
		t.Fatalf("publish: %v", err)
	}
	first := countPuts(t, store, agentsPrefix+"agent-a/routing")

	for i := 0; i < 3; i++ {
		if err := engine.registry.publish(ctx, partitions); err != nil {
			t.Fatalf("republish: %v", err)
		}
	}
	if got := countPuts(t, store, agentsPrefix+"agent-a/routing"); got != first {
		t.Errorf("routing table written %d times for an unchanged set, want %d", got, first)
	}

	// Changing the set does republish it.
	if err := engine.CreateTopic("events", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := engine.registry.publish(ctx, engine.routingPartitions()); err != nil {
		t.Fatalf("publish after change: %v", err)
	}
	if got := countPuts(t, store, agentsPrefix+"agent-a/routing"); got <= first {
		t.Errorf("a changed partition set must republish the table")
	}
}

// An agent with no advertised address cannot be dialled, so it must not appear in
// a broker list. It still owns what it holds, so its partitions are not
// leaderless.
func TestRouting_UndescribableAgentStaysOutOfTheBrokerList(t *testing.T) {
	store := newCASStore()

	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOwnership(testOwnership("agent-a", 30*time.Second)),
		WithAgentID("agent-a"),
		WithRegistry(RegistryConfig{Enabled: true, AgentID: "agent-a", NodeID: 1, TTL: time.Minute}))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer engine.Close()
	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	view := engine.Routing()
	if len(view.Brokers) != 0 {
		t.Errorf("brokers = %+v, want none: there is no address to advertise", view.Brokers)
	}
	if _, ok := view.Owner("orders", 0); !ok {
		t.Error("the partition is still owned here and must not be reported leaderless")
	}
}

// Liveness and routing are two different objects in one flat prefix, so
// discovery has to tell them apart from their key shapes.
func TestParseAgentKey_TellsTheThreeObjectsApart(t *testing.T) {
	for _, tc := range []struct {
		key  string
		ns   string
		kind agentKeyKind
	}{
		{key: "_agents/host-a/liveness", ns: "host-a", kind: agentKindLiveness},
		{key: "_agents/host-a/routing", ns: "host-a", kind: agentKindRouting},
		{key: "_agents/host-a/checkpoint.json", ns: "host-a", kind: agentKindCheckpoint},
		{key: "_topics/orders/_manifest/0", kind: agentKindUnknown},
		{key: "_agents/", kind: agentKindUnknown},
		{key: "_agents/host-a", kind: agentKindUnknown},
		{key: "_agents/host-a/something-else", kind: agentKindUnknown},
	} {
		ns, kind := parseAgentKey(tc.key)
		if ns != tc.ns || kind != tc.kind {
			t.Errorf("parseAgentKey(%q) = (%q, %v), want (%q, %v)", tc.key, ns, kind, tc.ns, tc.kind)
		}
	}
}

// A checkpoint must never be mistaken for an agent's liveness record: it is a
// private durable object, not an address anyone should dial.
func TestRouting_CheckpointIsNotAnAgent(t *testing.T) {
	store := newCASStore()
	engine := newRoutingEngine(t, store, "agent-a", 1)
	defer engine.Close()
	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := engine.SaveCheckpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	refreshRouting(t, engine)
	for _, b := range engine.Routing().Brokers {
		if b.Agent != "agent-a" {
			t.Errorf("a checkpoint was discovered as a broker: %+v", b)
		}
	}
}

// Two processes must not both claim to be one agent: a client handed the same
// node id from two brokers connects to whichever it heard last.
func TestRouting_LivenessClaimIsNotStolen(t *testing.T) {
	store := newCASStore()

	first := newRoutingEngine(t, store, "agent-a", 1)
	defer first.Close()

	// A second process sharing the identity but running as a different broker.
	impostor := newRoutingEngine(t, store, "agent-a", 42)

	record := store.routingRecord(t, "agent-a")
	if record.NodeID != 1 {
		t.Errorf("liveness record node = %d, want 1: the impostor must not have taken it over", record.NodeID)
	}
	if err := impostor.Close(); err != nil {
		t.Fatalf("close impostor: %v", err)
	}
}

// A graceful shutdown marks the agent dead, so peers stop routing to it
// immediately instead of waiting out the TTL.
func TestRouting_MarkedDeadOnClose(t *testing.T) {
	store := newCASStore()
	engine := newRoutingEngine(t, store, "agent-a", 1)
	if err := engine.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	body, err := store.Get(context.Background(), agentsPrefix+"agent-a/liveness")
	if err != nil {
		t.Fatalf("liveness record should still exist as a tombstone: %v", err)
	}
	var record RoutingRecord
	if err := json.NewDecoder(body).Decode(&record); err != nil {
		t.Fatalf("tombstone does not parse: %v", err)
	}
	body.Close()
	if !record.Expired(time.Now()) {
		t.Errorf("liveness record should be expired after shutdown: %+v", record)
	}
}

// Deleting a topic has to take its claims with it, or the partitions stay
// unclaimable for as long as the TTL.
func TestRouting_DeletingATopicReleasesItsClaims(t *testing.T) {
	store := newCASStore()
	engine := newRoutingEngine(t, store, "agent-a", 1)
	defer engine.Close()
	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := engine.DeleteTopic("orders"); err != nil {
		t.Fatalf("delete topic: %v", err)
	}

	// The cleanup runs in the background by design, so poll for it rather than
	// assuming it has landed by the time DeleteTopic returns.
	key := partitionOwnerKey("orders", 0)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := store.Get(context.Background(), key); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("the ownership claim for a deleted topic was never released: %s", key)
}

// ---- helpers ----

func put(t *testing.T, store *casStore, key string, body []byte) {
	t.Helper()
	if err := store.Put(context.Background(), key, bytes.NewReader(body)); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return body
}

// expireAgent ages an agent's liveness record past its expiry, which is what a
// crash followed by the TTL elapsing looks like from the outside.
func expireAgent(t *testing.T, store *casStore, agent string) {
	t.Helper()
	key := agentsPrefix + agent + "/liveness"
	store.mu.Lock()
	defer store.mu.Unlock()
	raw, ok := store.data[key]
	if !ok {
		t.Fatalf("no liveness record for %s", agent)
	}
	var record RoutingRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("liveness record for %s does not parse: %v", agent, err)
	}
	record.Expires = time.Now().Add(-time.Minute).Unix()
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	store.data[key] = body
	store.version[key]++
}

// publishStaleAgent writes a liveness record that is current and a routing table
// that is not, which is what an agent wedged between the two writes looks like.
func publishStaleAgent(t *testing.T, store *casStore, agent string, nodeID int32, owned []RoutingPartition, tableAge time.Duration) {
	t.Helper()
	now := time.Now()
	put(t, store, agentsPrefix+agent+"/liveness", mustJSON(t, RoutingRecord{
		Agent: agent, NodeID: nodeID, Host: agent + ".kimi.internal", Port: 19092,
		Updated: now.Unix(), Expires: now.Add(time.Minute).Unix(),
	}))
	put(t, store, agentsPrefix+agent+"/routing", mustJSON(t, RoutingTable{
		Agent:      agent,
		NodeID:     nodeID,
		Generation: 1,
		Partitions: owned,
		Updated:    now.Add(tableAge).Unix(),
	}))
}

// countPuts counts how many times a key has been written.
func countPuts(t *testing.T, store *casStore, key string) int {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.putKeys[key]
}
