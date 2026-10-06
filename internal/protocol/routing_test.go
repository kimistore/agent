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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"kimistore/internal/storage"
)

// casStore is an in-memory object store with real compare-and-swap, so two
// engines can share one bucket in a test. Routing and ownership are both built
// on conditional writes; a store without them cannot exercise either.
type casStore struct {
	mu     sync.Mutex
	data   map[string][]byte
	ver    map[string]int64
	putErr error
}

func newProtocolCASStore() *casStore {
	return &casStore{data: map[string][]byte{}, ver: map[string]int64{}}
}

func (s *casStore) Put(_ context.Context, key string, r io.Reader) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.putErr != nil {
		return s.putErr
	}
	s.data[key] = body
	s.ver[key]++
	return nil
}

// failPuts makes every Put fail, which is how a test simulates an object store
// this agent can no longer prove anything to.
func (s *casStore) failPuts(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fail {
		s.putErr = errors.New("injected put failure")
	} else {
		s.putErr = nil
	}
}

func (s *casStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (s *casStore) List(_ context.Context, prefix string) ([]storage.ObjectMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []storage.ObjectMetadata
	for k := range s.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, storage.ObjectMetadata{Key: k, Size: int64(len(s.data[k]))})
		}
	}
	return out, nil
}

func (s *casStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	delete(s.ver, key)
	return nil
}

func (s *casStore) GetRange(_ context.Context, key string, start, length int64) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.data[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	if start > int64(len(body)) {
		return nil, fmt.Errorf("range beyond end")
	}
	end := int64(len(body))
	if length > 0 && start+length < end {
		end = start + length
	}
	return io.NopCloser(bytes.NewReader(body[start:end])), nil
}

func (s *casStore) GetVersion(_ context.Context, key string) ([]byte, string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.data[key]
	if !ok {
		return nil, "", false, nil
	}
	return body, fmt.Sprintf("v%d", s.ver[key]), true, nil
}

func (s *casStore) PutVersion(_ context.Context, key string, data []byte, version string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.putErr != nil {
		return "", s.putErr
	}
	current, exists := s.ver[key]
	switch {
	case version == "" && exists:
		return "", storage.ErrVersionMismatch
	case version != "" && (!exists || fmt.Sprintf("v%d", current) != version):
		return "", storage.ErrVersionMismatch
	}
	s.data[key] = data
	s.ver[key]++
	return fmt.Sprintf("v%d", s.ver[key]), nil
}

// routingTTL is short so the discovery interval is short enough to test, and
// comfortably over a second because a claim's expiry is stored in whole seconds:
// at 1.5s the record rounds down to about one second and a peer would read a
// healthy claim as expired between renewals.
const routingTTL = 3 * time.Second

// agentConfig is the ServerConfig an agent under test reports.
func agentConfig(nodeID int32) ServerConfig {
	cfg := DefaultServerConfig()
	cfg.NodeID = nodeID
	cfg.AdvertisedHost = fmt.Sprintf("agent-%d.kimi.internal", nodeID)
	cfg.AdvertisedPort = 19092 + nodeID
	return cfg
}

// startAgent brings up an engine that participates in ownership and routing.
func startAgent(t *testing.T, store storage.ObjectStore, id string, nodeID int32) *storage.StorageEngine {
	t.Helper()
	engine, err := storage.NewStorageEngine(t.TempDir(), store, "bucket", storage.RetentionConfig{},
		storage.WithOwnership(storage.OwnershipConfig{Enabled: true, Agent: id, TTL: routingTTL}),
		storage.WithAgentID(id),
		storage.WithRegistry(storage.RegistryConfig{
			Enabled: true,
			AgentID: id,
			NodeID:  nodeID,
			Host:    fmt.Sprintf("agent-%d.kimi.internal", nodeID),
			Port:    19092 + nodeID,
			TTL:     routingTTL,
		}),
	)
	if err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	return engine
}

// waitForRouting blocks until the engine's routing view satisfies cond, or fails
// the test. Discovery runs on a timer, so this is how a test waits for the
// cluster view rather than reaching into the registry.
func waitForRouting(t *testing.T, engine *storage.StorageEngine, what string, cond func(storage.RoutingSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond(engine.Routing()) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; view is %+v", what, engine.Routing())
}

// ---- decoded Metadata response ----

type metaBroker struct {
	nodeID int32
	host   string
	port   int32
}

type metaPartition struct {
	id          int32
	errorCode   int16
	leader      int32
	leaderEpoch int32
	replicas    []int32
	isr         []int32
}

type metaTopic struct {
	name       string
	partitions []metaPartition
}

type metaResponse struct {
	brokers    []metaBroker
	controller int32
	topics     []metaTopic
}

func (r metaResponse) topic(t *testing.T, name string) metaTopic {
	t.Helper()
	for _, tp := range r.topics {
		if tp.name == name {
			return tp
		}
	}
	t.Fatalf("no topic %q in the response (%v)", name, r.topics)
	return metaTopic{}
}

func (r metaResponse) brokerNodeIDs() []int32 {
	out := make([]int32, 0, len(r.brokers))
	for _, b := range r.brokers {
		out = append(out, b.nodeID)
	}
	return out
}

func (tp metaTopic) partition(t *testing.T, id int32) metaPartition {
	t.Helper()
	for _, p := range tp.partitions {
		if p.id == id {
			return p
		}
	}
	t.Fatalf("topic %q has no partition %d (has %v)", tp.name, id, tp.partitions)
	return metaPartition{}
}

// readInt32Array decodes a length-prefixed int32 array, used for replica, isr and
// offline-replica lists.
func readInt32Array(t *testing.T, dec *Decoder, what string) []int32 {
	t.Helper()
	n, err := dec.Int32()
	if err != nil {
		t.Fatalf("%s length: %v", what, err)
	}
	if n < 0 || n > 1_000_000 {
		t.Fatalf("%s length = %d, implausible", what, n)
	}
	out := make([]int32, 0, n)
	for i := int32(0); i < n; i++ {
		v, err := dec.Int32()
		if err != nil {
			t.Fatalf("%s[%d]: %v", what, i, err)
		}
		out = append(out, v)
	}
	return out
}

// parseMetadata decodes a Metadata response body, asserting on nothing: it
// exists so a test can talk about what a client receives rather than about how
// the encoder was written.
func parseMetadata(t *testing.T, version int16, dec *Decoder) metaResponse {
	t.Helper()
	var resp metaResponse

	if version >= 3 {
		if _, err := dec.Int32(); err != nil { // ThrottleTimeMs
			t.Fatalf("throttle: %v", err)
		}
	}
	n, err := dec.Int32()
	if err != nil {
		t.Fatalf("broker count: %v", err)
	}
	for i := int32(0); i < n; i++ {
		nodeID, err := dec.Int32()
		if err != nil {
			t.Fatalf("broker %d node id: %v", i, err)
		}
		host, err := dec.String()
		if err != nil {
			t.Fatalf("broker %d host: %v", i, err)
		}
		port, err := dec.Int32()
		if err != nil {
			t.Fatalf("broker %d port: %v", i, err)
		}
		if version >= 1 {
			if _, err := dec.String(); err != nil { // rack
				t.Fatalf("broker %d rack: %v", i, err)
			}
		}
		resp.brokers = append(resp.brokers, metaBroker{nodeID: nodeID, host: host, port: port})
	}
	if version >= 2 {
		if _, err := dec.String(); err != nil { // cluster id
			t.Fatalf("cluster id: %v", err)
		}
	}
	if version >= 1 {
		if resp.controller, err = dec.Int32(); err != nil {
			t.Fatalf("controller id: %v", err)
		}
	}

	topicCount, err := dec.Int32()
	if err != nil {
		t.Fatalf("topic count: %v", err)
	}
	for i := int32(0); i < topicCount; i++ {
		code, err := dec.Int16()
		if err != nil {
			t.Fatalf("topic %d error: %v", i, err)
		}
		if code != ErrNone {
			t.Errorf("topic %d error code = %d, want 0", i, code)
		}
		name, err := dec.String()
		if err != nil {
			t.Fatalf("topic %d name: %v", i, err)
		}
		topic := metaTopic{name: name}
		if version >= 1 {
			if _, err := dec.Int8(); err != nil { // is_internal
				t.Fatalf("topic %q is_internal: %v", name, err)
			}
		}
		partCount, err := dec.Int32()
		if err != nil {
			t.Fatalf("topic %q partition count: %v", name, err)
		}
		for j := int32(0); j < partCount; j++ {
			// Kafka's layout: error code first, then the partition number.
			pcode, err := dec.Int16()
			if err != nil {
				t.Fatalf("topic %q partition %d error: %v", name, j, err)
			}
			pid, err := dec.Int32()
			if err != nil {
				t.Fatalf("partition %d id: %v", j, err)
			}
			leader, err := dec.Int32()
			if err != nil {
				t.Fatalf("partition %d leader: %v", pid, err)
			}
			p := metaPartition{id: pid, errorCode: pcode, leader: leader}
			if version >= 7 {
				if p.leaderEpoch, err = dec.Int32(); err != nil {
					t.Fatalf("partition %d leader epoch: %v", pid, err)
				}
			}
			p.replicas = readInt32Array(t, dec, fmt.Sprintf("partition %d replicas", pid))
			p.isr = readInt32Array(t, dec, fmt.Sprintf("partition %d isr", pid))
			if version >= 5 {
				readInt32Array(t, dec, fmt.Sprintf("partition %d offline replicas", pid))
			}
			topic.partitions = append(topic.partitions, p)
		}
		resp.topics = append(resp.topics, topic)
	}
	return resp
}

// ---- tests ----

// The single-agent case must be unchanged by routing: every partition is led by
// this agent, under the node id it reports, and the controller is a broker the
// client can actually resolve.
func TestMetadata_SingleAgentLeadsEverythingItOwns(t *testing.T) {
	se := testStore(t)
	if err := se.CreateTopic("orders", 3); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	cfg := testConfig()
	cfg.NodeID = 42

	dec := dispatch(t, se, cfg, ApiKeyMetadata, 7, metaRequestFor(7, "orders"))
	resp := parseMetadata(t, 7, dec)

	if len(resp.brokers) != 1 || resp.brokers[0].nodeID != 42 {
		t.Fatalf("brokers = %v, want this agent as node 42", resp.brokerNodeIDs())
	}
	if got := resp.brokers[0]; got.host != cfg.AdvertisedHost || got.port != cfg.AdvertisedPort {
		t.Errorf("broker address = %s:%d, want %s:%d", got.host, got.port, cfg.AdvertisedHost, cfg.AdvertisedPort)
	}
	if resp.controller != 42 {
		t.Errorf("controller = %d, want 42", resp.controller)
	}

	topic := resp.topic(t, "orders")
	if len(topic.partitions) != 3 {
		t.Fatalf("partitions = %d, want 3", len(topic.partitions))
	}
	for _, p := range topic.partitions {
		if p.errorCode != ErrNone || p.leader != 42 {
			t.Errorf("partition %d: error=%d leader=%d, want 0/42", p.id, p.errorCode, p.leader)
		}
		if len(p.replicas) != 1 || p.replicas[0] != 42 {
			t.Errorf("partition %d replicas = %v, want [42]", p.id, p.replicas)
		}
	}
}

// v7 is what carries LeaderEpoch. v6 must still decode, because a client that
// negotiates v6 must not be handed four bytes it does not expect.
func TestMetadata_V6AndV7BothDecode(t *testing.T) {
	se := testStore(t)
	if err := se.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	cfg := testConfig()
	cfg.NodeID = 3

	// The engine here has no ownership claims, so it has no epoch to report and
	// v7's field is legitimately zero. The assertion that it carries a real epoch
	// belongs to an owning agent; see TestMetadata_RoutesToTheAgentThatOwnsThePartition.
	v7 := parseMetadata(t, 7, dispatch(t, se, cfg, ApiKeyMetadata, 7, metaRequestFor(7, "orders")))
	if got := v7.topic(t, "orders").partition(t, 0).leaderEpoch; got < 0 {
		t.Errorf("v7 leader epoch = %d, want >= 0", got)
	}

	v6 := parseMetadata(t, 6, dispatch(t, se, cfg, ApiKeyMetadata, 6, metaRequestFor(6, "orders")))
	if len(v6.topic(t, "orders").partitions) != 1 {
		t.Error("the v6 layout no longer round-trips")
	}
}

// An empty topic list means "everything". A topic created a moment ago has to be
// in the answer: before routing this came from local state alone, which is what
// made a restarted agent advertise nothing.
func TestMetadata_AllTopicsIncludesARecentlyCreatedTopic(t *testing.T) {
	se := testStore(t)
	if err := se.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	cfg := testConfig()

	resp := parseMetadata(t, 3, dispatch(t, se, cfg, ApiKeyMetadata, 3, metaRequestFor(3)))
	if len(resp.topic(t, "orders").partitions) != 1 {
		t.Error("an \"all topics\" request must include a topic created a moment ago")
	}
}

// Two agents, one bucket: each must report the other as a broker and attribute
// the partitions it does not own to the agent that does. This is the property
// phase 3 exists to deliver, and the reason it is worth the added machinery.
func TestMetadata_RoutesToTheAgentThatOwnsThePartition(t *testing.T) {
	store := newProtocolCASStore()

	// agent-1 creates the topic and therefore owns both partitions.
	owner := startAgent(t, store, "agent-1", 1)
	if err := owner.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := owner.Append("orders", 0, testBatch(1), 1, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	// agent-2 joins and owns nothing, because agent-1's claims are live.
	other := startAgent(t, store, "agent-2", 2)
	defer other.Close()
	if other.Owns("orders", 0) {
		t.Fatal("agent-2 must not own a partition agent-1 holds")
	}

	waitForRouting(t, other, "agent-1 to appear as a broker", func(s storage.RoutingSnapshot) bool {
		return len(s.Brokers) == 2
	})

	// agent-2's Metadata must describe the cluster, not just itself.
	resp := parseMetadata(t, 7, dispatch(t, other, agentConfig(2), ApiKeyMetadata, 7, metaRequestFor(7, "orders")))
	if len(resp.brokers) != 2 {
		t.Fatalf("brokers = %v, want both agents", resp.brokerNodeIDs())
	}

	topic := resp.topic(t, "orders")
	if len(topic.partitions) != 2 {
		t.Fatalf("partitions = %d, want 2: a client must be told about partitions it does not own", len(topic.partitions))
	}
	for _, p := range topic.partitions {
		if p.errorCode != ErrNone {
			t.Errorf("partition %d error = %d, want 0", p.id, p.errorCode)
		}
		if p.leader != 1 {
			t.Errorf("partition %d leader = %d, want agent-1 (node 1)", p.id, p.leader)
		}
		if p.leaderEpoch < 1 {
			t.Errorf("partition %d leader epoch = %d, want the ownership epoch (>= 1)", p.id, p.leaderEpoch)
		}
		if len(p.replicas) != 1 || p.replicas[0] != 1 {
			t.Errorf("partition %d replicas = %v, want [1]", p.id, p.replicas)
		}
	}
	if resp.controller != 2 {
		t.Errorf("controller = %d; each agent reports itself as controller in this implementation", resp.controller)
	}

	// And agent-1, which does own them, keeps attributing them to itself.
	ownResp := parseMetadata(t, 7, dispatch(t, owner, agentConfig(1), ApiKeyMetadata, 7, metaRequestFor(7, "orders")))
	if p := ownResp.topic(t, "orders").partition(t, 0); p.leader != 1 {
		t.Errorf("agent-1's own view of orders/0 leader = %d, want 1", p.leader)
	}
}

// A wedged owner must stop being advertised. Its liveness record is still valid
// but its routing table has not been refreshed, so its partitions are treated as
// unowned: a client is told there is no leader and retries, rather than being sent
// to a broker that has stopped renewing.
//
// This is the leaderless case that actually occurs. A partition nobody has ever
// claimed cannot arise here, because a live agent claims everything it can see.
func TestMetadata_WedgedOwnerIsReportedAsLeaderless(t *testing.T) {
	store := newProtocolCASStore()

	owner := startAgent(t, store, "agent-1", 1)
	if err := owner.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := owner.Append("orders", 0, testBatch(1), 1, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	other := startAgent(t, store, "agent-2", 2)
	defer other.Close()

	waitForRouting(t, other, "agent-1 to own orders/0", func(s storage.RoutingSnapshot) bool {
		route, ok := s.Owner("orders", 0)
		return ok && route.NodeID == 1
	})

	// agent-1 wedges: its routing table stops being refreshed while its liveness
	// record still has time on it.
	ageRoutingTable(t, store, "agent-1", -time.Hour)

	waitForRouting(t, other, "the wedged agent's partitions to be dropped", func(s storage.RoutingSnapshot) bool {
		_, ok := s.Owner("orders", 0)
		return !ok
	})

	resp := parseMetadata(t, 7, dispatch(t, other, agentConfig(2), ApiKeyMetadata, 7, metaRequestFor(7, "orders")))
	topic := resp.topic(t, "orders")
	if len(topic.partitions) != 1 {
		t.Fatalf("partitions = %d, want 1: an unowned partition still exists", len(topic.partitions))
	}
	p := topic.partitions[0]
	if p.errorCode != ErrLeaderNotAvailable {
		t.Errorf("error = %d, want %d (LEADER_NOT_AVAILABLE)", p.errorCode, ErrLeaderNotAvailable)
	}
	if p.leader != -1 {
		t.Errorf("leader = %d, want -1: Kafka's encoding for no leader", p.leader)
	}
	if len(p.replicas) != 0 || len(p.isr) != 0 {
		t.Errorf("an unowned partition must claim no replicas, got replicas=%v isr=%v", p.replicas, p.isr)
	}

	// The wedged broker must also be gone from the broker list, so a client is
	// not told to connect to it for anything.
	for _, b := range resp.brokers {
		if b.nodeID == 1 {
			t.Errorf("the wedged agent is still advertised as a broker: %+v", b)
		}
	}
}

// A produce for a partition this agent does not own must be refused. Appending
// would assign offsets that collide with the owner's.
func TestProduce_ForAPartitionThisAgentDoesNotOwnIsRefused(t *testing.T) {
	store := newProtocolCASStore()

	owner := startAgent(t, store, "agent-1", 1)
	if err := owner.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := owner.Append("orders", 0, testBatch(1), 1, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	other := startAgent(t, store, "agent-2", 2)
	defer other.Close()

	dec := dispatch(t, other, agentConfig(2), ApiKeyProduce, 3, produceRequestFor("orders", 0, testBatch(1)))
	if _, err := dec.Int32(); err != nil { // topic count
		t.Fatalf("topic count: %v", err)
	}
	if _, err := dec.String(); err != nil { // topic
		t.Fatalf("topic: %v", err)
	}
	if _, err := dec.Int32(); err != nil { // partition count
		t.Fatalf("partition count: %v", err)
	}
	if _, err := dec.Int32(); err != nil { // partition
		t.Fatalf("partition: %v", err)
	}
	code, err := dec.Int16()
	if err != nil {
		t.Fatalf("error code: %v", err)
	}
	if code != ErrNotLeaderForPartition {
		t.Errorf("error code = %d, want %d (NOT_LEADER_OR_FOLLOWER)", code, ErrNotLeaderForPartition)
	}
}

// A fetch for a partition this agent does not own must not be answered from its
// local WAL. Records written before a partition moved away have been replaced by
// the new owner, and a guessed high watermark would make the consumer skip
// records it never read.
func TestFetch_ForAPartitionThisAgentDoesNotOwnIsRefused(t *testing.T) {
	store := newProtocolCASStore()

	owner := startAgent(t, store, "agent-1", 1)
	if err := owner.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err := owner.Append("orders", 0, testBatch(1), 1, true); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	other := startAgent(t, store, "agent-2", 2)
	defer other.Close()

	dec := dispatch(t, other, agentConfig(2), ApiKeyFetch, 5, fetchRequestFor(t, "orders", 0, 0, 1, 0))
	if _, err := dec.Int32(); err != nil { // throttle
		t.Fatalf("throttle: %v", err)
	}
	if _, err := dec.Int32(); err != nil { // topic count
		t.Fatalf("topic count: %v", err)
	}
	if _, err := dec.String(); err != nil { // topic
		t.Fatalf("topic: %v", err)
	}
	if _, err := dec.Int32(); err != nil { // partition count
		t.Fatalf("partition count: %v", err)
	}
	if _, err := dec.Int32(); err != nil { // partition
		t.Fatalf("partition: %v", err)
	}
	code, err := dec.Int16()
	if err != nil {
		t.Fatalf("error code: %v", err)
	}
	if code != ErrLeaderNotAvailable {
		t.Errorf("fetch error = %d, want %d (LEADER_NOT_AVAILABLE)", code, ErrLeaderNotAvailable)
	}
}

// A fetch must not park on a partition it cannot serve. The append latch fires on
// this process's own writes, so waiting on someone else's partition burns the
// whole timeout on every request and then returns nothing.
func TestFetch_DoesNotWaitOnAPartitionItCannotServe(t *testing.T) {
	store := newProtocolCASStore()

	owner := startAgent(t, store, "agent-1", 1)
	if err := owner.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	other := startAgent(t, store, "agent-2", 2)
	defer other.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// minBytes > 0 with a 30s max wait: it must not park, because no append
		// on this agent can ever satisfy this request.
		dispatch(t, other, agentConfig(2), ApiKeyFetch, 5, fetchRequestFor(t, "orders", 0, 0, 1, 30_000))
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the fetch parked on a partition this agent does not own")
	}
}

// The single-agent fetch path must be untouched by all of this.
func TestFetch_OwnedPartitionIsStillServed(t *testing.T) {
	se := testStore(t)
	if err := se.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err := se.Append("orders", 0, testBatch(1), 1, true); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	dec := dispatch(t, se, testConfig(), ApiKeyFetch, 5, fetchRequestFor(t, "orders", 0, 0, 1, 0))
	dec.Int32() // throttle
	dec.Int32() // topic count
	dec.String()
	dec.Int32() // partition count
	dec.Int32() // partition
	code, err := dec.Int16()
	if err != nil {
		t.Fatalf("error code: %v", err)
	}
	if code != ErrNone {
		t.Fatalf("fetch error = %d, want 0", code)
	}
	hw, err := dec.Int64()
	if err != nil {
		t.Fatalf("high watermark: %v", err)
	}
	if hw != 4 {
		t.Errorf("high watermark = %d, want 4", hw)
	}
}

// ListOffsets has to refuse an unowned partition too. Its durable position was
// dropped when the claim moved, so "earliest" would report offset 0 for a log
// that starts far above it -- and a consumer resetting there spins on
// OffsetOutOfRange for ever.
func TestListOffsets_ForAPartitionThisAgentDoesNotOwnIsRefused(t *testing.T) {
	store := newProtocolCASStore()

	owner := startAgent(t, store, "agent-1", 1)
	if err := owner.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := owner.Append("orders", 0, testBatch(1), 1, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	other := startAgent(t, store, "agent-2", 2)
	defer other.Close()

	dec := dispatch(t, other, agentConfig(2), ApiKeyListOffsets, 1, listOffsetsRequestFor("orders", 0, -2))
	if _, err := dec.Int32(); err != nil { // topic count
		t.Fatalf("topic count: %v", err)
	}
	if _, err := dec.String(); err != nil { // topic
		t.Fatalf("topic: %v", err)
	}
	if _, err := dec.Int32(); err != nil { // partition count
		t.Fatalf("partition count: %v", err)
	}
	if _, err := dec.Int32(); err != nil { // partition
		t.Fatalf("partition: %v", err)
	}
	code, err := dec.Int16()
	if err != nil {
		t.Fatalf("error code: %v", err)
	}
	if code != ErrNotLeaderForPartition {
		t.Errorf("ListOffsets error = %d, want %d (NOT_LEADER_OR_FOLLOWER)", code, ErrNotLeaderForPartition)
	}
}

// FindCoordinator has to name a node id that appears in Metadata's broker list,
// or a client is told to coordinate with a broker nobody advertised.
func TestFindCoordinator_NamesAKnownBroker(t *testing.T) {
	se := testStore(t)
	cfg := testConfig()
	cfg.NodeID = 9

	dec := dispatch(t, se, cfg, ApiKeyFindCoordinator, 0, coordinatorRequestFor("group-1"))
	if code, _ := dec.Int16(); code != ErrNone {
		t.Fatalf("FindCoordinator error = %d", code)
	}
	node, err := dec.Int32()
	if err != nil {
		t.Fatalf("node id: %v", err)
	}
	if node != 9 {
		t.Errorf("coordinator node = %d, want this agent's own id 9", node)
	}
	host, _ := dec.String()
	port, _ := dec.Int32()
	if host != cfg.AdvertisedHost || port != cfg.AdvertisedPort {
		t.Errorf("coordinator address = %s:%d, want %s:%d", host, port, cfg.AdvertisedHost, cfg.AdvertisedPort)
	}
	_ = node

	meta := parseMetadata(t, 3, dispatch(t, se, cfg, ApiKeyMetadata, 3, metaRequestFor(3)))
	known := false
	for _, id := range meta.brokerNodeIDs() {
		if id == node {
			known = true
		}
	}
	if !known {
		t.Errorf("the coordinator node %d is not in the broker list %v", node, meta.brokerNodeIDs())
	}
}

// metaRequestFor encodes a Metadata request, including the
// allow_auto_topic_creation byte v6 added.
func metaRequestFor(version int16, topics ...string) []byte {
	enc := NewEncoder()
	enc.Int32(int32(len(topics)))
	for _, tp := range topics {
		enc.String(tp)
	}
	if version >= 6 {
		enc.Int8(1)
	}
	return enc.Bytes()
}

// fetchRequestFor encodes a Fetch request at v5, which has every field the
// ceiling implies: replica id, max wait, min bytes, max bytes, isolation level.
func fetchRequestFor(t *testing.T, topic string, partition int32, offset int64, minBytes, maxWaitMs int32) []byte {
	t.Helper()
	enc := NewEncoder()
	enc.Int32(-1) // ReplicaId
	enc.Int32(maxWaitMs)
	enc.Int32(minBytes)
	enc.Int32(50 * 1024 * 1024) // MaxBytes
	enc.Int8(0)                 // IsolationLevel: read uncommitted
	enc.Int32(1)                // topic count
	enc.String(topic)
	enc.Int32(1) // partition count
	enc.Int32(partition)
	enc.Int64(offset)
	enc.Int32(1 * 1024 * 1024) // partitionMaxBytes
	return enc.Bytes()
}

// ---- request builders ----

// testBatch builds a single-record wrapped RecordBatch, which is what a produce
// request carries on the wire.
func testBatch(count int) []byte {
	const headerLen = 61
	var records []byte
	for i := 0; i < count; i++ {
		records = append(records,
			1,       // record length
			0,       // attributes
			0,       // timestamp delta
			byte(i), // offset delta
			0xFF,    // null key
			0x01, byte('v'),
			0x00, // no headers
		)
	}
	batchLen := headerLen - 12 + len(records)

	buf := make([]byte, 0, 12+batchLen)
	var tmp8 [8]byte
	var tmp2 [2]byte
	binary.BigEndian.PutUint64(tmp8[:], 0) // base offset
	buf = append(buf, tmp8[:]...)
	binary.BigEndian.PutUint32(tmp8[:4], uint32(batchLen))
	buf = append(buf, tmp8[:4]...)
	binary.BigEndian.PutUint32(tmp8[:4], 0xFFFFFFFF) // partition leader epoch
	buf = append(buf, tmp8[:4]...)
	binary.BigEndian.PutUint16(tmp2[:], 2) // magic
	buf = append(buf, tmp2[:]...)
	buf = append(buf, 0, 0, 0, 0) // crc placeholder
	binary.BigEndian.PutUint16(tmp2[:], 0)
	buf = append(buf, tmp2[:]...)
	binary.BigEndian.PutUint32(tmp8[:4], uint32(count-1))
	buf = append(buf, tmp8[:4]...)
	buf = append(buf, tmp8[:]...) // first timestamp
	buf = append(buf, tmp8[:]...) // max timestamp
	binary.BigEndian.PutUint32(tmp8[:4], 0xFFFFFFFF)
	buf = append(buf, tmp8[:4]...) // producer id
	binary.BigEndian.PutUint16(tmp2[:], 0xFFFF)
	buf = append(buf, tmp2[:]...) // producer epoch
	binary.BigEndian.PutUint32(tmp8[:4], 0xFFFFFFFF)
	buf = append(buf, tmp8[:4]...) // base sequence
	binary.BigEndian.PutUint32(tmp8[:4], uint32(count))
	buf = append(buf, tmp8[:4]...) // record count
	buf = append(buf, records...)

	return buf
}

// produceRequestFor encodes a Produce v3 request for one partition.
func produceRequestFor(topic string, partition int32, batch []byte) []byte {
	enc := NewEncoder()
	enc.Int16(-1) // transactional id: null
	enc.Int16(1)  // acks = 1
	enc.Int32(5_000)
	enc.Int32(1) // topic count
	enc.String(topic)
	enc.Int32(1) // partition count
	enc.Int32(partition)
	// The message set is length-prefixed exactly as the request declares it.
	enc.PutBytes(batch)
	return enc.Bytes()
}

func listOffsetsRequestFor(topic string, partition int32, timestamp int64) []byte {
	enc := NewEncoder()
	enc.Int32(-1) // replica id
	enc.Int32(1)  // topic count
	enc.String(topic)
	enc.Int32(1) // partition count
	enc.Int32(partition)
	enc.Int64(timestamp)
	return enc.Bytes()
}

// ageRoutingTable backdates an agent's routing table, which is what an agent
// that has stopped renewing looks like to everybody else.
func ageRoutingTable(t *testing.T, store *casStore, agent string, age time.Duration) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	key := "_agents/" + agent + "/routing"
	raw, ok := store.data[key]
	if !ok {
		t.Fatalf("no routing table at %s", key)
	}
	var table routingTableShape
	if err := json.Unmarshal(raw, &table); err != nil {
		t.Fatalf("routing table at %s does not parse: %v", key, err)
	}
	table.Updated = time.Now().Add(age).Unix()
	body, err := json.Marshal(table)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	store.data[key] = body
	store.ver[key]++
}

// routingTableShape mirrors the published table so a test can rewrite one field.
type routingTableShape struct {
	Agent      string `json:"agent"`
	NodeID     int32  `json:"node_id"`
	Generation int64  `json:"generation"`
	Partitions []struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Epoch     int64  `json:"epoch"`
	} `json:"partitions"`
	Updated int64 `json:"updated_at"`
}

func coordinatorRequestFor(group string) []byte {
	enc := NewEncoder()
	enc.String(group)
	return enc.Bytes()
}
