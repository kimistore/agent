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
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kimistore/internal/metrics"
)

// Everything under _agents/ that is not a checkpoint belongs to the routing
// view (phase 3). Without it an agent owns partitions no client can find: the
// write side is fenced per partition, but nothing tells a client which agent
// holds what, so it never routes there.
//
// The three objects are deliberately different in kind:
//
//   - _agents/<ns>/liveness          who is running, and where to reach them.
//     Written with a compare-and-swap, because two processes sharing an agent id
//     must not both claim to be the same broker.
//   - _agents/<ns>/routing           what this agent owns, with the ownership
//     epoch of each partition. Advisory: only its author writes it, so a plain
//     put is enough and a version conflict would mean nothing useful.
//   - _agents/<ns>/checkpoint.json   the agent's private durable position. Not
//     part of routing, and deliberately skipped when discovering agents.
//
// All three live *under* the agent's prefix rather than at it. Putting the
// liveness record at `_agents/<ns>` would make that key both an object and a
// directory prefix, which object storage tolerates and a filesystem-backed store
// cannot represent at all -- and the local S3 shim the end-to-end test runs
// against is exactly that. One namespace per agent, three objects in it.
const (
	agentLivenessKey = "liveness"
	agentRoutingKey  = "routing"
)

// RoutingRecord is one agent's liveness and reachability.
//
// It is CAS-renewed. That is not about fencing the log -- partition ownership
// does that -- it is about the broker list. Two brokers answering with the same
// node id would send a client to whichever answered last, so a duplicate agent
// id has to be visible rather than silently tolerated.
type RoutingRecord struct {
	Agent   string `json:"agent"`
	NodeID  int32  `json:"node_id"`
	Host    string `json:"host"`
	Port    int32  `json:"port"`
	Updated int64  `json:"updated_at"`
	Expires int64  `json:"expires_at"`
}

// Expired reports whether this agent's liveness has lapsed, judged against now.
func (r RoutingRecord) Expired(now time.Time) bool {
	return r.Expires <= now.Unix()
}

// RoutingPartition is one partition an agent claims.
type RoutingPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Epoch     int64  `json:"epoch"`
}

// RoutingTable is what one agent publishes about what it owns.
//
// Epoch is bumped whenever the set of partitions changes, so a reader can tell a
// newer table from an older one without comparing contents. It is not the
// ownership epoch: those are per partition and are carried on each entry.
type RoutingTable struct {
	Agent      string             `json:"agent"`
	NodeID     int32              `json:"node_id"`
	Generation int64              `json:"generation"`
	Partitions []RoutingPartition `json:"partitions"`
	Updated    int64              `json:"updated_at"`
}

// PartitionRoute is the resolved owner of one partition.
type PartitionRoute struct {
	AgentID string
	NodeID  int32
	Epoch   int64
}

// Broker is one reachable agent, as reported in Metadata.
type Broker struct {
	NodeID int32
	Host   string
	Port   int32
	Agent  string
}

// RoutingSnapshot is an immutable view of the cluster, rebuilt on a timer and
// read by every Metadata request.
//
// It is a snapshot rather than a live read on purpose. Metadata is the request
// every client makes first and periodically thereafter; answering it from object
// storage would put a LIST plus a GET per agent on the client-facing path. The
// cost of the cache is that routing can be up to one refresh interval stale,
// which is the same order as the failover it exists to survive.
type RoutingSnapshot struct {
	// Brokers are the live agents, including this one, ordered by node id so
	// the response is stable and diffable.
	Brokers []Broker
	// Owners maps "topic/partition" to its owner. A partition that exists but
	// is absent here has no live owner.
	Owners map[string]PartitionRoute
	// Partitions is the cluster-wide inventory of topic/partition, including
	// partitions no live agent owns. Metadata has to report those: a client
	// that is not told a partition exists will hash keys onto it and get
	// nowhere, whereas one told it exists with no leader will retry.
	Partitions map[string][]int32
	// ObservedAt is when this snapshot was built.
	ObservedAt time.Time
	// SelfNodeID is the node id this agent attributes its own partitions to.
	// Metadata's fallback broker entry must use the same id, or a client would
	// be handed a leader it cannot look up in the broker list.
	SelfNodeID int32
	// Stale reports that the last refresh failed and this is an older snapshot.
	// Metadata still answers from it: reporting an empty cluster because one
	// LIST timed out would be far worse than answering slightly out of date.
	Stale bool
}

// Owner returns the route for a partition.
func (s RoutingSnapshot) Owner(topic string, partition int32) (PartitionRoute, bool) {
	r, ok := s.Owners[topic+"/"+strconv.Itoa(int(partition))]
	return r, ok
}

// TopicExists reports whether the cluster knows this topic, which is not the
// same question as whether this agent can write to it.
func (s RoutingSnapshot) TopicExists(topic string) bool {
	_, ok := s.Partitions[topic]
	return ok
}

// Topics lists every topic the cluster knows, sorted.
func (s RoutingSnapshot) Topics() []string {
	out := make([]string, 0, len(s.Partitions))
	for t := range s.Partitions {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// PartitionsOf lists a topic's partitions, sorted.
func (s RoutingSnapshot) PartitionsOf(topic string) []int32 {
	parts := append([]int32(nil), s.Partitions[topic]...)
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	return parts
}

// Self returns this agent's own broker entry.
func (s RoutingSnapshot) Self(nodeID int32) (Broker, bool) {
	for _, b := range s.Brokers {
		if b.NodeID == nodeID {
			return b, true
		}
	}
	return Broker{}, false
}

// RegistryConfig describes this agent's identity in the cluster.
type RegistryConfig struct {
	// AgentID is the stable identity claims and routing are namespaced by.
	AgentID string
	// NodeID is what clients see as this broker's id. It has to be stable across
	// restarts and unique across agents, and it is the same value Metadata and
	// FindCoordinator report.
	NodeID int32
	// Host and Port are what clients dial. They must be reachable from the
	// client: a loopback address makes the agent unusable anywhere else.
	Host string
	Port int32
	// TTL is how long a liveness record and a routing table stay valid without
	// renewal, and therefore how long a dead agent stays in the broker list.
	TTL time.Duration
	// Enabled turns publishing and discovery off. An agent with no registry
	// still serves a single-broker view of itself.
	Enabled bool
}

// registry publishes this agent's liveness and routing, and caches the union of
// every live agent's.
//
// It borrows the engine's object-store helpers rather than calling the store
// directly, so every call it makes is bounded by the engine's operation timeout
// and cancelled when the engine closes. A registry with its own, longer bound
// would be able to hold engine construction or shutdown open, which is the
// failure the engine's timeout exists to prevent.
type registry struct {
	engine *StorageEngine
	cfg    RegistryConfig
	// fenced is true when the store can make writes conditional, which liveness
	// renewal needs. Routing tables are advisory and do not.
	fenced bool

	mu   sync.RWMutex
	snap RoutingSnapshot

	// live reports whether the last liveness renewal succeeded. It gates the
	// coordinator role, which is fenced far more tightly than partition
	// ownership: see Live.
	live atomic.Bool

	// generation counts changes to the owned-partition set, so a republished
	// routing table can be recognised as new.
	generation atomic.Int64
	// published remembers the routing table last written, so a republish only
	// happens when the set actually changed. publishedOnce distinguishes "the set
	// is empty and nothing is on disk" from "the set is empty and the empty table
	// is already there": without it an agent that owns nothing would write no
	// routing table at all, and since discovery finds agents *by* their table,
	// it would be invisible in the cluster.
	published     map[string]struct{}
	publishedOnce bool

	quit chan struct{}
	wg   sync.WaitGroup
}

// newRegistry builds the registry and takes a first look at the cluster.
//
// The initial publish is synchronous so a peer can find this agent immediately
// rather than after a refresh interval. A failure is not fatal: the loop retries,
// and until it succeeds this agent simply is not in anyone else's broker list.
func newRegistry(engine *StorageEngine, cfg RegistryConfig, owned func() []RoutingPartition) *registry {
	r := &registry{
		engine:    engine,
		cfg:       cfg,
		published: make(map[string]struct{}),
		quit:      make(chan struct{}),
	}
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultOwnershipTTL
		r.cfg.TTL = cfg.TTL
	}
	_, r.fenced = engine.objStore.(ConditionalObjectStore)

	ctx := engine.closedCtx
	// Assume live until proven otherwise. The first publish below either
	// succeeds, in which case the claim is real, or fails and logs; a store that
	// cannot be written to at startup is caught by the ownership fence, and
	// defaulting to live here keeps a single-agent deployment working while its
	// object store is briefly unavailable.
	r.live.Store(true)

	// An agent with no advertised address cannot be advertised to anyone. Rather
	// than publish a broker entry with an empty host -- which a client would try
	// to dial -- this agent simply stays out of its own view, and the protocol
	// layer falls back to describing itself from its own configuration. That is
	// the behaviour of an engine built without a registry, which is what unit
	// tests and one-shot tools do.
	selfDescribable := cfg.Enabled && cfg.Host != "" && cfg.Port > 0
	if cfg.Enabled && !selfDescribable {
		log.Printf("Routing: no advertised host and port, so this agent will not publish a broker entry; " +
			"set KIMISTORE_ADVERTISED_HOST and KIMISTORE_ADVERTISED_PORT to be routable")
	}

	r.setView(r.buildView(ctx, owned(), selfDescribable))

	if !cfg.Enabled {
		return r
	}

	if err := r.publish(ctx, owned()); err != nil {
		log.Printf("Routing: could not publish this agent's routing table: %v", err)
	}

	r.wg.Add(1)
	go r.loop(owned)
	return r
}

// loop republishes and rediscovers on the same cadence as a partition claim.
func (r *registry) loop(owned func() []RoutingPartition) {
	ctx := r.engine.closedCtx
	// Whether this agent may advertise itself at all. Decided once, at
	// construction, so the loop cannot start claiming to be a broker after the
	// initial view decided it is not.
	selfDescribable := r.cfg.Enabled && r.cfg.Host != "" && r.cfg.Port > 0

	defer r.wg.Done()

	interval := r.cfg.TTL / 3
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.quit:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			partitions := owned()
			if err := r.publish(ctx, partitions); err != nil {
				log.Printf("Routing: could not publish this agent's routing table: %v", err)
			}
			r.setView(r.buildView(ctx, partitions, selfDescribable))
		}
	}
}

// publish writes the liveness record and, when the owned set has changed, the
// routing table.
//
// Liveness goes first and is CAS-written: it is the record that says a broker
// exists, so a stale one must never overwrite a live claim. The routing table is
// a plain put because only this agent writes it.
func (r *registry) publish(ctx context.Context, partitions []RoutingPartition) error {
	now := time.Now()
	record := RoutingRecord{
		Agent:   r.cfg.AgentID,
		NodeID:  r.cfg.NodeID,
		Host:    r.cfg.Host,
		Port:    r.cfg.Port,
		Updated: now.Unix(),
		Expires: now.Add(r.cfg.TTL).Unix(),
	}

	if r.fenced {
		if err := r.writeLiveness(ctx, record); err != nil {
			// One failed renewal is enough to stop coordinating. Unlike
			// partition ownership, which waits out the TTL because a stale writer
			// is still fenced by the epoch, a coordinator that cannot prove it is
			// alive may already have been replaced -- and a second coordinator for
			// one group is the split brain the whole mechanism exists to prevent.
			// The cost is a rebalance on a transient object-store blip, which is
			// the cheaper of the two.
			if wasLive := r.live.Swap(false); wasLive {
				log.Printf("Routing: liveness renewal failed (%v); stepping down as coordinator. "+
					"Group requests are refused until the next successful renewal.", err)
				metrics.AgentNotLive.Inc()
			}
			return err
		}
		r.live.Store(true)
		metrics.AgentNotLive.Set(1)
	} else {
		body, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := r.engine.objPut(ctx, r.livenessKey(), bytes.NewReader(body)); err != nil {
			return fmt.Errorf("routing: could not write %s: %w", r.livenessKey(), err)
		}
	}

	key := r.routingKey()
	if r.routingUnchanged(partitions) {
		r.rememberPublished(partitions)
		return nil
	}
	generation := r.generation.Add(1)
	table := RoutingTable{
		Agent:      r.cfg.AgentID,
		NodeID:     r.cfg.NodeID,
		Generation: generation,
		Partitions: partitions,
		Updated:    now.Unix(),
	}
	body, err := json.Marshal(table)
	if err != nil {
		return err
	}
	if err := r.engine.objPut(ctx, key, bytes.NewReader(body)); err != nil {
		return fmt.Errorf("routing: could not write %s: %w", key, err)
	}
	r.rememberPublished(partitions)
	log.Printf("Routing: published %d partition(s) as %q at node %d (generation %d)",
		len(partitions), r.cfg.AgentID, r.cfg.NodeID, generation)
	return nil
}

// routingUnchanged reports whether the routing table already on disk says exactly
// what this agent owns. Republishing an identical table on every tick would be
// one PUT per agent per interval for no information, and it would make every
// reader's table look freshly written when nothing moved.
func (r *registry) routingUnchanged(partitions []RoutingPartition) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.publishedOnce || len(r.published) != len(partitions) {
		return false
	}
	for _, p := range partitions {
		if _, ok := r.published[routeKey(p.Topic, p.Partition)]; !ok {
			return false
		}
	}
	return true
}

// rememberPublished records the partition set that is now on disk.
func (r *registry) rememberPublished(partitions []RoutingPartition) {
	next := make(map[string]struct{}, len(partitions))
	for _, p := range partitions {
		next[routeKey(p.Topic, p.Partition)] = struct{}{}
	}
	r.mu.Lock()
	r.published = next
	r.publishedOnce = true
	r.mu.Unlock()
}

// writeLiveness CAS-writes this agent's liveness record.
//
// A conflict means another process is publishing the same agent id. That is a
// misconfiguration -- two brokers claiming one identity would answer Metadata
// with the same node id -- so it is logged loudly and the claim is not stolen.
func (r *registry) writeLiveness(ctx context.Context, record RoutingRecord) error {
	key := r.livenessKey()
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// One read serves both purposes: it says whether the record exists, whether
	// another process owns this agent id, and which version the write must name.
	raw, version, found, err := r.engine.objGetVersion(ctx, key)
	if err != nil {
		return fmt.Errorf("routing: could not read %s: %w", key, err)
	}

	if !found {
		if err := r.engine.objPutVersion(ctx, key, body, ""); err != nil {
			return fmt.Errorf("routing: could not create %s: %w", key, err)
		}
		return nil
	}

	var current RoutingRecord
	if json.Unmarshal(raw, &current) != nil {
		// An unreadable record is treated as absent rather than as a permanent
		// block, the same way a corrupt claim is. The cost is one extra renewal
		// cycle; the alternative is a corrupt record wedging the agent out of
		// its own broker list forever.
		if err := r.engine.objPutVersion(ctx, key, body, ""); err != nil && !errors.Is(err, ErrVersionMismatch) {
			return fmt.Errorf("routing: could not replace unreadable %s: %w", key, err)
		}
		return nil
	}
	switch {
	case current.Agent != record.Agent:
		// Somebody else publishes this key, which means two agents were given one
		// identity. Refusing is the only safe answer: both would otherwise answer
		// Metadata with the same broker id.
		log.Printf("Routing: %s is published by %q at node %d but this agent is %q at node %d; "+
			"refusing to take it over -- give each agent a distinct KIMISTORE_AGENT_ID",
			key, current.Agent, current.NodeID, record.Agent, record.NodeID)
		metrics.RoutingPublishConflicts.Inc()
		return nil

	case current.NodeID != record.NodeID && !current.Expired(time.Now()):
		// One identity, two broker ids, and the other one is still live. This is
		// usually a rolling change to KIMISTORE_NODE_ID with the old process still
		// running, and last-writer-wins would make clients flap between two
		// addresses for one broker. Refuse and say so; the fix is to stop the other
		// process, after which this agent takes the record over once it expires.
		log.Printf("Routing: %s is live at node %d but this agent is %q at node %d; "+
			"refusing to overwrite it -- stop the other agent, or give them distinct "+
			"KIMISTORE_AGENT_ID values",
			key, current.NodeID, record.Agent, record.NodeID)
		metrics.RoutingPublishConflicts.Inc()
		return nil
	}

	if err := r.engine.objPutVersion(ctx, key, body, version); err != nil {
		if errors.Is(err, ErrVersionMismatch) {
			// Someone wrote between our read and our write. Next tick will
			// either win or report the conflict.
			return nil
		}
		return fmt.Errorf("routing: could not renew %s: %w", key, err)
	}
	return nil
}

// buildView assembles the cluster view from object storage.
//
// It must never return an empty cluster because of a transient failure, so the
// caller's previous view is kept on error. That is why the error is returned
// rather than swallowed: the caller decides, and the decision is "keep what we
// had and mark it stale".
func (r *registry) buildView(ctx context.Context, own []RoutingPartition, includeSelf bool) RoutingSnapshot {
	now := time.Now()

	snap := RoutingSnapshot{
		Owners:     make(map[string]PartitionRoute),
		Partitions: make(map[string][]int32),
		ObservedAt: now,
		SelfNodeID: r.cfg.NodeID,
	}

	// This agent owns everything it says it owns, always: a partition it has
	// lost is refused at the write path anyway, and advertising a stale claim as
	// authoritative would send clients to a broker that cannot serve them.
	//
	// Ownership and reachability are separate questions. An agent with no
	// advertised address still writes every partition it holds -- it just cannot
	// be dialled for them -- so the owner map is populated either way and only
	// the broker list depends on there being an address to publish.
	for _, p := range own {
		snap.Owners[routeKey(p.Topic, p.Partition)] = PartitionRoute{
			AgentID: r.cfg.AgentID, NodeID: r.cfg.NodeID, Epoch: p.Epoch,
		}
	}
	if includeSelf {
		snap.Brokers = append(snap.Brokers, Broker{
			NodeID: r.cfg.NodeID, Host: r.cfg.Host, Port: r.cfg.Port, Agent: r.cfg.AgentID,
		})
	}

	// Union in what this agent has recovered locally.
	//
	// The object-store inventory is the cluster's view, but it is only written on
	// a checkpoint, so a topic created a moment ago is not in it yet. Without
	// this union an "all topics" Metadata request would come back empty for a
	// topic that exists and that this agent owns, and a client that asks for all
	// topics on connect would be told nothing exists.
	for topic, parts := range r.engine.metadataCache.TopicsSnapshot() {
		for pid := range parts {
			snap.Partitions[topic] = append(snap.Partitions[topic], pid)
		}
	}

	// With no registry there is no cluster to look at: this agent is the whole
	// cluster, and its own recovered inventory is the topic and partition set.
	if !r.cfg.Enabled {
		return r.finish(snap)
	}

	objects, err := r.engine.objList(ctx, agentsPrefix)
	if err != nil {
		return r.keepOnError(fmt.Errorf("routing: could not list %s: %w", agentsPrefix, err), snap)
	}

	// The cluster-wide inventory of what exists.
	//
	// It comes from the per-partition manifest keys rather than from routing
	// tables, because a partition nobody owns appears in no routing table -- and
	// Metadata still has to report it, so the client is told to retry rather than
	// that the topic has no such partition.
	//
	// A failure here is not fatal. The broker list below is what clients need to
	// connect at all; an inventory that is briefly incomplete costs a client one
	// more Metadata round trip, whereas losing the broker list costs it the
	// cluster. So the previous view is kept for the brokers and the inventory is
	// merged with what is already known.
	if manifests, listErr := r.engine.objList(ctx, topicsMetadataPrefix); listErr != nil {
		log.Printf("Routing: could not list %s: %v; the topic and partition inventory is "+
			"incomplete until the next refresh", topicsMetadataPrefix, listErr)
		metrics.RoutingInventoryFailures.Inc()
	} else {
		for _, obj := range manifests {
			if topic, pid, ok := parsePartitionManifestKey(obj.Key); ok {
				snap.Partitions[topic] = append(snap.Partitions[topic], pid)
			}
		}
	}

	ns := agentNamespace(r.cfg.AgentID)
	for _, obj := range objects {
		other, kind := parseAgentKey(obj.Key)
		if kind != agentKindRouting || other == ns {
			continue
		}

		record, ok := r.readLiveAgent(ctx, other, now)
		if !ok {
			continue
		}

		table, ok := r.readRoutingTable(ctx, obj.Key, now)
		if !ok {
			continue
		}
		if table.NodeID != record.NodeID {
			// The liveness record and the table disagree about who this is. One
			// of them is from an agent that has since restarted with a different
			// identity. Trusting the pair would send clients to an address with
			// the wrong node id.
			log.Printf("Routing: %s disagrees with its liveness record (node %d vs %d); ignoring the table",
				obj.Key, table.NodeID, record.NodeID)
			metrics.RoutingInconsistentTables.Inc()
			continue
		}

		snap.Brokers = append(snap.Brokers, Broker{
			NodeID: record.NodeID, Host: record.Host, Port: record.Port, Agent: record.Agent,
		})
		for _, p := range table.Partitions {
			snap.Owners[routeKey(p.Topic, p.Partition)] = PartitionRoute{
				AgentID: table.Agent, NodeID: table.NodeID, Epoch: p.Epoch,
			}
		}
	}

	return r.finish(snap)
}

// finish normalises the assembled view: the inventory is deduped, brokers are
// ordered, and partitions owned by nobody are still listed.
func (r *registry) finish(snap RoutingSnapshot) RoutingSnapshot {
	for topic, parts := range snap.Partitions {
		seen := make(map[int32]bool, len(parts))
		out := make([]int32, 0, len(parts))
		for _, p := range parts {
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
		snap.Partitions[topic] = out
	}

	// A partition this agent owns must appear in the inventory even if no
	// manifest exists for it yet -- a topic created a moment ago, whose first
	// checkpoint has not run.
	for key, route := range snap.Owners {
		if topic, pid, ok := splitRouteKey(key); ok && route.NodeID == r.cfg.NodeID {
			parts := snap.Partitions[topic]
			found := false
			for _, p := range parts {
				if p == pid {
					found = true
					break
				}
			}
			if !found {
				snap.Partitions[topic] = append(parts, pid)
			}
		}
	}

	sort.Slice(snap.Brokers, func(i, j int) bool {
		if snap.Brokers[i].NodeID != snap.Brokers[j].NodeID {
			return snap.Brokers[i].NodeID < snap.Brokers[j].NodeID
		}
		return snap.Brokers[i].Agent < snap.Brokers[j].Agent
	})
	snap.Brokers = dedupeBrokers(snap.Brokers)

	metrics.RoutingBrokers.Set(float64(len(snap.Brokers)))
	metrics.AgentsLive.Set(float64(len(snap.Brokers)))
	return snap
}

// dedupeBrokers collapses agents that report the same node id.
//
// This is the failure the CAS on liveness is meant to make visible. It can still
// happen -- two agents configured with the same KIMISTORE_NODE_ID, or one that
// started before the other's record existed -- and a client given two brokers
// with one id will connect to whichever it heard last. So the duplicate is
// dropped rather than emitted, and reported loudly.
func dedupeBrokers(in []Broker) []Broker {
	out := make([]Broker, 0, len(in))
	byID := make(map[int32]Broker, len(in))
	for _, b := range in {
		if first, clash := byID[b.NodeID]; clash {
			log.Printf("Routing: node id %d is claimed by both %q (%s:%d) and %q (%s:%d); "+
				"serving only the first -- give each agent a distinct KIMISTORE_NODE_ID",
				b.NodeID, first.Agent, first.Host, first.Port, b.Agent, b.Host, b.Port)
			metrics.RoutingDuplicateNodeIDs.Inc()
			continue
		}
		byID[b.NodeID] = b
		out = append(out, b)
	}
	return out
}

// readBody reads one small object whole. Liveness records and routing tables are
// a few hundred bytes; anything larger is a sign the key space is being misused.
func (r *registry) readBody(ctx context.Context, key string) ([]byte, error) {
	rc, err := r.engine.objGet(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

// readLiveAgent reads an agent's liveness record, reporting whether it is live.
func (r *registry) readLiveAgent(ctx context.Context, ns string, now time.Time) (RoutingRecord, bool) {
	body, err := r.readBody(ctx, agentsPrefix+ns+"/"+agentLivenessKey)
	if err != nil {
		return RoutingRecord{}, false
	}
	var record RoutingRecord
	if json.Unmarshal(body, &record) != nil {
		return RoutingRecord{}, false
	}
	if record.Expired(now) {
		return RoutingRecord{}, false
	}
	return record, true
}

// readRoutingTable reads one agent's routing table, rejecting one older than the
// TTL. A live agent republishes every TTL/3, so a table older than the TTL means
// the writer is wedged even though its liveness record still has time on it, and
// its partitions should be treated as unowned rather than served from stale
// routing.
func (r *registry) readRoutingTable(ctx context.Context, key string, now time.Time) (RoutingTable, bool) {
	body, err := r.readBody(ctx, key)
	if err != nil {
		return RoutingTable{}, false
	}
	var table RoutingTable
	if json.Unmarshal(body, &table) != nil {
		return RoutingTable{}, false
	}
	if table.Updated <= 0 || now.Sub(time.Unix(table.Updated, 0)) > r.cfg.TTL {
		return RoutingTable{}, false
	}
	return table, true
}

// keepOnError returns the previous view marked stale, or a self-only view when
// there is no previous one.
func (r *registry) keepOnError(err error, fallback RoutingSnapshot) RoutingSnapshot {
	r.mu.RLock()
	prev := r.snap
	r.mu.RUnlock()

	if len(prev.Brokers) > 0 || len(prev.Owners) > 0 {
		prev.Stale = true
		log.Printf("Routing: keeping the previous view (%d broker(s), age %s): %v",
			len(prev.Brokers), time.Since(prev.ObservedAt).Truncate(time.Second), err)
		return prev
	}
	return fallback
}

func (r *registry) setView(snap RoutingSnapshot) {
	r.mu.Lock()
	r.snap = snap
	r.mu.Unlock()
	metrics.RoutingAgeSeconds.Set(time.Since(snap.ObservedAt).Seconds())
}

// Live reports whether this agent can currently prove it is alive, which is what
// gates the coordinator role.
//
// It is deliberately much stricter than partition ownership. A partition owner
// that loses a claim is still safe, because the epoch in every segment name and
// manifest keeps its writes from colliding. A coordinator has no such token: two
// coordinators for one group assign different partitions to the same consumers
// and neither knows it is wrong. So there is no grace window at all here.
func (r *registry) Live() bool {
	if r == nil || !r.cfg.Enabled {
		// Without a registry there is nothing to renew, so nothing to fail.
		return true
	}
	return r.live.Load()
}

func (r *registry) view() RoutingSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snap
}

// livenessKey is where this agent's liveness record lives.
func (r *registry) livenessKey() string {
	return r.agentPrefix() + agentLivenessKey
}

// agentPrefix is this agent's private namespace under _agents/.
func (r *registry) agentPrefix() string {
	return agentsPrefix + agentNamespace(r.cfg.AgentID) + "/"
}

// routingKey is where this agent's routing table lives.
func (r *registry) routingKey() string {
	return r.agentPrefix() + agentRoutingKey
}

// close stops the publish/discover loop.
func (r *registry) close() {
	if r == nil {
		return
	}
	select {
	case <-r.quit:
	default:
		close(r.quit)
	}
}

// release marks this agent dead so a replacement cluster does not wait out the
// TTL for a broker that is gone.
//
// The record is expired rather than deleted, for the same reason the writer lease
// writes a tombstone: nothing reads this for a monotonic token, but leaving a
// parseable record means a reader never has to distinguish "gone" from "never
// existed", and the next agent to claim the id starts from a clean slate.
func (r *registry) release(ctx context.Context) {
	if r == nil || !r.cfg.Enabled {
		return
	}
	key := r.livenessKey()
	raw, version, found, err := r.engine.objGetVersion(ctx, key)
	if err != nil || !found {
		return
	}
	var current RoutingRecord
	if json.Unmarshal(raw, &current) != nil || current.Agent != r.cfg.AgentID {
		return
	}
	gone := current
	gone.Expires = 0
	gone.Updated = time.Now().Unix()
	body, err := json.Marshal(gone)
	if err != nil {
		return
	}
	if err := r.engine.objPutVersion(ctx, key, body, version); err != nil {
		log.Printf("Routing: could not mark %s dead: %v; it will expire in up to %s", key, err, r.cfg.TTL)
		return
	}
	log.Printf("Routing: %s marked dead at shutdown", key)
}

func routeKey(topic string, partition int32) string {
	return topic + "/" + strconv.Itoa(int(partition))
}

// splitRouteKey is the inverse of routeKey. A Kafka topic name cannot contain
// '/', so splitting on the last one is unambiguous.
func splitRouteKey(key string) (topic string, partition int32, ok bool) {
	i := strings.LastIndex(key, "/")
	if i <= 0 || i == len(key)-1 {
		return "", 0, false
	}
	p, err := strconv.ParseInt(key[i+1:], 10, 32)
	if err != nil {
		return "", 0, false
	}
	return key[:i], int32(p), true
}

// agentKeyKind distinguishes the objects under _agents/ from one another.
type agentKeyKind int

const (
	agentKindUnknown agentKeyKind = iota
	agentKindLiveness
	agentKindRouting
	agentKindCheckpoint
)

// parseAgentKey classifies a key under _agents/.
//
// The three objects sit in one flat prefix, so discovery has to tell them apart
// by shape: `_agents/<ns>` is a liveness record, `_agents/<ns>/routing` a
// routing table, and `_agents/<ns>/checkpoint.json` the agent's private durable
// state. The checkpoint must be skipped -- it is not addressed to anyone.
func parseAgentKey(key string) (namespace string, kind agentKeyKind) {
	rest := strings.TrimPrefix(key, agentsPrefix)
	if rest == key || rest == "" {
		return "", agentKindUnknown
	}
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) != 2 {
		return "", agentKindUnknown
	}
	switch parts[1] {
	case agentLivenessKey:
		return parts[0], agentKindLiveness
	case agentRoutingKey:
		return parts[0], agentKindRouting
	case "checkpoint.json":
		return parts[0], agentKindCheckpoint
	default:
		return "", agentKindUnknown
	}
}
