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
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"kimistore/internal/coordinator"
	"kimistore/internal/metrics"
)

// Partition assignment decides which agent *should* own a partition, so that a
// fleet shares the work instead of every agent racing for everything.
//
// Without it, partition claims alone are a race that the first agent to start
// wins outright. Three agents and six partitions gives one agent all six and two
// idle agents: scaling out adds cost and failure surface without adding
// throughput. Assignment fixes that, at the price of ownership moving when the
// live set changes.
//
// Assignment is an admission gate, never an authority. Two agents refresh their
// view of the live set independently, so they can compute different winners for
// the same partition. The claim in the object store settles it: whoever wins the
// compare-and-swap owns the partition, and the other is refused with
// NOT_LEADER_OR_FOLLOWER. Assignment only bounds who is allowed to try, so a
// disagreement costs one failed claim rather than two writers.
//
// Grace policy is deliberately the ownership grace, not the coordinator's
// stricter one. A coordinator has no fencing token, so two of them is silently
// corrupt and it refuses on the first failed liveness renewal. A partition owner
// does have one: the epoch is in every segment name and in the manifest, so a
// writer that has lost its claim cannot corrupt the new owner's data. That is
// the same reasoning registry.Live already records, and it is why the live set
// here is allowed to be a little behind.

// AssignmentConfig is the engine-side shape of partition assignment.
type AssignmentConfig struct {
	Enabled bool
	// Settle is how long the live set must hold still before this agent
	// rebalances.
	Settle time.Duration
}

// WithAssignment turns deterministic partition assignment on.
func WithAssignment(cfg AssignmentConfig) Option {
	return func(se *StorageEngine) {
		se.assignment = cfg
	}
}

// assignedTo reports whether this agent should own a partition, given a live
// agent set.
//
// It returns true when assignment is off, when the live set is unusable, or
// when there is nobody else to share with. Those three cases all mean the same
// thing: never give up a partition on the strength of a view this agent does not
// trust. Releasing on a bad view is how an agent would hand its entire log to
// nobody.
func (s *StorageEngine) assignedTo(live []coordinator.AgentRef, topic string, partition int32) bool {
	if !s.assignment.Enabled {
		return true
	}
	// One agent owns everything, and so did it before assignment existed.
	// Releasing here would empty the cluster.
	if len(live) <= 1 {
		return true
	}

	winner, ok := coordinator.AssignPartition(topic, partition, live)
	if !ok {
		return true
	}
	return winner.Agent == s.agentID
}

// assignmentAgents is the live set used for a claim admission decision.
//
// Unlike the rebalance path, an untrusted view is not fatal here: the worst a
// stale set can do is admit this agent to a claim it will lose at the
// compare-and-swap, which is the failure mode the gate exists to make
// harmless. The opposite error would be refusing a partition this agent should
// own, which would leave a gap nobody fills.
func (s *StorageEngine) assignmentAgents() []coordinator.AgentRef {
	live, ok := s.assignmentView()
	if !ok {
		return nil
	}
	return live
}

// acquireAssigned claims the partitions this agent is assigned but does not hold.
//
// Release without acquire leaves the fleet briefly ownerless: the partition is
// handed over and then sits with no owner until a producer happens to send to the
// right broker, at which point Metadata reports a leaderless partition and the
// client retries. A rebalance has to close both halves in one pass.
//
// Discovery comes from the routing snapshot's partition inventory, which lists
// every partition in the cluster including ones this agent has never touched.
// Without assignment the engine only ever learns a partition by producing to it,
// which is too late to claim it.
func (s *StorageEngine) acquireAssigned(ctx context.Context, live []coordinator.AgentRef) int {
	if s.ownership == nil || !s.ownership.fenced {
		return 0
	}

	inventory := s.registry.view().Partitions
	topics := make([]string, 0, len(inventory))
	for topic := range inventory {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	acquired := 0
	for _, topic := range topics {
		for _, partition := range inventory[topic] {
			if s.ownership.Owns(topic, partition) {
				continue
			}
			if !s.assignedTo(live, topic, partition) {
				continue
			}
			if _, err := s.claimPartition(ctx, topic, partition); err != nil {
				// A held claim means a peer still owns it, or this agent has not
				// won the race. Either way the next pass tries again.
				continue
			}
			if s.ownership.Owns(topic, partition) {
				acquired++
				metrics.AssignmentAcquired.Inc()
				log.Printf("Assignment: claimed %s/%d, assigned to this agent at epoch %d",
					topic, partition, s.partitionEpoch(topic, partition))
			}
		}
	}
	return acquired
}

// assignmentView is the live set used for a rebalance decision, and whether it
// can be trusted to justify releasing a partition.
func (s *StorageEngine) assignmentView() ([]coordinator.AgentRef, bool) {
	if s.registry == nil {
		return nil, false
	}
	snap := s.registry.view()
	if snap.Stale {
		// The last refresh failed. This snapshot may name an agent that has since
		// been replaced, and acting on it could release a partition to an agent
		// that does not exist.
		return nil, false
	}
	out := make([]coordinator.AgentRef, 0, len(snap.Brokers))
	for _, b := range snap.Brokers {
		out = append(out, coordinator.AgentRef{Agent: b.Agent, NodeID: b.NodeID, Host: b.Host, Port: b.Port})
	}
	return coordinator.SortAgents(out), true
}

// settleTracker watches the live set for the Settle window.
//
// The window exists because a rolling deploy changes the live set once per
// agent. Without it, each agent would see a membership change, give up the
// partitions it no longer wins, and the next agent would come up and find a
// partition it is not assigned -- producing a continuous churn of handovers in
// which nothing is ever stable.
type settleTracker struct {
	mu     sync.Mutex
	last   string
	change time.Time
}

func (t *settleTracker) stable(set []string, settle time.Duration, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	joined := strings.Join(set, ",")
	if t.last == "" || joined != t.last {
		t.last = joined
		t.change = now
		return false
	}
	return now.Sub(t.change) >= settle
}

// rebalanceLoop gives up the partitions this agent no longer wins.
//
// Releasing is the half that matters. Refusing to acquire keeps an agent from
// taking a partition it should not have, but without a matching release the
// first agent to start keeps everything and assignment reduces to a slower way
// of doing what the race already did.
//
// A release uses DrainPartition, so the tail is made durable, the manifest is
// written, and the claim is released in that order. A partition whose tail is
// not durable keeps its claim, which is a slower failover rather than a lossy
// one.
func (s *StorageEngine) rebalanceLoop(ctx context.Context) {
	defer s.wg.Done()

	interval := s.assignment.rebalanceInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	settle := &settleTracker{}

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.quit:
			return
		case <-ticker.C:
		}

		live, ok := s.assignmentView()
		if !ok {
			continue
		}

		names := make([]string, 0, len(live))
		for _, a := range live {
			names = append(names, a.Agent)
		}
		sort.Strings(names)

		if !settle.stable(names, s.assignment.Settle, time.Now()) {
			log.Printf("Assignment: live set changed, holding off for %s before rebalancing", s.assignment.Settle)
			continue
		}

		s.releaseUnassigned(ctx, live)
		s.acquireAssigned(ctx, live)
	}
}

func (c AssignmentConfig) rebalanceInterval() time.Duration {
	// Check more often than the settle window, so a change is acted on shortly
	// after the window closes rather than a whole window later.
	d := c.Settle / 2
	if d < time.Second {
		d = time.Second
	}
	return d
}

// releaseUnassigned drains and releases every partition the assignment no longer
// gives this agent.
func (s *StorageEngine) releaseUnassigned(ctx context.Context, live []coordinator.AgentRef) {
	if s.ownership == nil || !s.ownership.fenced {
		return
	}

	var excess []PartitionRef
	for _, key := range s.OwnedPartitions() {
		topic, partition, err := splitOwnedPartitionKey(key)
		if err != nil {
			continue
		}
		if !s.assignedTo(live, topic, partition) {
			excess = append(excess, PartitionRef{Topic: topic, Partition: partition})
		}
	}
	if len(excess) == 0 {
		return
	}

	sort.Slice(excess, func(i, j int) bool {
		if excess[i].Topic != excess[j].Topic {
			return excess[i].Topic < excess[j].Topic
		}
		return excess[i].Partition < excess[j].Partition
	})

	log.Printf("Assignment: %d of %d held partition(s) are now assigned elsewhere; draining them",
		len(excess), len(s.OwnedPartitions()))

	for _, ref := range excess {
		winner, _ := coordinator.AssignPartition(ref.Topic, ref.Partition, live)

		// Bound each handover, and give up on the rest once the budget is gone,
		// because a release that overran would be cut off mid-drain by the very
		// supervisor that started the rebalance.
		remaining := DefaultHandoverBudget
		if ctx.Err() != nil {
			return
		}
		report, err := s.DrainPartition(ctx, ref.Topic, ref.Partition, remaining)
		if err != nil {
			// The partition keeps its claim. That is the safe direction: the
			// partition stays writable here and the next agent picks it up when
			// the claim expires.
			log.Printf("Assignment: keeping %s/%d (assigned to %q): %v",
				ref.Topic, ref.Partition, winner.Agent, err)
			continue
		}

		metrics.AssignmentReleased.Inc()
		log.Printf("Assignment: released %s/%d to %q at epoch %d, log end %d, durable in %s",
			ref.Topic, ref.Partition, winner.Agent, report.Epoch, report.Tail,
			report.Elapsed.Truncate(time.Millisecond))
	}
}

// rebalanceNow runs one rebalance pass synchronously. It exists for tests and
// for an operator who does not want to wait for the next tick.
func (s *StorageEngine) rebalanceNow(ctx context.Context) (int, error) {
	live, ok := s.assignmentView()
	if !ok {
		return 0, fmt.Errorf("assignment: the live agent view is stale, refusing to rebalance")
	}

	before := len(s.OwnedPartitions())
	s.releaseUnassigned(ctx, live)
	s.acquireAssigned(ctx, live)
	return before - len(s.OwnedPartitions()), nil
}
