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
	"encoding/binary"
	"hash/fnv"
	"sort"
	"strconv"
)

// AgentRef is a candidate coordinator, as published in an agent's routing view.
type AgentRef struct {
	Agent  string
	NodeID int32
	Host   string
	Port   int32
}

// PartitionKey is the rendezvous key for a topic partition.
//
// It exists so that the key derivation lives in one place. Assignment and group
// coordination must not drift apart here: both are "hash this string against the
// live set", and a change to one that did not reach the other would be a
// correctness bug in whichever was not updated.
func PartitionKey(topic string, partition int32) string {
	return topic + "/" + strconv.FormatInt(int64(partition), 10)
}

// AssignPartition names the agent a topic partition belongs to, by the same
// rendezvous hashing that picks a group coordinator.
//
// It is the same function for the same reason. Rendezvous rather than modulo
// means a departing agent releases only its own partitions: adding a fourth
// agent to three does not reshuffle the other six, and the sixth partition a
// newcomer takes is the one it scores highest for, not an arbitrary one.
//
// The result is an *admission* decision, not authority. Two agents can disagree
// about the live set, and the object store's claim is what settles it. See
// StorageEngine.assignedTo.
func AssignPartition(topic string, partition int32, agents []AgentRef) (AgentRef, bool) {
	return Rendezvous(PartitionKey(topic, partition), agents)
}

// Rendezvous picks the coordinator for a group from the live agent set, by
// rendezvous (highest random weight) hashing.
//
// It is here rather than in the protocol layer because it is a pure function of
// two inputs that both agents can derive identically: the group id and the live
// set. That identity is the whole safety property. If two agents disagree about
// the live set they may disagree about the coordinator, and a group that then
// has two coordinators holds two independent assignment states -- members get
// different partitions and neither side knows it is wrong.
//
// Rendezvous rather than modulo for a reason that matters operationally: when an
// agent leaves, only the groups it coordinated move. Modulo would reshuffle
// every group in the cluster, so one agent's failure would rebalance every
// consumer group in the deployment. That is more churn than the problem needs,
// and it costs nothing to avoid.
func Rendezvous(groupID string, agents []AgentRef) (AgentRef, bool) {
	if len(agents) == 0 {
		return AgentRef{}, false
	}

	best := agents[0]
	bestScore := rendezvousScore(groupID, best.Agent)
	for _, a := range agents[1:] {
		if s := rendezvousScore(groupID, a.Agent); s > bestScore {
			best, bestScore = a, s
		}
	}
	return best, true
}

// rendezvousScore weights one agent for one group.
//
// The seed is derived rather than random because every agent has to compute the
// same value: a hash seeded from anything local (a process id, a clock) would
// give each agent a different answer and guarantee the split brain. Mixing the
// group into an FNV-1a hash of the agent id, with the two lengths in front so
// ("ab","c") and ("a","bc") cannot collide, is enough: the inputs are
// independent strings and the property wanted is only that the mapping is
// deterministic and reasonably uniform.
func rendezvousScore(groupID, agentID string) uint64 {
	h := fnv.New64a()
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(groupID)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(groupID))
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(agentID)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(agentID))
	return h.Sum64()
}

// IsCoordinator reports whether agentID is the rendezvous winner for a group.
//
// Every agent that receives a group request asks this of its own live set, and
// refuses the request if the answer is not itself. The client is then told
// NOT_COORDINATOR and re-resolves the coordinator, which is the one mechanism
// that keeps two coordinators from coexisting once the views differ.
func IsCoordinator(groupID, agentID string, agents []AgentRef) bool {
	winner, ok := Rendezvous(groupID, agents)
	return ok && winner.Agent == agentID
}

// SortAgents orders a live set deterministically.
//
// Rendezvous does not need the order -- it takes the maximum -- but every agent
// still has to agree on *which* agents are in the set, and a sorted, de-duplicated
// list is the easiest way to make that agreement checkable in a test.
func SortAgents(agents []AgentRef) []AgentRef {
	out := make([]AgentRef, len(agents))
	copy(out, agents)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Agent != out[j].Agent {
			return out[i].Agent < out[j].Agent
		}
		return out[i].NodeID < out[j].NodeID
	})
	return dedupeAgents(out)
}

// dedupeAgents drops repeated agent identities, keeping the first. A duplicated
// agent id would otherwise be weighed twice, and two brokers with one identity is
// a misconfiguration the routing layer already refuses.
func dedupeAgents(in []AgentRef) []AgentRef {
	out := in[:0:0]
	for i, a := range in {
		if i > 0 && a.Agent == in[i-1].Agent {
			continue
		}
		out = append(out, a)
	}
	return out
}
