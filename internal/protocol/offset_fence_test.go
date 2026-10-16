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

	"kimistore/internal/storage"
)

// offsetCommitRequest builds an OffsetCommit v0 body: a group id and one
// (topic, partition, offset) triple.
func offsetCommitRequest(group, topic string, partition int32, offset int64) []byte {
	enc := NewEncoder()
	enc.String(group)
	enc.Int32(1) // one topic
	enc.String(topic)
	enc.Int32(1) // one partition
	enc.Int32(partition)
	enc.Int64(offset)
	enc.String("") // metadata
	return enc.Bytes()
}

// offsetCommitCode runs an OffsetCommit and returns the error code for that
// partition.
func offsetCommitCode(t *testing.T, se *storage.StorageEngine, group, topic string, partition int32, offset int64) int16 {
	t.Helper()
	dec := dispatch(t, se, DefaultServerConfig(), ApiKeyOffsetCommit, 0,
		offsetCommitRequest(group, topic, partition, offset))

	if _, err := dec.Int32(); err != nil { // topic count
		t.Fatalf("OffsetCommit topic count: %v", err)
	}
	if _, err := dec.String(); err != nil { // topic
		t.Fatalf("OffsetCommit topic: %v", err)
	}
	if _, err := dec.Int32(); err != nil { // partition count
		t.Fatalf("OffsetCommit partition count: %v", err)
	}
	gotPart, err := dec.Int32()
	if err != nil {
		t.Fatalf("OffsetCommit partition: %v", err)
	}
	if gotPart != partition {
		t.Fatalf("OffsetCommit answered for partition %d, asked about %d", gotPart, partition)
	}
	code, err := dec.Int16()
	if err != nil {
		t.Fatalf("OffsetCommit error code: %v", err)
	}
	return code
}

// offsetFetchRequest builds an OffsetFetch v1 body naming one partition.
func offsetFetchRequest(group, topic string, partition int32) []byte {
	enc := NewEncoder()
	enc.String(group)
	enc.Int32(1) // one topic
	enc.String(topic)
	enc.Int32(1) // one partition
	enc.Int32(partition)
	return enc.Bytes()
}

// offsetFetchAnswer runs an OffsetFetch and returns the committed offset and the
// error code for that partition.
func offsetFetchAnswer(t *testing.T, se *storage.StorageEngine, group, topic string, partition int32) (int64, int16) {
	t.Helper()
	dec := dispatch(t, se, DefaultServerConfig(), ApiKeyOffsetFetch, 1,
		offsetFetchRequest(group, topic, partition))

	if _, err := dec.Int32(); err != nil { // topic count
		t.Fatalf("OffsetFetch topic count: %v", err)
	}
	if _, err := dec.String(); err != nil { // topic
		t.Fatalf("OffsetFetch topic: %v", err)
	}
	if _, err := dec.Int32(); err != nil { // partition count
		t.Fatalf("OffsetFetch partition count: %v", err)
	}
	if _, err := dec.Int32(); err != nil { // partition
		t.Fatalf("OffsetFetch partition: %v", err)
	}
	offset, err := dec.Int64()
	if err != nil {
		t.Fatalf("OffsetFetch offset: %v", err)
	}
	_, _ = dec.String() // metadata
	code, err := dec.Int16()
	if err != nil {
		t.Fatalf("OffsetFetch error code: %v", err)
	}
	return offset, code
}

// TestOffsetCommit_OnlyThePartitionOwnerAccepts is the fence for §14.5.
//
// Every group's offsets live in one flat `_offsets/` prefix shared by every
// agent. Both offset handlers used to bypass coordinatorFence, so any agent in
// the fleet accepted any commit and wrote the key last-writer-wins -- which
// silently rewinds whichever consumer was relying on the value it overwrote.
//
// Kafka routes OffsetCommit to the partition leader rather than the group
// coordinator, and so does this: NOT_LEADER_OR_FOLLOWER is retriable and sends
// the client to the agent that holds the claim.
func TestOffsetCommit_OnlyThePartitionOwnerAccepts(t *testing.T) {
	store := newProtocolCASStore()
	first := startAgent(t, store, "agent-1", 1)
	defer first.Close()
	second := startAgent(t, store, "agent-2", 2)
	defer second.Close()

	waitForRouting(t, second, "both agents to appear", func(s storage.RoutingSnapshot) bool { return len(s.Brokers) == 2 })
	waitForRouting(t, first, "both agents to appear", func(s storage.RoutingSnapshot) bool { return len(s.Brokers) == 2 })

	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Ask the engine which agent holds the claim rather than assuming. Claims
	// are taken lazily and a race is possible, and the property under test is
	// "only the owner accepts", not "the first agent started wins".
	owner, other := first, second
	if second.Owns("orders", 0) {
		owner, other = second, first
	}
	if !owner.Owns("orders", 0) {
		t.Fatal("neither agent claims orders/0; the test cannot tell owner from non-owner")
	}
	if other.Owns("orders", 0) {
		t.Fatal("both agents claim orders/0; ownership itself is broken")
	}

	const group = "mimir-ingesters"

	if code := offsetCommitCode(t, owner, group, "orders", 0, 4242); code != ErrNone {
		t.Errorf("OffsetCommit at the partition owner = %d, want %d (the owner must accept)", code, ErrNone)
	}

	code := offsetCommitCode(t, other, group, "orders", 0, 999999)
	if code != ErrNotLeaderForPartition {
		t.Errorf("OffsetCommit at a non-owner = %d, want %d (NOT_LEADER_OR_FOLLOWER); "+
			"an unfenced commit overwrites the owner's offset", code, ErrNotLeaderForPartition)
	}

	// The refusal must not have landed. This is the actual damage: the owner
	// committed 4242, a peer committed 999999, and whoever wrote last won.
	got, err := owner.LoadOffset(group, "orders", 0)
	if err != nil {
		t.Fatalf("LoadOffset: %v", err)
	}
	if got != 4242 {
		t.Errorf("stored offset = %d, want 4242; a commit refused by the fence still "+
			"overwrote the owner's value (999999)", got)
	}
}

// TestOffsetFetch_OnlyTheCoordinatorAnswers is the other half of §14.5.
//
// OffsetFetch goes to the group coordinator, not the partition leader, so it
// gets the coordinator fence. The failure it prevents is the expensive one: a
// non-coordinator answers -1, the client reads that as "no committed offset",
// applies auto.offset.reset, and replays the log from the beginning.
func TestOffsetFetch_OnlyTheCoordinatorAnswers(t *testing.T) {
	store := newProtocolCASStore()
	first := startAgent(t, store, "agent-1", 1)
	defer first.Close()
	second := startAgent(t, store, "agent-2", 2)
	defer second.Close()

	waitForRouting(t, second, "both agents to appear", func(s storage.RoutingSnapshot) bool { return len(s.Brokers) == 2 })
	waitForRouting(t, first, "both agents to appear", func(s storage.RoutingSnapshot) bool { return len(s.Brokers) == 2 })

	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Find a group where the two agents disagree about the coordinator, so the
	// test exercises a real split rather than one where both happen to agree.
	const group = "mimir-ingesters"
	var coord, other *storage.StorageEngine
	if first.IsCoordinator(group) {
		coord, other = first, second
	} else if second.IsCoordinator(group) {
		coord, other = second, first
	} else {
		t.Fatal("neither agent believes it coordinates the group; cannot test the fence")
	}

	// Seed a committed offset from the owner so the coordinator has something
	// real to return.
	owner, _ := first, second
	if !first.Owns("orders", 0) {
		owner = second
	}
	if err := owner.SaveOffset(group, "orders", 0, 777); err != nil {
		t.Fatalf("seed offset: %v", err)
	}

	offset, code := offsetFetchAnswer(t, coord, group, "orders", 0)
	if code != ErrNone {
		t.Errorf("OffsetFetch at the coordinator = %d, want %d", code, ErrNone)
	}
	if offset != 777 {
		t.Errorf("coordinator answered offset %d, want 777", offset)
	}

	offset, code = offsetFetchAnswer(t, other, group, "orders", 0)
	if code != ErrNotCoordinator {
		t.Errorf("OffsetFetch at a non-coordinator = %d, want %d (NOT_COORDINATOR)", code, ErrNotCoordinator)
	}
	if offset != -1 {
		t.Errorf("a refused OffsetFetch answered offset %d, want -1; a client that "+
			"ignores the error code would treat it as a real position", offset)
	}
}

// TestOffsetFetch_WholeGroupAtANonCoordinatorIsRefused covers the other
// response shape. A count of zero means "every topic this group has an offset
// for", which is how librdkafka and Mimir's ingester ask for the group
// position, and it is the path that produced the full replay on knode3.
func TestOffsetFetch_WholeGroupAtANonCoordinatorIsRefused(t *testing.T) {
	store := newProtocolCASStore()
	first := startAgent(t, store, "agent-1", 1)
	defer first.Close()
	second := startAgent(t, store, "agent-2", 2)
	defer second.Close()

	waitForRouting(t, second, "both agents to appear", func(s storage.RoutingSnapshot) bool { return len(s.Brokers) == 2 })
	waitForRouting(t, first, "both agents to appear", func(s storage.RoutingSnapshot) bool { return len(s.Brokers) == 2 })

	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	const group = "mimir-ingesters"
	var coord, other *storage.StorageEngine
	if first.IsCoordinator(group) {
		coord, other = first, second
	} else {
		coord, other = second, first
	}
	if err := coord.SaveOffset(group, "orders", 0, 321); err != nil {
		t.Fatalf("seed offset: %v", err)
	}

	enc := NewEncoder()
	enc.String(group)
	enc.Int32(0) // every topic
	dec := dispatch(t, other, DefaultServerConfig(), ApiKeyOffsetFetch, 1, enc.Bytes())

	topics, err := dec.Int32()
	if err != nil {
		t.Fatalf("topic count: %v", err)
	}
	for i := int32(0); i < topics; i++ {
		if _, err := dec.String(); err != nil {
			t.Fatalf("topic: %v", err)
		}
		parts, err := dec.Int32()
		if err != nil {
			t.Fatalf("partition count: %v", err)
		}
		for j := int32(0); j < parts; j++ {
			if _, err := dec.Int32(); err != nil {
				t.Fatalf("partition: %v", err)
			}
			offset, _ := dec.Int64()
			_, _ = dec.String()
			code, _ := dec.Int16()
			if code != ErrNotCoordinator {
				t.Errorf("whole-group OffsetFetch at a non-coordinator = %d, want %d (NOT_COORDINATOR)", code, ErrNotCoordinator)
			}
			if offset != -1 {
				t.Errorf("a refused whole-group fetch answered offset %d, want -1", offset)
			}
		}
	}
}
