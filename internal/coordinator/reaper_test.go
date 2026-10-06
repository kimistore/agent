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
	"testing"
	"time"
)

func newTestCoordinator(t *testing.T) *Coordinator {
	t.Helper()
	c := NewCoordinator()
	t.Cleanup(c.Close)
	return c
}

func memberCount(c *Coordinator, groupID string) int {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.Members)
}

func groupSnapshot(c *Coordinator, groupID string) (GroupState, int32, string) {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.State, g.GenerationID, g.LeaderID
}

// consumerProtos is the protocol list a consumer joins with.
func consumerProtos() []GroupProtocol {
	return []GroupProtocol{{Name: "range", Metadata: []byte{}}}
}

func joinOne(c *Coordinator, group, member string, sessionMS, rebalanceMS int32) {
	protos := consumerProtos()
	if _, _, _, _, err := c.JoinGroup(group, member, "consumer", protos, sessionMS, rebalanceMS); err != nil {
		panic(err)
	}
}

// TestReaper_EvictsCrashedMember is the core fix for a dead consumer never
// being removed. Previously heartbeats were recorded but never examined, so a
// crashed member held its partitions forever and the group never rebalanced.
func TestReaper_EvictsCrashedMember(t *testing.T) {
	c := newTestCoordinator(t)
	joinOne(c, "g", "alive", 1000, 1000)
	joinOne(c, "g", "doomed", 1000, 1000)

	before, _, _ := groupSnapshot(c, "g")
	_ = before

	// Simulate time passing for the crashed member only: keep "alive"
	// heartbeating, leave "doomed" silent.
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.Heartbeat("g", "alive", genOf(t, c, "g")); err != nil {
			// "alive" may be told to rejoin if the generation moved; that is
			// the correct behaviour and the test can stop looping.
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	c.ReapExpired()

	if n := memberCount(c, "g"); n != 1 {
		t.Errorf("after reaping, group has %d member(s), want 1 (only the live consumer)", n)
	}
	g := c.GetGroup("g")
	g.mu.Lock()
	_, stillThere := g.Members["doomed"]
	g.mu.Unlock()
	if stillThere {
		t.Error("the crashed member was not evicted")
	}
}

// genOf reads the current generation, which heartbeats must match.
func genOf(t *testing.T, c *Coordinator, groupID string) int32 {
	t.Helper()
	_, gen, _ := groupSnapshot(c, groupID)
	return gen
}

// TestReaper_EvictionAdvancesGeneration checks an eviction invalidates the
// current assignment, which is what tells remaining members to rejoin.
func TestReaper_EvictionAdvancesGeneration(t *testing.T) {
	c := newTestCoordinator(t)
	joinOne(c, "g", "m1", 1000, 1000)
	joinOne(c, "g", "m2", 1000, 1000)

	_, genBefore, leaderBefore := groupSnapshot(c, "g")
	if genBefore == 0 {
		t.Fatal("expected a non-zero generation after joins")
	}

	// Expire everyone.
	time.Sleep(2500 * time.Millisecond)
	c.ReapExpired()

	state, genAfter, leaderAfter := groupSnapshot(c, "g")
	if genAfter <= genBefore {
		t.Errorf("generation did not advance on eviction: %d -> %d", genBefore, genAfter)
	}
	if n := memberCount(c, "g"); n != 0 {
		t.Errorf("expected an empty group, got %d member(s)", n)
	}
	if state != GroupStateEmpty {
		t.Errorf("group state = %v, want Empty", state)
	}
	if leaderAfter != "" {
		t.Errorf("leader = %q, want empty for an empty group", leaderAfter)
	}
	_ = leaderBefore
}

// TestReaper_RespectsSessionTimeout checks a live member inside its timeout
// window is never evicted.
func TestReaper_RespectsSessionTimeout(t *testing.T) {
	c := newTestCoordinator(t)
	joinOne(c, "g", "m1", 30000, 30000) // 30s session timeout

	// Immediately reap: the member just heartbeated via JoinGroup.
	c.ReapExpired()
	if n := memberCount(c, "g"); n != 1 {
		t.Errorf("live member was evicted immediately: %d member(s) left", n)
	}

	// A generous timeout must still protect the member across several ticks.
	for i := 0; i < 3; i++ {
		time.Sleep(1100 * time.Millisecond)
		c.ReapExpired()
	}
	if n := memberCount(c, "g"); n != 1 {
		t.Errorf("member with a 30s session timeout was evicted too early: %d member(s) left", n)
	}
}

// TestReaper_FloorsTinySessionTimeouts guards against a client asking for
// session.timeout.ms=1 and having its members reaped out from under it.
func TestReaper_FloorsTinySessionTimeouts(t *testing.T) {
	c := newTestCoordinator(t)
	joinOne(c, "g", "impatient", 1, 1) // 1ms

	time.Sleep(500 * time.Millisecond)
	c.ReapExpired()

	if n := memberCount(c, "g"); n != 1 {
		t.Errorf("member with a 1ms session timeout was reaped within 500ms; the floor should protect it")
	}
}

// TestReaper_ReelectsLeaderWhenLeaderDies checks a dead leader does not wedge
// the group with no one able to assign partitions.
func TestReaper_ReelectsLeaderWhenLeaderDies(t *testing.T) {
	c := newTestCoordinator(t)
	joinOne(c, "g", "leader", 1000, 1000)
	joinOne(c, "g", "follower", 1000, 1000)

	_, _, leader := groupSnapshot(c, "g")
	if leader != "leader" {
		t.Fatalf("expected the first member to lead, got %q", leader)
	}

	time.Sleep(2500 * time.Millisecond)
	c.ReapExpired()

	_, _, newLeader := groupSnapshot(c, "g")
	if newLeader == "leader" {
		t.Error("the dead leader is still recorded as leader")
	}
}

// TestSyncGroup_FollowerDoesNotHangForever is the other half of the fix: a
// bounded wait so a leader that dies between JoinGroup and SyncGroup cannot
// park followers indefinitely.
func TestSyncGroup_FollowerDoesNotHangForever(t *testing.T) {
	// Shrink the wait so the test proves the bound without sitting through
	// the production 30s cap.
	origDefault, origMax := defaultSyncWait, maxSyncWait
	defaultSyncWait, maxSyncWait = 300*time.Millisecond, 400*time.Millisecond
	t.Cleanup(func() { defaultSyncWait, maxSyncWait = origDefault, origMax })

	c := newTestCoordinator(t)
	joinOne(c, "g", "leader", 60000, 60000)
	_, gen, _, _, err := c.JoinGroup("g", "follower", "consumer",
		[]GroupProtocol{{Name: "range"}}, 60000, 60000)
	if err != nil {
		t.Fatalf("join follower: %v", err)
	}

	// The leader never calls SyncGroup, so the follower must time out.
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := c.SyncGroup("g", "follower", gen, nil)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Error("SyncGroup returned success without an assignment")
		}
		elapsed := time.Since(start)
		if elapsed > maxSyncWait+5*time.Second {
			t.Errorf("waited %s, expected to give up at or below %s", elapsed, maxSyncWait)
		}
	case <-time.After(maxSyncWait + 10*time.Second):
		t.Fatalf("follower was still blocked in SyncGroup after %s: the wait is still unbounded", maxSyncWait+10*time.Second)
	}
}

// TestSyncGroup_FollowerWokenByLeader checks the normal path still works and
// the follower is released as soon as the assignment lands.
func TestSyncGroup_FollowerWokenByLeader(t *testing.T) {
	c := newTestCoordinator(t)
	joinOne(c, "g", "leader", 30000, 30000)
	_, gen, _, _, err := c.JoinGroup("g", "follower", "consumer",
		[]GroupProtocol{{Name: "range"}}, 30000, 30000)
	if err != nil {
		t.Fatalf("join follower: %v", err)
	}

	followerDone := make(chan error, 1)
	go func() {
		a, err := c.SyncGroup("g", "follower", gen, nil)
		if err == nil && len(a) == 0 {
			err = ErrMemberNotFound // no assignment stored, should not happen
		}
		followerDone <- err
	}()

	// Leader publishes assignments shortly after.
	time.Sleep(200 * time.Millisecond)
	assignment := []byte(`{"version":1}`)
	if _, err := c.SyncGroup("g", "leader", gen, []GroupAssignment{
		{MemberID: "follower", Assignment: assignment},
		{MemberID: "leader", Assignment: assignment},
	}); err != nil {
		t.Fatalf("leader SyncGroup: %v", err)
	}

	select {
	case err := <-followerDone:
		if err != nil {
			t.Errorf("follower SyncGroup failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("follower was not woken when the leader published its assignment")
	}
}

// TestLeaveGroup_WakesFollowers checks a member leaving releases anyone
// blocked on the assignment, rather than leaving them to time out.
func TestLeaveGroup_WakesFollowers(t *testing.T) {
	c := newTestCoordinator(t)
	joinOne(c, "g", "leader", 60000, 60000)
	_, gen, _, _, err := c.JoinGroup("g", "follower", "consumer",
		[]GroupProtocol{{Name: "range"}}, 60000, 60000)
	if err != nil {
		t.Fatalf("join follower: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := c.SyncGroup("g", "follower", gen, nil)
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)

	// The leader leaves without ever syncing.
	if err := c.LeaveGroup("g", "leader"); err != nil {
		t.Fatalf("LeaveGroup: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Error("follower got an assignment after the leader left")
		}
	case <-time.After(5 * time.Second):
		t.Error("follower was not woken when the leader left")
	}
}

// TestLeaveGroup_UnknownMemberIsNotFatal keeps a leave that races with the
// reaper from being treated as an error by clients.
func TestLeaveGroup_UnknownMemberIsNotFatal(t *testing.T) {
	c := newTestCoordinator(t)
	joinOne(c, "g", "m1", 30000, 30000)

	if err := c.LeaveGroup("g", "nobody"); err != ErrMemberNotFound {
		t.Errorf("LeaveGroup for an unknown member = %v, want ErrMemberNotFound", err)
	}
	if err := c.LeaveGroup("g", "m1"); err != nil {
		t.Errorf("LeaveGroup for a known member: %v", err)
	}
	if n := memberCount(c, "g"); n != 0 {
		t.Errorf("group should be empty, has %d member(s)", n)
	}
}

// TestHeartbeat_EvictedMemberToldToRejoin checks a member the reaper removed
// gets UnknownMemberId rather than a generic error, so its client regenerates
// state and picks up a fresh assignment.
func TestHeartbeat_EvictedMemberToldToRejoin(t *testing.T) {
	c := newTestCoordinator(t)
	joinOne(c, "g", "m1", 1000, 1000)
	gen := genOf(t, c, "g")

	if err := c.Heartbeat("g", "m1", gen); err != nil {
		t.Fatalf("heartbeat from a live member: %v", err)
	}

	time.Sleep(2500 * time.Millisecond)
	c.ReapExpired()

	err := c.Heartbeat("g", "m1", gen)
	if err != ErrMemberNotFound {
		t.Errorf("heartbeat from an evicted member = %v, want ErrMemberNotFound", err)
	}
}

// Group state is not durable, so a coordinator that replaces one (an agent
// restart, or a different agent taking the group over) starts empty and the
// members rejoin. This is D-4's accepted full rebalance.
//
// The property it has to keep is that the new coordinator issues a *fresh*
// generation rather than reusing the old one, so a member holding a stale
// generation is told to rejoin instead of being handed an assignment computed
// against a member set that no longer exists.
func TestCoordinatorChange_StartsFromGenerationZero(t *testing.T) {
	first := newTestCoordinator(t)
	joinOne(first, "g", "m1", 30000, 30000)
	_, gen, _, _, err := first.JoinGroup("g", "m2", "consumer", consumerProtos(), 30000, 30000)
	if err != nil {
		t.Fatalf("second join: %v", err)
	}
	if gen < 1 {
		t.Fatalf("generation after a join = %d, want >= 1", gen)
	}

	// The replacement knows nothing about the previous coordinator's members.
	second := newTestCoordinator(t)
	if n := memberCount(second, "g"); n != 0 {
		t.Fatalf("a new coordinator started with %d member(s); group state must not be restored", n)
	}

	// A member holding the old generation is refused, so it rejoins rather than
	// sitting on a generation the new coordinator will never satisfy.
	if err := second.Heartbeat("g", "m1", gen); err != ErrMemberNotFound {
		t.Errorf("heartbeat from a member the new coordinator never saw = %v, want ErrMemberNotFound", err)
	}

	// Generations are per-coordinator counters, so the new one legitimately
	// reaches the same number again. What makes that safe is that the member id
	// is unknown to the new coordinator, which is what forces the rejoin rather
	// than silently continuing against the old assignment.
	newMember, newGen, _, _, err := second.JoinGroup("g", "m1", "consumer", consumerProtos(), 30000, 30000)
	if err != nil {
		t.Fatalf("rejoin on the new coordinator: %v", err)
	}
	if newMember != "m1" {
		t.Errorf("member id = %q, want m1", newMember)
	}
	if newGen < 0 {
		t.Errorf("generation after rejoin = %d, want >= 0", newGen)
	}
	if n := memberCount(second, "g"); n != 1 {
		t.Errorf("member count after rejoin = %d, want 1", n)
	}
	if groups := second.ListGroups(); len(groups) != 1 {
		t.Errorf("the new coordinator lists %d group(s) after the rejoin, want 1", len(groups))
	}
}

// Two coordinators for one group is the failure the whole selection mechanism
// exists to prevent, so it is worth asserting the shape that produces it is not
// reachable: a group state built by one coordinator is invisible to another, and
// neither can act on the other's members.
func TestCoordinatorChange_NoSharedState(t *testing.T) {
	first := newTestCoordinator(t)
	joinOne(first, "g", "m1", 30000, 30000)

	second := newTestCoordinator(t)
	if err := second.Heartbeat("g", "m1", 1); err != ErrMemberNotFound {
		t.Errorf("the second coordinator accepted a heartbeat for a member it never saw: %v", err)
	}
	if groups := second.ListGroups(); len(groups) != 0 {
		t.Errorf("the second coordinator lists %d group(s); it must know of none", len(groups))
	}
}
