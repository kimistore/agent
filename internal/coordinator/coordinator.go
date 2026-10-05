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
	"log"
	"strings"
	"sync"
	"time"

	"kimistore/internal/metrics"
)

// GroupState represents the state of a consumer group.
type GroupState int

const (
	GroupStateDead                GroupState = 0
	GroupStateEmpty               GroupState = 1
	GroupStatePreparingRebalance  GroupState = 2
	GroupStateCompletingRebalance GroupState = 3
	GroupStateStable              GroupState = 4
)

// Tunables. Declared as vars rather than consts so tests can shrink the
// timeouts instead of having to wait them out.
var (
	// reapInterval is how often the reaper scans for dead members.
	reapInterval = time.Second

	// minSessionTimeout floors a client-supplied session.timeout.ms. A client
	// asking for 0 or 1ms would otherwise get its members reaped out from
	// under it on the very next tick.
	minSessionTimeout = time.Second

	// reapGrace is added on top of the effective timeout so a member that is
	// mid-request, or mid-rebalance where heartbeats legitimately pause, is
	// not evicted on the boundary.
	reapGrace = time.Second

	// defaultSyncWait bounds how long a follower waits in SyncGroup for the
	// leader to publish an assignment. Without a bound, a leader that dies
	// between JoinGroup and SyncGroup parks every follower forever.
	defaultSyncWait = 10 * time.Second

	// maxSyncWait caps the SyncGroup wait regardless of what a client asks
	// for, so a pathological rebalanceTimeout cannot pin goroutines.
	maxSyncWait = 30 * time.Second
)

// Sentinel errors. The protocol layer maps these onto Kafka error codes.
var (
	ErrRebalanceInProgress = fmt.Errorf("rebalance in progress")
	ErrMemberNotFound      = fmt.Errorf("member not found")
	ErrSyncTimeout         = fmt.Errorf("timed out waiting for assignment")
	ErrNoProtocols         = fmt.Errorf("no consumer protocols provided")
)

type MemberMetadata struct {
	MemberID         string          `json:"member_id"`
	ClientID         string          `json:"client_id"`
	ClientHost       string          `json:"client_host"`
	SessionTimeout   int32           `json:"session_timeout"`
	RebalanceTimeout int32           `json:"rebalance_timeout"`
	ProtocolType     string          `json:"protocol_type"`
	Protocols        []GroupProtocol `json:"protocols"`
	Assignment       []byte          `json:"assignment"`
	Heartbeat        time.Time       `json:"heartbeat"`
}

type GroupProtocol struct {
	Name     string `json:"name"`
	Metadata []byte `json:"metadata"`
}

type CoordinatorState struct {
	Groups map[string]GroupSnapshot `json:"groups"`
}

type GroupSnapshot struct {
	Name         string                    `json:"name"`
	State        GroupState                `json:"state"`
	GenerationID int32                     `json:"generation_id"`
	ProtocolType string                    `json:"protocol_type"`
	Protocol     string                    `json:"protocol"`
	Members      map[string]MemberMetadata `json:"members"`
	LeaderID     string                    `json:"leader_id"`
}

func (c *Coordinator) ToState() CoordinatorState {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := CoordinatorState{
		Groups: make(map[string]GroupSnapshot),
	}

	for name, g := range c.groups {
		g.mu.Lock()
		members := make(map[string]MemberMetadata)
		for mID, m := range g.Members {
			members[mID] = *m
		}
		state.Groups[name] = GroupSnapshot{
			Name:         g.Name,
			State:        g.State,
			GenerationID: g.GenerationID,
			ProtocolType: g.ProtocolType,
			Protocol:     g.Protocol,
			Members:      members,
			LeaderID:     g.LeaderID,
		}
		g.mu.Unlock()
	}

	return state
}

func (c *Coordinator) FromState(state CoordinatorState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for name, gs := range state.Groups {
		g := &Group{
			Name:         gs.Name,
			State:        gs.State,
			GenerationID: gs.GenerationID,
			ProtocolType: gs.ProtocolType,
			Protocol:     gs.Protocol,
			Members:      make(map[string]*MemberMetadata),
			Offsets:      make(map[string]map[int32]int64),
			LeaderID:     gs.LeaderID,
			waitCh:       make(chan struct{}),
		}
		for mID, m := range gs.Members {
			mCopy := m
			// Restored members are always dead: their connections belonged to
			// the process that just exited. Zeroing the heartbeat lets the
			// reaper converge the group instead of leaving phantom members
			// that hold partitions hostage.
			mCopy.Heartbeat = time.Time{}
			g.Members[mID] = &mCopy
		}
		c.groups[name] = g
	}
}

type Group struct {
	Name         string
	State        GroupState
	GenerationID int32
	ProtocolType string
	Protocol     string // Selected protocol
	Members      map[string]*MemberMetadata
	Offsets      map[string]map[int32]int64 // Topic -> Partition -> Offset
	LeaderID     string
	mu           sync.Mutex

	// waitCh is a broadcast latch. It is closed and replaced whenever group
	// state changes, waking every waiter at once. A sync.Cond was used here
	// previously, but it cannot express a timeout, and an untimed wait is what
	// let followers hang forever when the leader died.
	waitCh chan struct{}
}

// notifyLocked wakes every goroutine waiting on this group. Callers must hold
// g.mu.
func (g *Group) notifyLocked() {
	close(g.waitCh)
	g.waitCh = make(chan struct{})
}

// effectiveTimeoutLocked is how long a member may go without a heartbeat
// before it is considered dead. During a rebalance the rebalanceTimeout
// applies instead, since that is the window a member legitimately spends
// rejoining rather than heartbeating.
func (g *Group) effectiveTimeoutLocked(m *MemberMetadata) time.Duration {
	t := time.Duration(m.SessionTimeout) * time.Millisecond
	if g.State == GroupStatePreparingRebalance || g.State == GroupStateCompletingRebalance {
		if rt := time.Duration(m.RebalanceTimeout) * time.Millisecond; rt > t {
			t = rt
		}
	}
	if t < minSessionTimeout {
		t = minSessionTimeout
	}
	return t + reapGrace
}

// electLeaderLocked picks a leader from the current members. Callers must hold
// g.mu.
func (g *Group) electLeaderLocked() {
	if g.LeaderID != "" {
		if _, ok := g.Members[g.LeaderID]; ok {
			return
		}
	}
	g.LeaderID = ""
	for id := range g.Members {
		g.LeaderID = id
		break
	}
}

type Coordinator struct {
	groups map[string]*Group
	mu     sync.Mutex

	stop   chan struct{}
	wg     sync.WaitGroup
	closed bool
}

func NewCoordinator() *Coordinator {
	c := &Coordinator{
		groups: make(map[string]*Group),
		stop:   make(chan struct{}),
	}
	c.wg.Add(1)
	go c.reapLoop()
	return c
}

// Close stops the background reaper. Safe to call more than once.
func (c *Coordinator) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()
	close(c.stop)
	c.wg.Wait()
}

// SelectedProtocol reports the protocol name the group has settled on, so the
// protocol layer can return the same name, and the matching member metadata,
// to every participant.
func (c *Coordinator) SelectedProtocol(groupID string) string {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.Protocol
}

func (c *Coordinator) GetGroup(groupID string) *Group {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Lazy create? Or explicitly CreateGroup?
	// JoinGroup usually creates it.
	if g, ok := c.groups[groupID]; ok {
		return g
	}
	g := &Group{
		Name:    groupID,
		State:   GroupStateEmpty,
		Members: make(map[string]*MemberMetadata),
		Offsets: make(map[string]map[int32]int64),
		waitCh:  make(chan struct{}),
	}
	c.groups[groupID] = g
	return g
}

func (c *Coordinator) JoinGroup(groupID, memberID, protocolType string, protocols []GroupProtocol, sessionTimeout int32, rebalanceTimeout int32) (string, int32, string, []MemberMetadata, error) {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()

	// Generate MemberID if missing
	if memberID == "" {
		memberID = fmt.Sprintf("member-%d", time.Now().UnixNano()) // Simple ID
	}

	// Reject a client that sends an empty Protocols array. Selecting a
	// protocol is mandatory, so reject rather than indexing into an empty
	// slice and panicking (which would kill the connection).
	if len(protocols) == 0 {
		return "", 0, "", nil, ErrNoProtocols
	}

	// 2. Add/Update Member
	g.Members[memberID] = &MemberMetadata{
		MemberID:         memberID,
		SessionTimeout:   sessionTimeout,
		RebalanceTimeout: rebalanceTimeout,
		ProtocolType:     protocolType,
		Protocols:        protocols,
		Heartbeat:        time.Now(),
	}

	if g.ProtocolType == "" {
		g.ProtocolType = protocolType
	}

	// 3. Fake Rebalance Logic
	// Simplification: Always elect first member as leader.
	if g.LeaderID == "" || g.Members[g.LeaderID] == nil {
		g.LeaderID = memberID
	}

	// Increment generation to signal new rebalance
	// But ONLY if we are not already in rebalance (CompletingRebalance).
	// If we are Completing, it means we are gathering members for the NEW generation.
	// Late joiners will join this generation. The Leader (joining last usually) will verify everyone.
	if g.State != GroupStateCompletingRebalance {
		g.GenerationID++
		g.State = GroupStateCompletingRebalance
	}
	g.Protocol = protocols[0].Name

	// Return list of members so Leader can assign
	memberList := make([]MemberMetadata, 0, len(g.Members))
	for _, m := range g.Members {
		memberList = append(memberList, *m)
	}

	return memberID, g.GenerationID, g.LeaderID, memberList, nil
}

// SyncGroup stores the leader's assignment and returns each member's own.
//
// Followers block until the leader publishes an assignment, but the wait is
// bounded. Previously this was an untimed cond.Wait(), so a leader that died
// between JoinGroup and SyncGroup left every follower parked forever, holding
// a goroutine and a client connection.
func (c *Coordinator) SyncGroup(groupID, memberID string, generationID int32, groupAssignment []GroupAssignment) ([]byte, error) {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()

	// Membership first, for the same reason as Heartbeat: being absent from the
	// group is a stronger and more actionable signal than a stale generation.
	if _, ok := g.Members[memberID]; !ok {
		return nil, ErrMemberNotFound
	}
	if g.GenerationID != generationID {
		return nil, ErrRebalanceInProgress
	}

	if memberID == g.LeaderID {
		// Leader: publish the assignment and release everyone waiting.
		for _, assign := range groupAssignment {
			if m, ok := g.Members[assign.MemberID]; ok {
				m.Assignment = assign.Assignment
			}
		}
		g.State = GroupStateStable
		g.notifyLocked()
	} else {
		if err := g.waitForAssignmentLocked(generationID); err != nil {
			return nil, err
		}
	}

	if m, ok := g.Members[memberID]; ok {
		return m.Assignment, nil
	}
	return nil, ErrMemberNotFound
}

// waitForAssignmentLocked blocks until the group reaches Stable on this
// generation, the generation moves on, or the bounded wait expires. The caller
// must hold g.mu; it is released while waiting and reacquired on return.
func (g *Group) waitForAssignmentLocked(generationID int32) error {
	timeout := defaultSyncWait
	if m, ok := g.Members[g.LeaderID]; ok {
		if rt := time.Duration(m.RebalanceTimeout) * time.Millisecond; rt > timeout {
			timeout = rt
		}
	}
	if timeout > maxSyncWait {
		timeout = maxSyncWait
	}
	deadline := time.Now().Add(timeout)

	for g.State != GroupStateStable && g.GenerationID == generationID {
		ch := g.waitCh
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		g.mu.Unlock()
		timer := time.NewTimer(remaining)
		select {
		case <-ch:
		case <-timer.C:
		}
		timer.Stop()
		g.mu.Lock()
	}

	// The generation moving on means another rebalance started, which is a
	// normal and expected outcome; the client should rejoin.
	if g.GenerationID != generationID {
		return ErrRebalanceInProgress
	}
	if g.State != GroupStateStable {
		return ErrSyncTimeout
	}
	return nil
}

func (c *Coordinator) Heartbeat(groupID, memberID string, generationID int32) error {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()

	// Membership is checked before generation. A member the reaper evicted is
	// no longer part of the group at any generation, and telling the client
	// UnknownMemberId is what makes it discard its member ID and rejoin
	// cleanly. Answering RebalanceInProgress instead would leave it heartbeating
	// against a generation it can never satisfy.
	m, ok := g.Members[memberID]
	if !ok {
		return ErrMemberNotFound
	}
	if g.GenerationID != generationID {
		return ErrRebalanceInProgress
	}
	m.Heartbeat = time.Now()
	return nil
}

func (c *Coordinator) LeaveGroup(groupID, memberID string) error {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.Members[memberID]; !ok {
		return ErrMemberNotFound
	}
	delete(g.Members, memberID)

	// A membership change invalidates the current generation, and followers
	// may be blocked waiting on the assignment. Wake them so they rejoin
	// rather than sitting on a stale generation until their wait expires.
	g.GenerationID++
	g.electLeaderLocked()
	if len(g.Members) == 0 {
		g.State = GroupStateEmpty
	}
	g.notifyLocked()
	return nil
}

// External Store is needed for Offset Commit/Fetch?
// Actually better to have Coordinator call StorageEngine or have StorageEngine passed in methods.
// For specific OffsetCommit/Fetch APIs, we can just pass the StorageEngine interface.

type Storage interface {
	SaveOffset(groupID, topic string, partition int32, offset int64) error
	LoadOffset(groupID, topic string, partition int32) (int64, error)
}

func (c *Coordinator) CommitOffset(store Storage, groupID, topic string, partition int32, offset int64) error {
	return store.SaveOffset(groupID, topic, partition, offset)
}

func (c *Coordinator) FetchOffset(store Storage, groupID, topic string, partition int32) (int64, error) {
	return store.LoadOffset(groupID, topic, partition)
}

// Observability for ListGroups/DescribeGroups

type GroupOverview struct {
	GroupID      string
	ProtocolType string
}

func (c *Coordinator) ListGroups() []GroupOverview {
	c.mu.Lock()
	defer c.mu.Unlock()

	var list []GroupOverview
	for name, g := range c.groups {
		// Filter out dead/empty? Kafka usually lists all.
		g.mu.Lock()
		pType := g.ProtocolType
		g.mu.Unlock()

		list = append(list, GroupOverview{GroupID: name, ProtocolType: pType})
	}
	return list
}

type GroupDetail struct {
	State        string
	ProtocolType string
	Protocol     string
	Members      []MemberDetail
}

type MemberDetail struct {
	MemberID   string
	ClientID   string
	ClientHost string
	Metadata   []byte
	Assignment []byte
}

func (c *Coordinator) DescribeGroup(groupID string) (*GroupDetail, error) {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.State == GroupStateDead {
		return nil, fmt.Errorf("group dead")
	}

	stateStr := "Stable"
	switch g.State {
	case GroupStateEmpty:
		stateStr = "Empty"
	case GroupStatePreparingRebalance:
		stateStr = "PreparingRebalance"
	case GroupStateCompletingRebalance:
		stateStr = "CompletingRebalance"
	case GroupStateDead:
		stateStr = "Dead"
	}

	detail := &GroupDetail{
		State:        stateStr,
		ProtocolType: g.ProtocolType,
		Protocol:     g.Protocol,
	}

	for _, m := range g.Members {
		detail.Members = append(detail.Members, MemberDetail{
			MemberID:   m.MemberID,
			ClientID:   m.ClientID,
			ClientHost: m.ClientHost,
			// For DescribeGroup, we generally return Metadata/Assignment
			// BUT careful: Protocol metadata is per-protocol.
			// Assignment is result of SyncGroup.
			Assignment: m.Assignment,
			// Metadata is usually empty in DescribeGroup response?
			// Kafka DescribeGroups response has MemberMetadata and MemberAssignment fields.
		})
	}
	return detail, nil
}

type GroupAssignment struct {
	MemberID   string
	Assignment []byte
}

// ---------------------------------------------------------------------------
// Session reaper
// ---------------------------------------------------------------------------

// reapLoop periodically evicts members that have stopped heartbeating.
//
// Without this, a consumer that crashes is never removed from its group: its
// partitions are never reassigned, and the group never rebalances. Heartbeats
// were being recorded but never read.
func (c *Coordinator) reapLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.ReapExpired()
		}
	}
}

// ReapExpired evicts members whose session has expired and moves any affected
// group to a new generation. Exported so it can be driven directly in tests.
func (c *Coordinator) ReapExpired() {
	now := time.Now()

	c.mu.Lock()
	groups := make([]*Group, 0, len(c.groups))
	for _, g := range c.groups {
		groups = append(groups, g)
	}
	c.mu.Unlock()

	for _, g := range groups {
		g.mu.Lock()

		var evicted []string
		for id, m := range g.Members {
			if now.Sub(m.Heartbeat) > g.effectiveTimeoutLocked(m) {
				evicted = append(evicted, id)
				delete(g.Members, id)
			}
		}

		if len(evicted) == 0 {
			g.mu.Unlock()
			continue
		}

		// Moving to a new generation both invalidates the previous assignment
		// and releases anything blocked in SyncGroup.
		g.GenerationID++
		g.electLeaderLocked()
		if len(g.Members) == 0 {
			g.State = GroupStateEmpty
			g.LeaderID = ""
		} else {
			g.State = GroupStatePreparingRebalance
		}
		g.notifyLocked()
		g.mu.Unlock()

		metrics.CoordinatorEvictions.Add(float64(len(evicted)))
		log.Printf("Coordinator: evicted %d expired member(s) from group %s (%s); generation now %d, leader %q",
			len(evicted), g.Name, strings.Join(evicted, ","), g.GenerationID, g.LeaderID)
	}
}
