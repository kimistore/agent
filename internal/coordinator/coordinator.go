package coordinator

import (
	"fmt"
	"sync"
	"time"
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

type MemberMetadata struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	SessionTimeout   int32
	RebalanceTimeout int32
	ProtocolType     string
	Protocols        []GroupProtocol // List of (Name, Metadata)
	Assignment       []byte          // Assigned partitions (serialized)
	Heartbeat        time.Time
}

type GroupProtocol struct {
	Name     string
	Metadata []byte
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
}

type Coordinator struct {
	groups map[string]*Group
	mu     sync.Mutex
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		groups: make(map[string]*Group),
	}
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
	}
	c.groups[groupID] = g
	return g
}

func (c *Coordinator) JoinGroup(groupID, memberID, protocolType string, protocols []GroupProtocol, sessionTimeout int32) (string, int32, string, []MemberMetadata, error) {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()

	// 1. Generate MemberID if missing
	if memberID == "" {
		memberID = fmt.Sprintf("member-%d", time.Now().UnixNano()) // Simple ID
	}

	// 2. Add/Update Member
	g.Members[memberID] = &MemberMetadata{
		MemberID:       memberID,
		SessionTimeout: sessionTimeout,
		ProtocolType:   protocolType,
		Protocols:      protocols,
		Heartbeat:      time.Now(),
	}

	// 3. Logic for "Fake" Rebalance (Immediate)
	// If state is Empty or Stable, we start a "Rebalance".
	// For MVP, we treat every Join as a successful rebalance immediately if it's the Leader (first one),
	// or if we decide to support only 1 member for now.

	// Simplification: Always elect first member as leader.
	if g.LeaderID == "" || g.Members[g.LeaderID] == nil {
		g.LeaderID = memberID
	}

	g.GenerationID++
	g.State = GroupStateCompletingRebalance // We skip "Preparing" wait for MVP
	g.Protocol = protocols[0].Name          // Just pick first one

	// Return list of members so Leader can assign
	memberList := make([]MemberMetadata, 0, len(g.Members))
	for _, m := range g.Members {
		memberList = append(memberList, *m)
	}

	return memberID, g.GenerationID, g.LeaderID, memberList, nil
}

func (c *Coordinator) SyncGroup(groupID, memberID string, generationID int32, groupAssignment []GroupAssignment) ([]byte, error) {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.GenerationID != generationID {
		return nil, fmt.Errorf("invalid generation")
	}

	// If Leader, store assignments
	if memberID == g.LeaderID {
		for _, assign := range groupAssignment {
			if m, ok := g.Members[assign.MemberID]; ok {
				m.Assignment = assign.Assignment
			}
		}
		g.State = GroupStateStable
	}

	// Return my assignment
	if m, ok := g.Members[memberID]; ok {
		return m.Assignment, nil
	}
	return nil, fmt.Errorf("member not found")
}

func (c *Coordinator) Heartbeat(groupID, memberID string, generationID int32) error {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.GenerationID != generationID {
		return fmt.Errorf("rebalance needed")
	}
	if m, ok := g.Members[memberID]; ok {
		m.Heartbeat = time.Now()
		return nil
	}
	return fmt.Errorf("member not found")
}

func (c *Coordinator) LeaveGroup(groupID, memberID string) error {
	g := c.GetGroup(groupID)
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.Members, memberID)
	if g.LeaderID == memberID {
		// Elect new leader?
		g.LeaderID = ""
		for mID := range g.Members {
			g.LeaderID = mID
			break
		}
	}
	if len(g.Members) == 0 {
		g.State = GroupStateEmpty
	}
	return nil
}

type GroupAssignment struct {
	MemberID   string
	Assignment []byte
}
