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

import "testing"

// TestJoinGroup_EmptyProtocolListIsRejected guards a remotely triggerable
// panic. Selecting a consumer protocol is mandatory, and indexing protocols[0]
// without a length check crashed the handler, killing the client connection.
func TestJoinGroup_EmptyProtocolListIsRejected(t *testing.T) {
	c := NewCoordinator()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("JoinGroup panicked on an empty protocol list: %v", r)
		}
	}()

	_, _, _, _, err := c.JoinGroup("g", "m1", "consumer", []GroupProtocol{}, 10000, 10000)
	if err == nil {
		t.Error("expected an error for an empty protocol list, got nil")
	}

	// The group must not have been left with a half-registered member.
	g := c.GetGroup("g")
	g.mu.Lock()
	n := len(g.Members)
	g.mu.Unlock()
	if n != 0 {
		t.Errorf("rejected join left %d member(s) registered, want 0", n)
	}
}

// TestJoinGroup_NilProtocolListIsRejected covers the nil variant of the same
// malformed request.
func TestJoinGroup_NilProtocolListIsRejected(t *testing.T) {
	c := NewCoordinator()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("JoinGroup panicked on a nil protocol list: %v", r)
		}
	}()

	if _, _, _, _, err := c.JoinGroup("g", "m1", "consumer", nil, 10000, 10000); err == nil {
		t.Error("expected an error for a nil protocol list, got nil")
	}
}

// TestJoinGroup_AcceptsValidProtocolList is the positive control, so the
// guards above cannot be "fixed" by rejecting everything.
func TestJoinGroup_AcceptsValidProtocolList(t *testing.T) {
	c := NewCoordinator()
	protos := []GroupProtocol{
		{Name: "range", Metadata: []byte{1}},
		{Name: "roundrobin", Metadata: []byte{2}},
	}

	memberID, generation, leader, members, err := c.JoinGroup("g", "m1", "consumer", protos, 10000, 10000)
	if err != nil {
		t.Fatalf("JoinGroup: %v", err)
	}
	if memberID != "m1" {
		t.Errorf("memberID = %q, want \"m1\"", memberID)
	}
	if leader != "m1" {
		t.Errorf("leaderID = %q, want \"m1\"", leader)
	}
	if generation == 0 {
		t.Error("generationID should be non-zero for a joining member")
	}
	if len(members) != 1 {
		t.Errorf("member list length = %d, want 1", len(members))
	}

	g := c.GetGroup("g")
	g.mu.Lock()
	selected := g.Protocol
	g.mu.Unlock()
	if selected != "range" {
		t.Errorf("selected protocol = %q, want \"range\"", selected)
	}
}
