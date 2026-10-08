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

package auth

import (
	"context"
	"errors"
	"testing"
)

func TestPolicy_NoRulesAllowsEverything(t *testing.T) {
	// This is the property that makes ACLs safe to ship: a broker with no
	// rules must behave exactly as it did before authorization existed.
	p, err := NewPolicy(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range AllOperations {
		if !p.Allows("anyone", op, Topic("anything")) {
			t.Errorf("%s was denied with no rules configured", op)
		}
	}
	if p.Enabled() {
		t.Error("an empty policy reports itself enabled")
	}
}

func TestPolicy_DenyBeatsAllow(t *testing.T) {
	// Order must not matter: an operator who wrote an explicit deny meant it,
	// and an allow written earlier must not quietly override it.
	p, err := NewPolicy([]ACL{
		{Principal: "alice", Operation: OpAll, Resource: Topic("orders"), Permission: Allow},
		{Principal: "alice", Operation: OpWrite, Resource: Topic("orders"), Permission: Deny},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Allows("alice", OpWrite, Topic("orders")) {
		t.Error("an explicit Deny was overridden by an Allow")
	}
	if !p.Allows("alice", OpRead, Topic("orders")) {
		t.Error("Read should still be allowed")
	}
}

func TestPolicy_DenyBeatsAllowRegardlessOfOrder(t *testing.T) {
	p, err := NewPolicy([]ACL{
		{Principal: "alice", Operation: OpWrite, Resource: Topic("orders"), Permission: Deny},
		{Principal: "alice", Operation: OpAll, Resource: Topic("orders"), Permission: Allow},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Allows("alice", OpWrite, Topic("orders")) {
		t.Error("evaluation short-circuited on the first Allow")
	}
}

func TestPolicy_DefaultDenyOnceConfigured(t *testing.T) {
	// A typo in a topic name must not silently expose data.
	p, err := NewPolicy([]ACL{
		{Principal: "alice", Operation: OpAll, Resource: Topic("orders"), Permission: Allow},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Allows("alice", OpAll, Topic("orderz")) {
		t.Error("an unlisted topic was allowed once ACLs were configured")
	}
	if p.Allows("bob", OpAll, Topic("orders")) {
		t.Error("an unlisted principal was allowed once ACLs were configured")
	}
}

func TestPolicy_ClusterScopedRuleCoversEveryTopic(t *testing.T) {
	p, err := NewPolicy([]ACL{
		{Principal: "alice", Operation: OpRead, Resource: AllTopics(), Permission: Allow},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{"orders", "payments", "anything-at-all"} {
		if !p.Allows("alice", OpRead, Topic(topic)) {
			t.Errorf("all-topics Read did not cover %q", topic)
		}
	}
	if p.Allows("alice", OpWrite, Topic("orders")) {
		t.Error("cluster Read grant leaked into Write")
	}
}

func TestPolicy_WildcardPrincipal(t *testing.T) {
	p, err := NewPolicy([]ACL{
		{Principal: PrincipalAll, Operation: OpRead, Resource: Topic("public"), Permission: Allow},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, who := range []string{"alice", "bob", "ANONYMOUS", ""} {
		if !p.Allows(who, OpRead, Topic("public")) {
			t.Errorf("wildcard did not cover %q", who)
		}
	}
	if p.Allows("alice", OpWrite, Topic("public")) {
		t.Error("wildcard Read grant leaked into Write")
	}
}

func TestPolicy_ResourceKindsDoNotCross(t *testing.T) {
	p, err := NewPolicy([]ACL{
		{Principal: "alice", Operation: OpAll, Resource: Topic("orders"), Permission: Allow},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Allows("alice", OpAll, Group("orders")) {
		t.Error("a topic grant leaked into a group of the same name")
	}
}

func TestPolicy_PrincipalIsCaseInsensitive(t *testing.T) {
	// SCRAM usernames are case-sensitive, but operators typing rules by hand
	// will not remember that, and a silently non-matching rule is a denial they
	// will debug for an hour.
	p, err := NewPolicy([]ACL{
		{Principal: "Alice", Operation: OpAll, Resource: Topic("orders"), Permission: Allow},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !p.Allows("alice", OpAll, Topic("orders")) {
		t.Error("principal comparison is case sensitive")
	}
}

func TestNewPolicy_RejectsMalformedRules(t *testing.T) {
	bad := []struct {
		name string
		acl  ACL
	}{
		{"unknown permission", ACL{Principal: "a", Operation: OpRead, Resource: Topic("t"), Permission: "Maybe"}},
		{"unknown operation", ACL{Principal: "a", Operation: "Fly", Resource: Topic("t"), Permission: Allow}},
		{"unknown resource", ACL{Principal: "a", Operation: OpRead, Resource: Resource{Kind: "Queue"}, Permission: Allow}},
		{"empty principal", ACL{Operation: OpRead, Resource: Topic("t"), Permission: Allow}},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewPolicy([]ACL{tc.acl}); err == nil {
				t.Error("accepted a malformed rule; an operator would never learn it does nothing")
			}
		})
	}
}

func TestParseHelpers(t *testing.T) {
	if op, err := ParseOperation("read"); err != nil || op != OpRead {
		t.Errorf("ParseOperation(read) = %v, %v", op, err)
	}
	if _, err := ParseOperation("fly"); err == nil {
		t.Error("ParseOperation accepted an unknown operation")
	}
	if p, err := ParsePermission("ALLOW"); err != nil || p != Allow {
		t.Errorf("ParsePermission(ALLOW) = %v, %v", p, err)
	}
	if _, err := ParsePermission("maybe"); err == nil {
		t.Error("ParsePermission accepted an unknown permission")
	}
}

func TestACLStore_AddRemoveReload(t *testing.T) {
	objects := newFakeObjects()
	s, err := NewACLStore(objects)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	if s.Enabled() {
		t.Error("a fresh store reports itself enabled")
	}
	// Nothing configured means allow everything.
	if !s.Policy().Allows("alice", OpWrite, Topic("orders")) {
		t.Error("a store with no rules denies")
	}

	rule := ACL{Principal: "alice", Operation: OpRead, Resource: Topic("orders"), Permission: Allow}
	if err := s.Add(ctx, rule); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if !s.Enabled() {
		t.Error("store does not report itself enabled after a rule was added")
	}
	if !s.Policy().Allows("alice", OpRead, Topic("orders")) {
		t.Error("the added rule is not enforced")
	}
	// Adding rules must switch the broker to deny-by-default.
	if s.Policy().Allows("alice", OpWrite, Topic("orders")) {
		t.Error("Write was allowed, but only Read was granted")
	}
	if s.Policy().Allows("bob", OpRead, Topic("orders")) {
		t.Error("an unlisted principal was allowed")
	}

	// A second store over the same objects sees the rule: this is what makes
	// several agents share one policy.
	other, err := NewACLStore(objects)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Reload(ctx); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if !other.Policy().Allows("alice", OpRead, Topic("orders")) {
		t.Error("a second agent did not pick up the rule")
	}

	if err := s.Remove(ctx, rule); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if s.Enabled() {
		t.Error("store still reports itself enabled after its last rule was removed")
	}
	if err := s.Remove(ctx, rule); err == nil {
		t.Error("removing a rule that is not there should say so")
	}
}

func TestACLStore_AddIsIdempotent(t *testing.T) {
	s, _ := NewACLStore(newFakeObjects())
	defer s.Close()
	ctx := context.Background()

	rule := ACL{Principal: "alice", Operation: OpRead, Resource: Topic("orders"), Permission: Allow}
	for i := 0; i < 3; i++ {
		if err := s.Add(ctx, rule); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	if got := len(s.Policy().ACLs()); got != 1 {
		t.Errorf("policy has %d rules after adding the same one three times, want 1", got)
	}
}

func TestACLStore_ReloadKeepsPreviousPolicyOnFailure(t *testing.T) {
	// A broker that emptied its rules because the bucket was briefly
	// unreachable would allow everything, which is the worst possible failure
	// direction for this feature.
	objects := newFakeObjects()
	s, _ := NewACLStore(objects)
	defer s.Close()
	ctx := context.Background()

	rule := ACL{Principal: "alice", Operation: OpRead, Resource: Topic("orders"), Permission: Allow}
	if err := s.Add(ctx, rule); err != nil {
		t.Fatal(err)
	}

	objects.getErr = errors.New("dial tcp: connection refused")
	if err := s.Reload(ctx); err == nil {
		t.Error("Reload succeeded while the store was failing")
	}
	if !s.Policy().Allows("alice", OpRead, Topic("orders")) {
		t.Error("a failed reload discarded the rules that were in force")
	}
	if !s.Enabled() {
		t.Error("a failed reload turned the feature off")
	}
}

func TestACLStore_CorruptShardDoesNotApplyPartially(t *testing.T) {
	objects := newFakeObjects()
	s, _ := NewACLStore(objects)
	defer s.Close()
	ctx := context.Background()

	if err := s.Add(ctx, ACL{Principal: "alice", Operation: OpRead, Resource: Topic("orders"), Permission: Allow}); err != nil {
		t.Fatal(err)
	}
	// Corrupt a different shard: Reload must refuse rather than apply the half
	// it could read.
	for i := 0; i < aclShardCount; i++ {
		objects.data[aclShardKey(i)] = []byte("{not json")
	}
	_ = objects.data[aclShardKey(shardFor(ACL{Principal: "alice", Operation: OpRead, Resource: Topic("orders"), Permission: Allow}))]
	if err := s.Reload(ctx); !errors.Is(err, ErrACLUnavailable) {
		t.Errorf("Reload error = %v, want ErrACLUnavailable", err)
	}
	// The previous policy must still be in force.
	if !s.Policy().Allows("alice", OpRead, Topic("orders")) {
		t.Error("a corrupt shard discarded the rules that were in force")
	}
}

func TestACLStore_AddRejectsMalformedRule(t *testing.T) {
	s, _ := NewACLStore(newFakeObjects())
	defer s.Close()
	if err := s.Add(context.Background(), ACL{Principal: "", Operation: OpRead, Resource: Topic("t"), Permission: Allow}); err == nil {
		t.Error("Add accepted a rule with no principal")
	}
}
