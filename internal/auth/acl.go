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
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Operation is a permission, named after the Kafka operations it gates so a
// rule reads the same as the request it governs.
type Operation string

const (
	OpRead     Operation = "Read"
	OpWrite    Operation = "Write"
	OpCreate   Operation = "Create"
	OpDelete   Operation = "Delete"
	OpDescribe Operation = "Describe"
	// OpAlter covers Admin/AlterConfigs and DeleteRecords.
	OpAlter Operation = "Alter"
	// OpAll matches every operation. It is a grant shorthand, not a separate
	// capability.
	OpAll Operation = "All"
)

// AllOperations is every concrete operation, in a stable order. OpAll is not
// here: it is a matching rule, not something to grant individually.
var AllOperations = []Operation{OpRead, OpWrite, OpCreate, OpDelete, OpDescribe, OpAlter}

// ResourceKind is what an ACL applies to.
type ResourceKind string

const (
	ResourceTopic   ResourceKind = "Topic"
	ResourceGroup   ResourceKind = "Group"
	ResourceCluster ResourceKind = "Cluster"
)

// Permission is Allow or Deny.
type Permission string

const (
	Allow Permission = "Allow"
	Deny  Permission = "Deny"
)

// PrincipalAll is the wildcard principal, matching any authenticated identity.
// There is deliberately no wildcard for anonymous callers: an unauthenticated
// client is either allowed by a specific rule naming ANONYMOUS or it is not
// allowed at all, because "*" plus a broker that allows anonymous reads is how
// a multi-tenant broker leaks.
const PrincipalAll = "*"

// PrincipalAnonymous is the conventional name for an unauthenticated caller.
const PrincipalAnonymous = "ANONYMOUS"

// Resource names a thing an ACL protects.
//
// An empty Name means every resource of that kind, which is how an operator
// writes "alice may read every topic". This mirrors Kafka, where a topic ACL
// without a name covers all topics and Cluster is a separate kind for
// cluster-scoped operations like CreateTopics. Keeping those two apart matters:
// a Cluster rule that silently covered topics would hand out far more than
// whoever wrote it intended.
type Resource struct {
	Kind ResourceKind `json:"kind"`
	Name string       `json:"name,omitempty"`
}

// Topic builds a topic resource. An empty name covers all topics.
func Topic(name string) Resource { return Resource{Kind: ResourceTopic, Name: name} }

// AllTopics covers every topic.
func AllTopics() Resource { return Resource{Kind: ResourceTopic} }

// Cluster is the cluster-scoped resource: operations that are not about any one
// topic or group.
func Cluster() Resource { return Resource{Kind: ResourceCluster} }

// Group builds a consumer-group resource. An empty name covers all groups.
func Group(name string) Resource { return Resource{Kind: ResourceGroup, Name: name} }

// ACL is one rule.
type ACL struct {
	Principal  string     `json:"principal"`
	Operation  Operation  `json:"operation"`
	Resource   Resource   `json:"resource"`
	Permission Permission `json:"permission"`
}

// matches reports whether this ACL applies to a request. It answers only
// applicability, never permission: an ACL that does not match is skipped, and
// one that does may still say Allow or Deny.
func (a ACL) matches(principal string, op Operation, res Resource) bool {
	if a.Resource.Kind != res.Kind {
		return false
	}
	// Kinds never cross, and an empty name covers all names of its own kind.
	if a.Resource.Name != "" && a.Resource.Name != res.Name {
		return false
	}
	if a.Operation != OpAll && a.Operation != op {
		return false
	}
	if a.Principal == PrincipalAll {
		return true
	}
	return strings.EqualFold(a.Principal, principal)
}

// Policy is a set of rules evaluated per request.
//
// It is immutable once built, so it can be swapped atomically by a reload and
// in-flight requests keep evaluating against a consistent snapshot rather than
// a half-updated list. Building a new one is cheap; evaluating is a scan,
// because the rule sets involved are small and correctness is worth more here
// than a lookup structure.
type Policy struct {
	aclList []ACL
}

// NewPolicy builds a policy, rejecting malformed rules rather than silently
// dropping them: an operator who wrote a rule that does nothing should find out
// when they wrote it, not when a client is denied.
func NewPolicy(acls []ACL) (*Policy, error) {
	for i, a := range acls {
		switch a.Permission {
		case Allow, Deny:
		default:
			return nil, fmt.Errorf("acl %d: permission must be Allow or Deny, got %q", i, a.Permission)
		}
		switch a.Operation {
		case OpAll, OpRead, OpWrite, OpCreate, OpDelete, OpDescribe, OpAlter:
		default:
			return nil, fmt.Errorf("acl %d: unknown operation %q", i, a.Operation)
		}
		switch a.Resource.Kind {
		case ResourceTopic, ResourceGroup, ResourceCluster:
		default:
			return nil, fmt.Errorf("acl %d: unknown resource kind %q", i, a.Resource.Kind)
		}
		if a.Principal == "" {
			return nil, fmt.Errorf("acl %d: principal is empty", i)
		}

	}
	cp := make([]ACL, len(acls))
	copy(cp, acls)
	return &Policy{aclList: cp}, nil
}

// Empty reports whether the policy grants nothing, which is how the caller
// distinguishes "no ACLs configured" from "ACLs configured and this is denied".
//
// The distinction matters more than it looks. A broker with no ACLs must allow
// everything, or upgrading would break every existing deployment. A broker with
// ACLs must deny by default, or a typo in a topic name silently exposes data.
func (p *Policy) Empty() bool { return p == nil || len(p.aclList) == 0 }

// Enabled reports whether any rules are configured.
func (p *Policy) Enabled() bool { return !p.Empty() }

// ACLs returns the rules, for the admin tooling to display.
func (p *Policy) ACLs() []ACL {
	if p == nil {
		return nil
	}
	out := make([]ACL, len(p.aclList))
	copy(out, p.aclList)
	return out
}

// Allows decides a request.
//
// Deny beats Allow, matching Kafka: an explicit deny is a decision an operator
// made on purpose, and an allow written earlier must not quietly override it.
// Everything is evaluated rather than short-circuited on the first Allow, which
// is what makes Deny win.
func (p *Policy) Allows(principal string, op Operation, res Resource) bool {
	if p.Empty() {
		// ACLs are not configured: this is not a broker with authorization, it
		// is the broker people have been running.
		return true
	}
	allowed := false
	for _, a := range p.aclList {
		if !a.matches(principal, op, res) {
			continue
		}
		if a.Permission == Deny {
			return false
		}
		allowed = true
	}
	return allowed
}

// ErrNotAuthorized is the error returned for a refused request. The concrete
// Kafka codes live in internal/protocol, which cannot be imported here without
// a cycle, so the caller maps this to the code for the resource kind. Returning
// one sentinel rather than a string keeps the decision in one place instead of
// at every call site.
var ErrNotAuthorized = errors.New("not authorized for this operation")

// Authorize returns ErrNotAuthorized for a refused request, or nil if allowed.
func (p *Policy) Authorize(principal string, op Operation, res Resource) error {
	if p.Allows(principal, op, res) {
		return nil
	}
	return fmt.Errorf("%w: %s may not %s on %s %q",
		ErrNotAuthorized, principal, op, res.Kind, res.Name)
}

// Describe renders the policy the way the admin tooling and the docs express it,
// sorted so output is stable between runs.
func (p *Policy) Describe() []string {
	if p.Empty() {
		return nil
	}
	out := make([]string, 0, len(p.aclList))
	for _, a := range p.aclList {
		name := a.Resource.Kind.String()
		if a.Resource.Name != "" {
			name += ":" + a.Resource.Name
		}
		out = append(out, fmt.Sprintf("%s %s %s on %s", a.Permission, a.Principal, a.Operation, name))
	}
	sort.Strings(out)
	return out
}

// String makes ResourceKind printable.
func (k ResourceKind) String() string { return string(k) }

// ParseOperation maps a name to an operation, case-insensitively, so a config
// file and a flag agree about "read" and "Read".
func ParseOperation(s string) (Operation, error) {
	for _, op := range append(AllOperations, OpAll) {
		if strings.EqualFold(s, string(op)) {
			return op, nil
		}
	}
	return "", fmt.Errorf("unknown operation %q (want one of %s, All)", s, joinOps(AllOperations))
}

// ParsePermission maps a name to Allow or Deny.
func ParsePermission(s string) (Permission, error) {
	switch strings.ToLower(s) {
	case "allow":
		return Allow, nil
	case "deny":
		return Deny, nil
	default:
		return "", fmt.Errorf("unknown permission %q (want Allow or Deny)", s)
	}
}

func joinOps(ops []Operation) string {
	parts := make([]string, 0, len(ops))
	for _, o := range ops {
		parts = append(parts, string(o))
	}
	return strings.Join(parts, ", ")
}
