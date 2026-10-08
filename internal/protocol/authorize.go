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
	"kimistore/internal/auth"
)

// Authorization.
//
// Every check goes through authorize so there is one place that decides who a
// caller is and one place that turns a refusal into the right Kafka error code.
// Spreading that across handlers is how a data-plane request ends up checked
// against a different rule than the equivalent admin request.
//
// The checks are skipped entirely when no rules are configured, which is the
// whole point of Policy.Empty: a broker with no ACLs behaves exactly as it did
// before this existed, so enabling ACLs is a deliberate act rather than a
// side effect of an upgrade.

// principal returns the identity this session is acting as.
//
// An unauthenticated session is ANONYMOUS, never the empty string: a rule that
// names a principal must not match an unauthenticated caller by accident, and
// the wildcard exists for anyone who genuinely wants to cover everyone.
func (s *Session) principal() string {
	if s == nil || !s.Authenticated || s.User == "" {
		return auth.PrincipalAnonymous
	}
	return s.User
}

// policy returns the policy in force, or nil when ACLs are not configured.
func policyFor(cfg ServerConfig) *auth.Policy {
	if cfg.Auth.ACLs == nil {
		return nil
	}
	return cfg.Auth.ACLs.Policy()
}

// authorize returns 0 when the request is allowed, or the Kafka error code that
// says it is not.
func authorize(cfg ServerConfig, session *Session, op auth.Operation, res auth.Resource) int16 {
	policy := policyFor(cfg)
	if policy == nil || policy.Empty() {
		return ErrNone
	}
	if policy.Allows(session.principal(), op, res) {
		return ErrNone
	}
	return authorizeErrorCode(res)
}

// authorizeTopics checks every topic in a request and returns the first refusal.
//
// A request naming several topics is refused whole rather than partially
// fulfilled. Serving the topics the caller may see and silently dropping the
// others would let a client believe its write succeeded: the record count in
// the response would not match what it sent, and a producer retrying on
// per-topic errors would loop.
func authorizeTopics(cfg ServerConfig, session *Session, op auth.Operation, topics []string) int16 {
	for _, t := range topics {
		if code := authorize(cfg, session, op, auth.Topic(t)); code != ErrNone {
			return code
		}
	}
	return ErrNone
}

// authorizeErrorCode maps a refused resource to the code clients already handle.
func authorizeErrorCode(res auth.Resource) int16 {
	switch res.Kind {
	case auth.ResourceGroup:
		return ErrGroupAuthorizationFailed
	case auth.ResourceTopic:
		return ErrTopicAuthorizationFailed
	default:
		return ErrClusterAuthorizationFailed
	}
}

// aclActive reports whether authorization is actually in force.
//
// It is not the same question as whether a store exists. A store with no rules
// means the feature is off, and every request is allowed: doing the topic peek
// anyway would put a parse on the hot path of every produce for no decision, and
// a bug in the peek would then break traffic for a deployment that never asked
// for authorization.
func aclActive(cfg ServerConfig) bool {
	p := policyFor(cfg)
	return p != nil && !p.Empty()
}
