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
	"fmt"
	"net/http"
	"time"
)

// Readiness and liveness answer different questions, and conflating them causes
// outages in both directions.
//
// Liveness is "should this process be killed". A broker that has lost its
// claims must NOT be restarted: a restart is slow, and the process is still
// perfectly able to serve reads from object storage, which is the only thing
// left to serve. Restarting it removes a working reader and gains nothing.
//
// Readiness is "should clients be sent here right now". A broker that cannot
// prove it still owns the partitions it is advertising, or whose view of the
// cluster is too old to answer Metadata with, must be taken out of rotation.
//
// The distinction is the whole point of having two probes. A single
// process-alive check is the common mistake, and with a Kafka protocol it is
// worse than useless: it keeps routing clients to a broker that will answer
// NOT_LEADER_OR_FOLLOWER for everything.

// Readiness is the outcome of the readiness check, with the reason it is not
// ready. The reason is returned so an operator sees why, rather than a bare 503
// that could be anything.
type Readiness struct {
	Ready  bool
	Reason string
}

// Readiness reports whether this agent should be sent clients right now.
//
// The checks are ordered by how badly they break clients, and each one is a
// precondition for trusting the next.
func (s *StorageEngine) Readiness() Readiness {
	// Ownership first. Every other answer this broker gives -- the leader in
	// Metadata, the read position, the route for a group -- is only correct if
	// the claims behind it are still held. An agent that has not been able to
	// renew for a full TTL may already have been superseded, and advertising
	// itself as the leader sends every producer to a broker that will refuse it.
	if s.ownership != nil && !s.ownership.Live() {
		return Readiness{false, "ownership claims cannot be renewed"}
	}

	// Then the cluster view. Metadata answers from the routing snapshot, so a
	// snapshot that has aged out or came from a failed refresh is a Metadata
	// answer that may name a broker that is gone. Reporting that is worse than
	// being taken out of rotation briefly.
	if s.registry != nil && s.registry.cfg.Enabled {
		if reason, ok := s.routingViewTrustworthy(); !ok {
			return Readiness{false, reason}
		}
	}

	return Readiness{true, ""}
}

// routingViewTrustworthy reports whether the routing snapshot can be believed.
//
// Staleness has two forms and both matter. The registry marks a snapshot stale
// when a refresh failed, which it does so rather than report an empty cluster.
// And a snapshot that is simply old -- refreshes slow, no errors -- has not been
// marked, but is just as wrong. Only an agent whose view is younger than the TTL
// it refreshes at should answer for the cluster.
func (s *StorageEngine) routingViewTrustworthy() (string, bool) {
	snap := s.registry.view()

	if snap.Stale {
		return fmt.Sprintf("the cluster view is stale: the last refresh failed at %s",
			snap.ObservedAt.Format(time.RFC3339)), false
	}

	if snap.ObservedAt.IsZero() {
		return "no cluster view has been built yet", false
	}

	// The TTL is the same one the registry refreshes at and the one a claim
	// survives, so a view older than that has outlived the interval it was
	// sampled over.
	ttl := s.registry.cfg.TTL
	if ttl <= 0 {
		ttl = DefaultOwnershipTTL
	}
	if age := time.Since(snap.ObservedAt); age > ttl {
		return fmt.Sprintf("the cluster view is %s old, past the %s refresh interval",
			age.Truncate(time.Second), ttl), false
	}

	return "", true
}

// ReadyHandler answers /ready for a Kubernetes readiness probe.
//
// It is a separate endpoint from /metrics because it is a decision rather than a
// report. A probe should be able to read one boolean, and a scrape should not
// have to interpret a hundred series to work out whether to route traffic here.
//
// The body carries the reason, so `kubectl describe pod` shows why a pod is not
// ready without anyone having to reproduce the reasoning.
func (s *StorageEngine) ReadyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := s.Readiness()

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		// No caching: a probe that caches a 200 reports readiness that has
		// already lapsed.
		w.Header().Set("Cache-Control", "no-store")

		if !state.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "not ready: %s\n", state.Reason)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "ready: %d partition(s) owned\n", len(s.OwnedPartitions()))
	})
}

// LiveHandler answers /live for a Kubernetes liveness probe.
//
// It reports success for as long as the process can serve a request, which
// includes the case where it owns nothing and the case where it has lost every
// claim. Both are still working readers.
//
// It deliberately does not consult the object store. A liveness probe that fails
// when a dependency is unreachable restarts the process during exactly the outage
// it cannot fix, turning a dependency problem into a crash loop.
func (s *StorageEngine) LiveHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "alive")
	})
}
