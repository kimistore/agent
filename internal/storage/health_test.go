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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestReadiness_ReadyAfterStartup is the ordinary case: an agent that has
// recovered, holds its claims, and has a fresh view of the cluster.
func TestReadiness_ReadyAfterStartup(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a",
		WithAgentID("agent-a"),
		WithRegistry(testRegistry("agent-a", 1, "agent-a.kimi.internal", 19092)))
	defer engine.Close()

	if err := engine.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for p := 0; p < 2; p++ {
		if _, err := engine.Append("orders", int32(p), batchOfN(3), 3, true); err != nil {
			t.Fatalf("append %d: %v", p, err)
		}
	}
	refreshRouting(t, engine)

	if got := engine.Readiness(); !got.Ready {
		t.Fatalf("not ready after startup: %s", got.Reason)
	}
}

// TestReadiness_UnreadyWhenClaimsCannotBeRenewed is the case that matters. An
// agent that has not been able to renew for a full TTL may already have been
// superseded, and advertising it as the leader sends every producer to a broker
// that refuses the write.
func TestReadiness_UnreadyWhenClaimsCannotBeRenewed(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a",
		WithAgentID("agent-a"),
		WithRegistry(testRegistry("agent-a", 1, "agent-a.kimi.internal", 19092)))
	defer engine.Close()

	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(3), 3, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	refreshRouting(t, engine)

	if !engine.Readiness().Ready {
		t.Fatalf("not ready before the claim goes stale: %s", engine.Readiness().Reason)
	}

	// Backdate the last successful renewal past the claim TTL. The agent still
	// believes it holds the partition, which is exactly the dangerous state.
	engine.ownership.lastRenewOK.Store(time.Now().Add(-2 * engine.ownership.cfg.TTL).UnixNano())

	got := engine.Readiness()
	if got.Ready {
		t.Fatal("still ready after a full TTL without renewing a claim")
	}
	if !strings.Contains(got.Reason, "renew") {
		t.Errorf("the reason does not say what is wrong: %q", got.Reason)
	}
}

// TestReadiness_HoldingNothingIsReady is the guard against the most likely way to
// get this wrong. An agent assigned no partitions is perfectly healthy: it still
// serves reads from object storage, and it is exactly the redundancy a read path
// wants. Marking it unready would empty a healthy fleet.
func TestReadiness_HoldingNothingIsReady(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a",
		WithAgentID("agent-a"),
		WithRegistry(testRegistry("agent-a", 1, "agent-a.kimi.internal", 19092)))
	defer engine.Close()

	refreshRouting(t, engine)

	if got := engine.Readiness(); !got.Ready {
		t.Fatalf("an agent that owns nothing is not ready: %s", got.Reason)
	}
}

// TestReadiness_HandoverDoesNotMakeAnAgentUnready pins the reason the per-partition
// lost flag could not be used for readiness.
//
// A clean handover sets lost on the partition. An agent that handed a partition
// over on purpose is healthy, and must stay ready -- it is the release path
// doing its job, not a failure.
func TestReadiness_HandoverDoesNotMakeAnAgentUnready(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a",
		WithAgentID("agent-a"),
		WithRegistry(testRegistry("agent-a", 1, "agent-a.kimi.internal", 19092)))
	defer engine.Close()

	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(4), 4, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	refreshRouting(t, engine)

	if _, err := engine.DrainPartition(t.Context(), "orders", 0, DefaultHandoverBudget); err != nil {
		t.Fatalf("drain: %v", err)
	}

	if got := engine.Readiness(); !got.Ready {
		t.Fatalf("an agent that handed its partition over is not ready: %s", got.Reason)
	}
}

// TestReadiness_UnreadyOnAStaleView covers both forms of staleness. The registry
// marks a snapshot stale when a refresh fails; a snapshot that is merely old has
// not been marked, and is just as wrong.
func TestReadiness_UnreadyOnAStaleView(t *testing.T) {
	t.Run("marked stale by a failed refresh", func(t *testing.T) {
		store := newCASStore()
		engine := newOwningEngine(t, store, "agent-a",
			WithAgentID("agent-a"),
			WithRegistry(testRegistry("agent-a", 1, "agent-a.kimi.internal", 19092)))
		defer engine.Close()

		if err := engine.CreateTopic("orders", 1); err != nil {
			t.Fatalf("create topic: %v", err)
		}
		if _, err := engine.Append("orders", 0, batchOfN(3), 3, true); err != nil {
			t.Fatalf("append: %v", err)
		}
		refreshRouting(t, engine)

		snap := engine.Routing()
		snap.Stale = true
		engine.registry.snap = snap

		got := engine.Readiness()
		if got.Ready {
			t.Fatal("ready on a view marked stale by a failed refresh")
		}
		if !strings.Contains(got.Reason, "stale") {
			t.Errorf("reason does not mention staleness: %q", got.Reason)
		}
	})

	t.Run("simply too old", func(t *testing.T) {
		store := newCASStore()
		engine := newOwningEngine(t, store, "agent-a",
			WithAgentID("agent-a"),
			WithRegistry(testRegistry("agent-a", 1, "agent-a.kimi.internal", 19092)))
		defer engine.Close()

		if err := engine.CreateTopic("orders", 1); err != nil {
			t.Fatalf("create topic: %v", err)
		}
		if _, err := engine.Append("orders", 0, batchOfN(3), 3, true); err != nil {
			t.Fatalf("append: %v", err)
		}
		refreshRouting(t, engine)

		// Age the snapshot past the refresh interval without marking it stale,
		// which is what a slow refresh looks like.
		snap := engine.Routing()
		snap.ObservedAt = time.Now().Add(-2 * engine.registry.cfg.TTL)
		engine.registry.snap = snap

		got := engine.Readiness()
		if got.Ready {
			t.Fatal("ready on a cluster view older than the refresh interval")
		}
		if !strings.Contains(got.Reason, "old") {
			t.Errorf("reason does not mention age: %q", got.Reason)
		}
	})
}

// TestHealthEndpoints checks the HTTP contract a probe depends on: the status
// code, and the body explaining itself.
func TestHealthEndpoints(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a",
		WithAgentID("agent-a"),
		WithRegistry(testRegistry("agent-a", 1, "agent-a.kimi.internal", 19092)))
	defer engine.Close()

	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(3), 3, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	refreshRouting(t, engine)

	rec := httptest.NewRecorder()
	engine.ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/ready = %d, want 200; body %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("/ready Cache-Control = %q, want no-store; a probe that caches a 200 reports readiness that has lapsed", got)
	}

	// Liveness stays 200 through an ownership failure, which is the distinction
	// that stops a dependency problem becoming a crash loop.
	engine.ownership.lastRenewOK.Store(time.Now().Add(-2 * engine.ownership.cfg.TTL).UnixNano())
	if code := engine.Readiness().Ready; code {
		t.Fatal("expected the agent to be unready for this test to mean anything")
	}

	rec = httptest.NewRecorder()
	engine.LiveHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/live", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/live = %d, want 200 even when unready; restarting a broker that lost its claims "+
			"removes a working reader and gains nothing", rec.Code)
	}

	rec = httptest.NewRecorder()
	engine.ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/ready = %d when unready, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "not ready") {
		t.Errorf("/ready body does not explain itself: %q", rec.Body.String())
	}
}
