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
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// blipError is what a transient object-store outage looks like to the claim
// path. On 2026-10-17 RustFS returned connection-level failures (HTTP
// StatusCode 0) for about an hour; the agent recorded exactly one failed
// ownership renewal and fenced the partition permanently.
var blipError = errors.New("simulated object store outage")

// TestOwnership_RecoversFromTransientClaimLoss is the regression for the
// 9h43m outage on 2026-10-17.
//
// The old behaviour was deliberate and documented: a claim that could not be
// renewed was never retried in-process, because re-acquiring gives a newer epoch
// without a current view of the log. That reasoning is sound. The consequence
// was not: one transient object-store error fenced a partition until a human
// noticed ingest had stopped and restarted the agent nine hours later.
//
// The fix retries, and re-derives the log position from object storage before
// serving again, which is what makes retrying safe.
func TestOwnership_RecoversFromTransientClaimLoss(t *testing.T) {
	const ttl = 300 * time.Millisecond
	store := newCASStore()

	var recoveries atomic.Int32
	cfg := testOwnership("agent-1", ttl)
	cfg.OnReacquire = func(topic string, partition int32) error {
		recoveries.Add(1)
		return nil
	}

	om, err := newOwnershipManager(context.Background(), store, cfg)
	if err != nil {
		t.Fatalf("new ownership manager: %v", err)
	}
	defer om.close()

	if _, err := om.Claim(context.Background(), "orders", 0); err != nil {
		t.Fatalf("initial claim: %v", err)
	}
	if got := om.count(); got != 1 {
		t.Fatalf("owned partitions after claim = %d, want 1", got)
	}
	p := om.parts["orders/0"]

	// The blip: long enough that a renewal pass fails for a full TTL, which is
	// what declares the claim lost.
	store.mu.Lock()
	store.failPutVersion = blipError
	store.mu.Unlock()

	waitForRecovery(t, 5*time.Second, "the claim to be declared lost", func() bool { return p.lost.Load() })

	// While lost, the partition must stay fenced. That part is still correct.
	if _, err := om.Claim(context.Background(), "orders", 0); !errors.Is(err, ErrPartitionLost) {
		t.Errorf("Claim while lost = %v, want ErrPartitionLost: a lost partition must not serve writes", err)
	}

	// The store recovers. No restart, no operator.
	store.mu.Lock()
	store.failPutVersion = nil
	store.mu.Unlock()

	waitForRecovery(t, 5*time.Second, "the claim to be taken back", func() bool {
		return p.held.Load() && !p.lost.Load()
	})

	// Recovery must have run before serving resumed. A re-acquired claim on its
	// own is not enough: this agent would be writing at offsets a previous owner
	// had already handed out.
	if n := recoveries.Load(); n == 0 {
		t.Error("the claim came back without re-deriving the log position from object storage")
	}

	if _, err := om.Claim(context.Background(), "orders", 0); err != nil {
		t.Errorf("Claim after recovery = %v, want nil", err)
	}
	if got := om.count(); got != 1 {
		t.Errorf("owned partitions after recovery = %d, want 1", got)
	}
}

// TestOwnership_FailedRecoveryDoesNotServe checks the ordering that makes
// retrying safe at all.
//
// If the position cannot be re-derived, the partition must stay fenced even
// though the claim was successfully taken. A claim held with a position nobody
// can vouch for is worse than no claim, because Metadata advertises the
// partition as ours while our idea of where the log ends is stale.
func TestOwnership_FailedRecoveryDoesNotServe(t *testing.T) {
	const ttl = 300 * time.Millisecond
	store := newCASStore()

	var attempts atomic.Int32
	cfg := testOwnership("agent-1", ttl)
	cfg.OnReacquire = func(topic string, partition int32) error {
		attempts.Add(1)
		return errors.New("object storage still unhealthy")
	}

	om, err := newOwnershipManager(context.Background(), store, cfg)
	if err != nil {
		t.Fatalf("new ownership manager: %v", err)
	}
	defer om.close()

	if _, err := om.Claim(context.Background(), "orders", 0); err != nil {
		t.Fatalf("initial claim: %v", err)
	}
	p := om.parts["orders/0"]

	store.mu.Lock()
	store.failPutVersion = blipError
	store.mu.Unlock()

	waitForRecovery(t, 5*time.Second, "the claim to be declared lost", func() bool { return p.lost.Load() })
	store.mu.Lock()
	store.failPutVersion = nil
	store.mu.Unlock()

	// Let several recovery attempts happen while recovery keeps failing.
	waitForRecovery(t, 5*time.Second, "at least one recovery attempt", func() bool { return attempts.Load() >= 2 })
	// And give any that succeed a chance to show up as a held partition.
	time.Sleep(500 * time.Millisecond)

	if p.held.Load() {
		t.Error("the partition is being served even though its position could not be recovered; " +
			"this is the failure the recovery hook exists to prevent")
	}
	if !p.lost.Load() {
		t.Error("a failed recovery must leave the partition marked lost, not half-recovered")
	}
}

// waitForRecovery polls cond until it holds or a deadline passes, naming what it
// was waiting for. It is deliberately not waitFor: that one already exists in
// segmentcache_test.go with a different signature.
func waitForRecovery(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}
