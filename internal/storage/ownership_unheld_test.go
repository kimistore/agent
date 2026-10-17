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
	"sync/atomic"
	"testing"
	"time"
)

// registeredUnheld puts a partition into the manager's table without holding it,
// which is the state startup leaves behind when the claim cannot be taken.
//
// That is exactly what happened on 2026-10-17: the agent's own previous process
// was still holding the claim when this one started, so the startup claim was
// refused -- correctly, because two processes must not share an identity -- and
// the partition sat in neither state that anything retried.
func registeredUnheld(om *ownershipManager, topic string, partition int32) *partitionOwnership {
	key := partitionDurabilityKey(topic, partition)
	p := &partitionOwnership{topic: topic, partition: partition}
	om.mu.Lock()
	om.parts[key] = p
	om.mu.Unlock()
	return p
}

// TestOwnership_RetriesAPartitionItNeverManagedToClaim is the regression for the
// gap found after the 2026-10-17 outage.
//
// Before this, a partition that was registered but never claimed was stranded:
// renewPartition skipped it because !held, and retryLost skipped it because it
// was not lost. Nothing would ever try again, and the agent sat idle and
// unserved until someone restarted it a second time.
func TestOwnership_RetriesAPartitionItNeverManagedToClaim(t *testing.T) {
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
		t.Fatalf("our manager: %v", err)
	}
	defer om.close()

	// The predecessor: same bucket, same agent id, still holding the claim when
	// this process started.
	peer, err := newOwnershipManager(context.Background(), store, testOwnership("agent-1", time.Minute))
	if err != nil {
		t.Fatalf("predecessor manager: %v", err)
	}
	defer peer.close()
	if _, err := peer.Claim(context.Background(), "orders", 0); err != nil {
		t.Fatalf("predecessor claim: %v", err)
	}

	p := registeredUnheld(om, "orders", 0)

	// While the predecessor's claim is live, taking it must fail. This is the
	// refusal that started the outage: "held at epoch 41705 by an agent using
	// our id".
	if om.takePartition(context.Background(), p) {
		t.Fatal("the partition was taken while a live claim existed")
	}
	if p.held.Load() {
		t.Fatal("a partition with a live claim on it is being served")
	}

	// The predecessor exits and its claim ages out. Nobody does anything about it.
	peer.close()
	expireAllClaims(t, store)

	// This agent must notice on its own, with no restart.
	waitForRecovery(t, 10*time.Second, "the unheld partition to be claimed", func() bool {
		return p.held.Load()
	})

	// And it must have derived the position before serving. This partition was
	// never held, so it has no position at all rather than a stale one, which
	// makes the recovery hook more necessary, not less.
	if n := recoveries.Load(); n == 0 {
		t.Error("the partition was claimed without re-deriving its log position from object storage")
	}

	if _, err := om.Claim(context.Background(), "orders", 0); err != nil {
		t.Errorf("Claim after the retry = %v, want nil", err)
	}
	if got := om.count(); got != 1 {
		t.Errorf("owned partitions after the retry = %d, want 1", got)
	}
}

// TestOwnership_UnheldRetryIsPaced checks that a partition another agent
// legitimately owns does not cost a compare-and-swap on every renewal tick.
//
// One CAS per TTL is already the cost of renewing a held claim, so matching that
// for unheld partitions is proportionate. One per tick would be a hot loop
// against the object store.
func TestOwnership_UnheldRetryIsPaced(t *testing.T) {
	const ttl = 400 * time.Millisecond
	store := newCASStore()

	om, err := newOwnershipManager(context.Background(), store, testOwnership("agent-1", ttl))
	if err != nil {
		t.Fatalf("our manager: %v", err)
	}
	defer om.close()

	// A live peer holds it for the whole measurement.
	peer, err := newOwnershipManager(context.Background(), store, testOwnership("agent-2", time.Minute))
	if err != nil {
		t.Fatalf("peer manager: %v", err)
	}
	defer peer.close()
	if _, err := peer.Claim(context.Background(), "orders", 0); err != nil {
		t.Fatalf("peer claim: %v", err)
	}

	p := registeredUnheld(om, "orders", 0)

	const ttlWindow = 4
	store.mu.Lock()
	before := store.getVersionCalls
	store.mu.Unlock()

	deadline := time.Now().Add(ttlWindow * ttl)
	for time.Now().Before(deadline) {
		om.retryUnheld(context.Background())
		time.Sleep(5 * time.Millisecond)
	}

	store.mu.Lock()
	after := store.getVersionCalls
	store.mu.Unlock()

	// Counted as conditional *reads*, because that is what a losing retry costs:
	// when a peer holds the claim, claim() sees it is live and returns without
	// writing, so a write counter would not move and this test would pass with
	// the pacing removed.
	if maxWrites := ttlWindow + 2; after-before > maxWrites {
		t.Errorf("%d claim reads in %d TTLs, want at most %d; an un-paced retry is a "+
			"hot loop against the object store", after-before, ttlWindow, maxWrites)
	}
	if p.held.Load() {
		t.Error("the partition was taken although a live peer owns it")
	}
}

// TestOwnership_UnheldRetryDoesNotStealAHeldPartition is the safety property:
// retrying must never take a partition a live peer owns.
func TestOwnership_UnheldRetryDoesNotStealAHeldPartition(t *testing.T) {
	const ttl = 300 * time.Millisecond
	store := newCASStore()

	peer, err := newOwnershipManager(context.Background(), store, testOwnership("agent-2", time.Minute))
	if err != nil {
		t.Fatalf("peer manager: %v", err)
	}
	defer peer.close()
	if _, err := peer.Claim(context.Background(), "orders", 0); err != nil {
		t.Fatalf("peer claim: %v", err)
	}

	om, err := newOwnershipManager(context.Background(), store, testOwnership("agent-1", ttl))
	if err != nil {
		t.Fatalf("our manager: %v", err)
	}
	defer om.close()

	registeredUnheld(om, "orders", 0)

	for i := 0; i < 20; i++ {
		om.retryUnheld(context.Background())
		time.Sleep(10 * time.Millisecond)
	}

	if om.parts[partitionDurabilityKey("orders", 0)].held.Load() {
		t.Error("this agent took a partition a live peer owns")
	}
	if _, err := peer.Claim(context.Background(), "orders", 0); err != nil {
		t.Errorf("the peer can no longer renew its claim: %v", err)
	}
}
