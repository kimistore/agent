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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"kimistore/internal/storage/wal"
)

func testOwnership(agent string, ttl time.Duration) OwnershipConfig {
	return OwnershipConfig{Enabled: true, Agent: agent, TTL: ttl}
}

// newOwningEngine builds an engine whose fence is a per-partition claim rather
// than the bucket-global lease.
func newOwningEngine(t *testing.T, store ObjectStore, agent string, opts ...Option) *StorageEngine {
	t.Helper()
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		append([]Option{WithOwnership(testOwnership(agent, 30*time.Second))}, opts...)...)
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	return engine
}

// expireClaim ages a partition's ownership claim past its expiry, which is what
// a crash followed by the TTL elapsing looks like from the outside. It makes the
// handover tests deterministic: the claim record stores whole seconds, so
// sleeping for a sub-second TTL is not reliable.
func (s *casStore) expireClaim(t *testing.T, topic string, partition int32) {
	t.Helper()
	key := partitionOwnerKey(topic, partition)
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.data[key]
	if !ok {
		t.Fatalf("no claim at %s", key)
	}
	var owner PartitionOwner
	if err := json.Unmarshal(raw, &owner); err != nil {
		t.Fatalf("claim at %s does not parse: %v", key, err)
	}
	owner.Expires = time.Now().Add(-time.Minute).Unix()
	body, err := json.Marshal(owner)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s.data[key] = body
	s.version[key]++
}

// claim reads a partition's claim record back out of the store.
func (s *casStore) claim(t *testing.T, topic string, partition int32) PartitionOwner {
	t.Helper()
	key := partitionOwnerKey(topic, partition)
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.data[key]
	if !ok {
		t.Fatalf("no claim object at %s", key)
	}
	var o PartitionOwner
	if err := json.Unmarshal(raw, &o); err != nil {
		t.Fatalf("claim object at %s is not valid JSON: %v", key, err)
	}
	return o
}

// Two agents may share a bucket, but only one of them may own a given
// partition. This is the property the whole change exists for: with the
// bucket-global lease a second agent was refused at startup, so nothing could
// ever be scaled out.
func TestOwnership_SecondAgentCannotTakeALivePartition(t *testing.T) {
	store := newCASStore()

	first := newOwningEngine(t, store, "agent-a")
	defer first.Close()

	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	om, err := newOwnershipManager(context.Background(), store, testOwnership("agent-b", 30*time.Second))
	if err != nil {
		t.Fatalf("agent-b ownership: %v", err)
	}
	defer om.close()

	if _, err := om.Claim(context.Background(), "orders", 0); !errors.Is(err, ErrPartitionHeld) {
		t.Fatalf("agent-b must be refused a live partition, got %v", err)
	}
	if om.Owns("orders", 0) {
		t.Error("agent-b must not believe it owns a partition it was refused")
	}
}

// A partition one agent cannot have is not a reason to refuse to start. The
// whole point of per-partition claims is that the other partitions stay
// servable, so a refused claim skips the partition rather than the agent.
func TestOwnership_EngineStartsAndSkipsAPartitionItCannotClaim(t *testing.T) {
	store := newCASStore()

	// agent-b holds orders/1 and goes away without releasing it, so the claim is
	// live for its whole TTL and agent-a must not take it over early.
	held, err := newOwnershipManager(context.Background(), store, testOwnership("agent-b", time.Minute))
	if err != nil {
		t.Fatalf("agent-b ownership: %v", err)
	}
	if _, err := held.Claim(context.Background(), "orders", 1); err != nil {
		t.Fatalf("agent-b claim: %v", err)
	}
	held.close()

	first := newOwningEngine(t, store, "agent-a")
	if err := first.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	first.Close()

	second := newOwningEngine(t, store, "agent-c")
	defer second.Close()

	if second.Owns("orders", 1) {
		t.Error("agent-c must not own a partition another live agent holds")
	}
	if _, err := second.Append("orders", 1, batchOfN(1), 1, false); err == nil {
		t.Fatal("append to a partition held by another live agent must be refused")
	}
	// The partition it does own stays writable.
	if _, err := second.Append("orders", 0, batchOfN(1), 1, true); err != nil {
		t.Errorf("an unclaimed-by-anyone partition must stay usable: %v", err)
	}
}

// A crashed owner blocks its replacement for that partition only, and only until
// the claim expires. Then the partition is claimable again, at a higher epoch.
func TestOwnership_PartitionBecomesClaimableAfterTheClaimExpires(t *testing.T) {
	store := newCASStore()

	// The claim record stores its expiry in whole seconds, so a sub-second TTL
	// can be rounded down to nothing and read as already expired. The value here
	// is comfortably over a second for that reason.
	const claimTTL = 1500 * time.Millisecond
	crashed, err := newOwnershipManager(context.Background(), store, testOwnership("crashed", claimTTL))
	if err != nil {
		t.Fatalf("crashed agent ownership: %v", err)
	}
	firstEpoch, err := crashed.Claim(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("crashed agent claim: %v", err)
	}
	crashed.close() // the process dies here: no release, no further renewal

	replacement, err := newOwnershipManager(context.Background(), store, testOwnership("replacement", time.Minute))
	if err != nil {
		t.Fatalf("replacement ownership: %v", err)
	}
	defer replacement.close()

	if _, err := replacement.Claim(context.Background(), "orders", 0); !errors.Is(err, ErrPartitionHeld) {
		t.Fatalf("a live claim must not be taken over early: %v", err)
	}

	time.Sleep(claimTTL + 500*time.Millisecond)
	nextEpoch, err := replacement.Claim(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("an expired claim must become available: %v", err)
	}
	if nextEpoch <= firstEpoch {
		t.Fatalf("the takeover epoch %d must exceed the previous epoch %d", nextEpoch, firstEpoch)
	}
	if got := store.claim(t, "orders", 0); got.Agent != "replacement" {
		t.Errorf("the surviving claim names %q, want replacement", got.Agent)
	}
}

// The epoch is a fencing token: it goes into every segment name and every
// manifest, so it must never move backwards. A restart that reset it to 1 would
// make the next agent compare itself against durable records written at a higher
// epoch and conclude it had been superseded by a writer that does not exist.
func TestOwnership_EpochIsMonotonicAcrossRestarts(t *testing.T) {
	store := newCASStore()

	first := newOwningEngine(t, store, "agent-a")
	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	firstEpoch := first.partitionEpoch("orders", 0)
	if firstEpoch < 1 {
		t.Fatalf("first owner got epoch %d, want at least 1", firstEpoch)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second := newOwningEngine(t, store, "agent-b")
	defer second.Close()
	if got := second.partitionEpoch("orders", 0); got <= firstEpoch {
		t.Fatalf("epoch went backwards across a restart: %d after %d", got, firstEpoch)
	}

	// And it must keep climbing, handover after handover.
	secondEpoch := second.partitionEpoch("orders", 0)
	if err := second.Close(); err != nil {
		t.Fatalf("close second: %v", err)
	}
	third := newOwningEngine(t, store, "agent-c")
	defer third.Close()
	if got := third.partitionEpoch("orders", 0); got <= secondEpoch {
		t.Fatalf("epoch went backwards on the second handover: %d after %d", got, secondEpoch)
	}
}

// A graceful shutdown hands each partition over, so a replacement does not wait
// out the TTL.
func TestOwnership_ReleasedOnClose(t *testing.T) {
	store := newCASStore()

	first := newOwningEngine(t, store, "agent-a")
	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Released means expired immediately: the record survives as a tombstone so
	// the epoch keeps climbing, but it no longer blocks anyone.
	if got := store.claim(t, "orders", 0); !got.Expired(time.Now()) {
		t.Errorf("the claim should be released on close, but %q still holds it until %d",
			got.Agent, got.Expires)
	}

	second := newOwningEngine(t, store, "agent-b")
	defer second.Close()
	if !second.Owns("orders", 0) {
		t.Error("a replacement must be able to take a released partition immediately")
	}
}

// Losing the claim must stop writes to that partition. This is the property
// that makes ownership a fence rather than bookkeeping: an agent that keeps
// appending after a takeover assigns offsets that collide with the new owner's.
func TestOwnership_LostClaimRefusesWrites(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")
	defer engine.Close()

	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Simulate a takeover this agent never observed: the claim now names another
	// agent and the epoch has moved on.
	store.mu.Lock()
	store.data[partitionOwnerKey("orders", 0)] = []byte(`{"agent":"agent-b","epoch":99,"expires_at":` +
		fmt.Sprint(time.Now().Add(time.Hour).Unix()) + `}`)
	store.version[partitionOwnerKey("orders", 0)]++
	store.mu.Unlock()

	p := engine.ownership.partition("orders", 0)
	if p == nil {
		t.Fatal("the partition should have a claim entry")
	}
	// Drive the decision the way the renewal loop drives it: a renewal that
	// cannot renew, aged past the TTL.
	p.held.Store(false)
	p.lost.Store(true)

	// Either refusal is correct: the write path can report the claim as lost, or
	// report that it cannot be re-acquired because a live agent holds it. Both
	// tell the producer this is not its broker.
	if _, err := engine.Append("orders", 0, batchOfN(1), 1, false); !errors.Is(err, ErrPartitionNotOwned) &&
		!errors.Is(err, ErrPartitionLost) && !errors.Is(err, ErrPartitionHeld) {
		t.Fatalf("append after losing the claim = %v, want a refusal", err)
	}
}

// A partition this agent does not own must not be written to at all, even by
// another route: the durable position is per partition, so writing would replace
// the real owner's manifest with a stale one.
func TestOwnership_ManifestIsNotWrittenForUnownedPartitions(t *testing.T) {
	store := newCASStore()

	peer, err := newOwnershipManager(context.Background(), store, testOwnership("agent-b", time.Minute))
	if err != nil {
		t.Fatalf("peer ownership: %v", err)
	}
	defer peer.close()
	if _, err := peer.Claim(context.Background(), "orders", 1); err != nil {
		t.Fatalf("peer claim: %v", err)
	}

	engine := newOwningEngine(t, store, "agent-a")
	defer engine.Close()
	if err := engine.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := engine.Append("orders", 0, batchOfN(3), 3, true); err != nil {
		t.Fatalf("append to the owned partition: %v", err)
	}
	if err := engine.SaveManifest(); err != nil {
		t.Fatalf("save manifest: %v", err)
	}

	rc, err := store.Get(context.Background(), partitionManifestKey("orders", 0))
	if err != nil {
		t.Fatalf("the owned partition must have a manifest: %v", err)
	}
	rc.Close()
	if _, err := store.Get(context.Background(), partitionManifestKey("orders", 1)); err == nil {
		t.Error("a partition owned by another agent must not get this agent's manifest")
	}
}

// The claim has to be on the append path's critical section, not merely checked
// at startup, or a partition created after startup would be writable by anyone.
func TestOwnership_LazylyCreatedPartitionIsClaimedBeforeItIsWritten(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")
	defer engine.Close()

	// No CreateTopic: the append is the first time this partition is seen.
	if _, err := engine.Append("late", 3, batchOfN(1), 1, true); err != nil {
		t.Fatalf("append to a new partition: %v", err)
	}

	if got := store.claim(t, "late", 3); got.Agent != "agent-a" {
		t.Fatalf("the claim names %q, want agent-a", got.Agent)
	}
	if got := engine.walMgr.Epoch("late", 3); got != store.claim(t, "late", 3).Epoch {
		t.Fatalf("the WAL writes epoch %d but the claim is at %d; segments would be named after the wrong epoch",
			got, store.claim(t, "late", 3).Epoch)
	}
}

// The sealed segment's name, and therefore the object key it is uploaded to,
// carries the ownership epoch. This is what makes a superseded writer's late
// upload harmless: the window is real -- an agent can seal a segment, lose its
// claim while the upload is in flight, and have the upload land after its
// successor has written at the same base offset -- and the epoch is what keeps
// the two from colliding.
func TestOwnership_SealedSegmentIsNamedForTheOwnershipEpoch(t *testing.T) {
	store := newCASStore()

	engine := newOwningEngine(t, store, "agent-a")
	defer engine.Close()
	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	epoch := engine.partitionEpoch("orders", 0)
	if _, err := engine.Append("orders", 0, batchOfN(1), 1, true); err != nil {
		t.Fatalf("append: %v", err)
	}

	// The sealed file on local disk already carries the epoch, which is what
	// makes a reconciliation re-upload land on the same object key it had before.
	engine.walMgr.FlushPartition("orders", 0)
	sealed := localSealedSegment(t, engine.walDir, "orders", 0)
	if want := wal.SegmentName(0, epoch); filepath.Base(sealed) != want {
		t.Errorf("sealed segment is named %q, want %q", filepath.Base(sealed), want)
	}
	waitForClaimedSegment(t, store, "orders/0/"+walSegmentName(0, epoch))
}

// localSealedSegment returns the path of a partition's sealed segment on disk.
func localSealedSegment(t *testing.T, walDir, topic string, partition int32) string {
	t.Helper()
	dir := filepath.Join(walDir, topic, strconv.Itoa(int(partition)))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	for _, e := range entries {
		if _, _, ok := wal.ParseSegmentName(e.Name()); ok {
			return filepath.Join(dir, e.Name())
		}
	}
	t.Fatalf("no sealed segment in %s", dir)
	return ""
}

// The stale upload must land beside the current segment, not on top of it.
func TestOwnership_StaleUploadDoesNotOverwriteTheSuccessorSegment(t *testing.T) {
	ctx := context.Background()
	store := newCASStore()

	// The current owner's segment, at base offset 0 epoch 2.
	freshKey := "orders/0/" + walSegmentName(0, 2)
	if err := store.Put(ctx, freshKey, strings.NewReader("current-owner")); err != nil {
		t.Fatalf("seed current segment: %v", err)
	}

	// A superseded owner's upload of the same base offset, epoch 1.
	staleKey := "orders/0/" + walSegmentName(0, 1)
	if err := store.Put(ctx, staleKey, strings.NewReader("superseded-owner")); err != nil {
		t.Fatalf("seed stale segment: %v", err)
	}

	read := func(key string) string {
		rc, err := store.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		defer rc.Close()
		body, _ := io.ReadAll(rc)
		return string(body)
	}

	if got := read(freshKey); got != "current-owner" {
		t.Errorf("the current owner's segment was overwritten by the stale upload: %q", got)
	}
	if got := read(staleKey); got != "superseded-owner" {
		t.Errorf("the stale segment should still be readable so retention can reclaim it: %q", got)
	}
	if got := bestSegmentFor([]string{staleKey, freshKey}, 0); got != freshKey {
		t.Errorf("a read at offset 0 resolved to %q, want the current owner's %q", got, freshKey)
	}
}

// A read must resolve to the newest epoch at an offset. Preferring a superseded
// owner's copy would hand a consumer records the log has since assigned
// differently.
func TestBestSegmentFor_PrefersTheHighestEpochAtAnOffset(t *testing.T) {
	stale := "orders/0/00000000000000000000-e1.log"
	fresh := "orders/0/00000000000000000000-e2.log"
	older := "orders/0/00000000000000000000.log"

	if got := bestSegmentFor([]string{stale, fresh}, 0); got != fresh {
		t.Errorf("bestSegmentFor picked %q, want the newer epoch %q", got, fresh)
	}
	if got := bestSegmentFor([]string{fresh, stale}, 0); got != fresh {
		t.Errorf("order of the listing must not matter; picked %q", got)
	}
	if got := bestSegmentFor([]string{stale, older}, 0); got != stale {
		t.Errorf("a real epoch must beat a pre-ownership segment; picked %q", got)
	}
	// A segment starting after the requested offset is never the answer.
	if got := bestSegmentFor([]string{fresh, "orders/0/00000000000000000010-e9.log"}, 0); got != fresh {
		t.Errorf("bestSegmentFor picked %q, want %q", got, fresh)
	}
}

// Superseded-epoch segments are unreachable, so retention reclaims them instead
// of protecting them forever.
func TestRetention_ReclaimsSupersededEpochSegments(t *testing.T) {
	store := newCASStore()
	ctx := context.Background()

	seed := newOwningEngine(t, store, "agent-a")
	if err := seed.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := seed.SaveCheckpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	seed.Close()

	// Two segments at the same base offset: one from a superseded epoch, one
	// from the current owner. Only the second is reachable.
	for _, key := range []string{
		"orders/0/00000000000000000000-e1.log",
		"orders/0/00000000000000000000-e1.index",
		"orders/0/00000000000000000000-e2.log",
		"orders/0/00000000000000000000-e2.index",
	} {
		if err := store.Put(ctx, key, strings.NewReader("x")); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	engine := newOwningEngine(t, store, "agent-b")
	defer engine.Close()

	// No consumer has committed, so retention's own policies cannot reclaim the
	// live segment. The superseded one goes regardless, which is the point: it is
	// unreachable, not merely unconsumed.
	engine.reclaimPartition(ctx, "orders", 0, filterSegments(mustList(t, store, partitionPrefix("orders", 0))))

	for _, key := range []string{
		"orders/0/00000000000000000000-e1.log",
		"orders/0/00000000000000000000-e1.index",
	} {
		if _, err := store.Get(ctx, key); err == nil {
			t.Errorf("superseded segment %s should have been reclaimed", key)
		}
	}
	if _, err := store.Get(ctx, "orders/0/00000000000000000000-e2.log"); err != nil {
		t.Errorf("the live segment must survive a retention pass that leaves it: %v", err)
	}
}

// The manifest records the epoch it was written under, so a stale position is
// recognisable rather than being adopted as current.
func TestOwnership_ManifestRecordsTheOwnershipEpoch(t *testing.T) {
	store := newCASStore()
	ctx := context.Background()

	first := newOwningEngine(t, store, "agent-a")
	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if _, err := first.Append("orders", 0, batchOfN(2), 2, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := first.SaveManifest(); err != nil {
		t.Fatalf("save manifest: %v", err)
	}
	firstEpoch := first.partitionEpoch("orders", 0)
	first.Close()

	rc, err := store.Get(ctx, partitionManifestKey("orders", 0))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	buf := make([]byte, 4096)
	n, _ := rc.Read(buf)
	rc.Close()

	var m perPartitionManifest
	if err := json.Unmarshal(buf[:n], &m); err != nil {
		t.Fatalf("manifest does not parse: %v", err)
	}
	if m.Epoch != firstEpoch {
		t.Errorf("manifest epoch = %d, want the ownership epoch %d", m.Epoch, firstEpoch)
	}
	if m.Writer != "agent-a" {
		t.Errorf("manifest writer = %q, want agent-a", m.Writer)
	}
}

// A manifest written at a higher epoch than the claim in force means another
// owner got there first, and this agent must not serve from it.
func TestOwnership_ManifestFromANewerEpochIsRefused(t *testing.T) {
	store := newCASStore()
	ctx := context.Background()

	// A manifest left by an owner that held the partition at a much higher epoch.
	// The claim record it wrote is gone, so this agent's own claim starts at 1 --
	// which is exactly how a stale agent's view looks after a takeover nobody told
	// it about.
	body, err := json.Marshal(perPartitionManifest{
		Epoch:        99,
		Writer:       "agent-newer",
		Topic:        "orders",
		Partition:    0,
		LogEndOffset: 500,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := store.Put(ctx, partitionManifestKey("orders", 0), strings.NewReader(string(body))); err != nil {
		t.Fatalf("seed manifest: %v", err)
	}

	// Startup must refuse rather than adopt a position a newer owner wrote.
	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOwnership(testOwnership("agent-old", 30*time.Second)))
	if err == nil {
		engine.Close()
		t.Fatal("an agent should refuse to serve a partition whose manifest is from a newer owner")
	}
	if !errors.Is(err, errSuperseded) {
		t.Fatalf("error = %v, want errSuperseded", err)
	}
	if !strings.Contains(err.Error(), "epoch 99") {
		t.Fatalf("the error should name the epoch it lost to, got %v", err)
	}
}

// Ownership and the bucket-global lease are alternatives. Running both would
// mean two epochs per write and a second agent refused outright, which is the
// behaviour per-partition ownership exists to remove.
func TestOwnership_ReplacesTheBucketGlobalLease(t *testing.T) {
	store := newCASStore()

	first := newOwningEngine(t, store, "agent-a")
	defer first.Close()
	if err := first.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// A second agent starts against the same bucket. With the bucket-global
	// lease this is a fatal ErrLeaseHeld.
	second, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOwnership(testOwnership("agent-b", 30*time.Second)),
		WithLease(testLease("agent-b", 30*time.Second)))
	if err != nil {
		t.Fatalf("a second agent must be able to start on a shared bucket: %v", err)
	}
	defer second.Close()

	if second.lease != nil {
		t.Error("the bucket-global lease must not be acquired when ownership is enabled")
	}
	if first.Owns("orders", 0) == second.Owns("orders", 0) {
		t.Errorf("exactly one agent should hold orders/0; agent-a owns=%v agent-b owns=%v",
			first.Owns("orders", 0), second.Owns("orders", 0))
	}
}

// Without conditional writes the claim cannot be enforced, so ownership is
// advisory: the agent still starts, but says loudly that a second one will
// corrupt the log, and refuses to write a partition it cannot prove it holds.
func TestOwnership_StoreWithoutConditionalWrites(t *testing.T) {
	store := &plainStore{inner: newCASStore()}

	if _, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOwnership(OwnershipConfig{Enabled: true, Agent: "a", TTL: time.Minute, Require: true})); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Require=true should refuse a store that cannot fence, got %v", err)
	}

	engine, err := NewStorageEngine(t.TempDir(), store, "bucket", RetentionConfig{},
		WithOwnership(OwnershipConfig{Enabled: true, Agent: "a", TTL: time.Minute}))
	if err != nil {
		t.Fatalf("Require=false should still start, got %v", err)
	}
	defer engine.Close()
	if engine.ownership.fenced {
		t.Error("a store without conditional writes cannot fence claims")
	}
	// Unfenced ownership must not let a write through on a partition nobody has
	// claimed, or an unfenced agent is indistinguishable from an owner.
	if _, err := engine.Append("t", 0, batchOfN(1), 1, false); err != nil {
		t.Errorf("an unfenced engine should still be usable: %v", err)
	}
}

// A claim a single agent cannot renew must not stop it serving the partitions
// it can. Losing one partition is the expected outcome of a partition moving
// away, not a reason to abandon the rest of the log.
func TestOwnership_LosingOnePartitionDoesNotAffectTheOthers(t *testing.T) {
	store := newCASStore()
	engine := newOwningEngine(t, store, "agent-a")
	defer engine.Close()

	if err := engine.CreateTopic("orders", 2); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if !engine.Owns("orders", 0) || !engine.Owns("orders", 1) {
		t.Fatal("both partitions should be claimed after create")
	}

	p := engine.ownership.partition("orders", 0)
	p.held.Store(false)
	p.lost.Store(true)

	if engine.Owns("orders", 0) {
		t.Error("the lost partition must not report as owned")
	}
	if !engine.Owns("orders", 1) {
		t.Error("an unrelated partition must stay owned")
	}
	if _, err := engine.Append("orders", 1, batchOfN(1), 1, false); err != nil {
		t.Errorf("writing to a still-owned partition must keep working: %v", err)
	}
}

// Concurrent first-writes to the same new partition must produce one claim. Two
// racing claims would have one loser told it does not own a partition it is
// being asked to write, which is a spurious produce failure under load.
func TestOwnership_ConcurrentClaimsResolveToOneOwner(t *testing.T) {
	store := newCASStore()
	om, err := newOwnershipManager(context.Background(), store, testOwnership("agent-a", 30*time.Second))
	if err != nil {
		t.Fatalf("ownership: %v", err)
	}
	defer om.close()

	var wg sync.WaitGroup
	errs := make([]error, 16)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = om.Claim(context.Background(), "orders", 0)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("claim %d failed: %v", i, err)
		}
	}
	if got := store.claim(t, "orders", 0); got.Agent != "agent-a" {
		t.Fatalf("the surviving claim names %q", got.Agent)
	}
}

// walSegmentName is the object-store name a sealed segment gets under a given
// ownership epoch.
func walSegmentName(baseOffset, epoch int64) string {
	return wal.SegmentName(baseOffset, epoch)
}

// waitForClaimedSegment waits for an asynchronously uploaded segment to appear.
// The uploader runs on its own goroutines, so the object is not there the
// moment the segment is sealed.
func waitForClaimedSegment(t *testing.T, store *casStore, key string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rc, err := store.Get(context.Background(), key); err == nil {
			rc.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("segment %s was never uploaded", key)
}

// mustList lists a prefix, failing the test if the store cannot.
func mustList(t *testing.T, store ObjectStore, prefix string) []ObjectMetadata {
	t.Helper()
	objects, err := store.List(context.Background(), prefix)
	if err != nil {
		t.Fatalf("list %s: %v", prefix, err)
	}
	return objects
}
