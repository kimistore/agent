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
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"kimistore/internal/metrics"
)

// The writer lease (lease.go) makes an agent the only writer of a whole bucket.
// That is the right shape while there is one agent, and the wrong shape as soon
// as there are two: nothing can be scaled out, and one slow partition cannot be
// moved to a healthy agent without moving everything.
//
// Partition ownership narrows the claim from the bucket to the (topic,
// partition), so an agent that cannot take one partition leaves the rest of the
// log alone. It is the same claim, the same expiring record and the same
// compare-and-swap, just at a scope that lets more than one agent be useful.
//
// There is exactly one fence at a time. When ownership is enabled the
// bucket-global lease is not acquired at all: two overlapping fences would
// mean two epochs to reason about, and the coarse one would refuse every second
// agent, which is the thing this change exists to allow. With ownership
// disabled the bucket lease behaves exactly as before and segments are named
// without an epoch, so an existing bucket and its keys are untouched.
const ownersPrefix = "_owners/"

// partitionOwnerKey is the object one partition's claim lives at.
func partitionOwnerKey(topic string, partition int32) string {
	return ownersPrefix + topic + "/" + strconv.Itoa(int(partition))
}

// PartitionOwner is an exclusive, expiring claim on one partition.
//
// It mirrors Lease deliberately: same fields, same CAS protocol, same
// expiring-record semantics. The epoch is per partition and monotonic, which
// makes it a fencing token in two places at once -- the segment name, so a
// superseded writer's upload lands beside its successor's rather than on top
// of it, and the partition manifest, so a stale position is recognised as such
// on recovery.
type PartitionOwner struct {
	Agent   string `json:"agent"`
	Epoch   int64  `json:"epoch"`
	Expires int64  `json:"expires_at"`
}

// Expired reports whether the claim is no longer held, judged against now.
func (o PartitionOwner) Expired(now time.Time) bool {
	return o.Expires <= now.Unix()
}

// OwnershipConfig configures per-partition ownership.
//
// A zero value means "let the storage layer choose the defaults".
type OwnershipConfig struct {
	// Enabled turns per-partition claims on. It is on by default; the
	// bucket-global writer lease is used only when it is off.
	Enabled bool
	// Agent identifies this agent in every claim it holds. It must be stable
	// across restarts, because it is what tells a renewal of our own claim from
	// a claim another agent has taken over.
	Agent string
	// TTL is how long a claim survives without renewal, and therefore how long
	// a crashed agent blocks its replacement for that partition.
	TTL time.Duration
	// Require makes an object store that cannot make writes conditional a
	// startup failure instead of a degraded, unfenced run.
	Require bool
	// OnLost is called once for each partition this agent stops holding, after
	// the claim is marked lost.
	//
	// The engine uses it to discard the in-memory position it accumulated for
	// that partition. That position is a mix of records it appended locally and
	// records confirmed in object storage, and once the claim is gone none of it
	// can be told apart from what the new owner has replaced. Keeping it would
	// let this agent report a log end -- to a consumer, or to a failover
	// candidate -- that only its own unflushed WAL could justify.
	OnLost func(topic string, partition int32)
}

// DefaultOwnershipTTL is the default per-partition claim lifetime.
const DefaultOwnershipTTL = 30 * time.Second

// claimAttempts is how many times a compare-and-swap is retried before the
// contention is reported as another holder.
const claimAttempts = 5

// partitionOwnership is this agent's live view of one partition's claim.
type partitionOwnership struct {
	topic     string
	partition int32

	epoch atomic.Int64
	held  atomic.Bool
	lost  atomic.Bool

	// lastRenew is when the claim was last confirmed, used to decide when a
	// failed renewal has aged past the TTL and the claim is really gone.
	lastRenew atomic.Int64

	// claimMu serialises compare-and-swap attempts for this partition. Two
	// concurrent first-writes to the same partition must not both race to
	// create the claim: the loser would be told it does not own a partition it
	// is being asked to write. Claims for different partitions proceed in
	// parallel, because a slow claim on one partition must not delay another's.
	claimMu sync.Mutex
}

// ownershipManager holds every partition claim this agent has, renews them, and
// releases them on shutdown.
type ownershipManager struct {
	cfg    OwnershipConfig
	agent  string
	store  ConditionalObjectStore
	fenced bool

	mu    sync.RWMutex
	parts map[string]*partitionOwnership

	quit chan struct{}
	wg   sync.WaitGroup
}

// newOwnershipManager starts per-partition ownership.
//
// It fails only when the object store cannot make writes conditional and the
// operator asked for that to be fatal. Otherwise ownership is advisory: the
// agent still refuses to write a partition it cannot prove it owns, which is
// the safe direction, and says loudly that a second agent will corrupt the log.
func newOwnershipManager(ctx context.Context, store ObjectStore, cfg OwnershipConfig) (*ownershipManager, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.Agent == "" {
		cfg.Agent = defaultAgentID()
	}
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultOwnershipTTL
	}

	om := &ownershipManager{
		cfg:    cfg,
		agent:  cfg.Agent,
		parts:  make(map[string]*partitionOwnership),
		quit:   make(chan struct{}),
		store:  nil,
		fenced: true,
	}

	cond, ok := store.(ConditionalObjectStore)
	if !ok {
		if cfg.Require {
			return nil, fmt.Errorf("%w: %T cannot fence partition writers, and conditional writes are required", ErrUnsupported, store)
		}
		om.fenced = false
		log.Printf("Ownership: %T cannot make writes conditional, so per-partition claims are advisory only; "+
			"running a second agent against this bucket will corrupt the log", store)
		return om, nil
	}
	om.store = cond

	om.wg.Add(1)
	go om.renewLoop(ctx)

	return om, nil
}

// partition returns the live claim for a partition, or nil when this agent has
// never claimed it.
func (om *ownershipManager) partition(topic string, partition int32) *partitionOwnership {
	if om == nil {
		return nil
	}
	key := partitionDurabilityKey(topic, partition)
	om.mu.RLock()
	p := om.parts[key]
	om.mu.RUnlock()
	return p
}

// Claim takes or renews this agent's claim on one partition and returns the
// ownership epoch in force.
//
// It is safe to call on every append: a partition already claimed costs a map
// lookup. The compare-and-swap happens only on the first claim and after a
// takeover, so contention lands on ownership changes and never on the append
// path.
func (om *ownershipManager) Claim(ctx context.Context, topic string, partition int32) (int64, error) {
	if om == nil {
		return 0, nil
	}
	if !om.fenced {
		return 0, nil
	}

	key := partitionDurabilityKey(topic, partition)
	om.mu.RLock()
	p := om.parts[key]
	om.mu.RUnlock()

	if p == nil {
		p = &partitionOwnership{topic: topic, partition: partition}
		om.mu.Lock()
		if existing, ok := om.parts[key]; ok {
			p = existing
		} else {
			om.parts[key] = p
		}
		om.mu.Unlock()
	}

	// Already held: the caller is an ordinary append and must not pay for an
	// object-store round trip.
	if p.held.Load() {
		return p.epoch.Load(), nil
	}

	p.claimMu.Lock()
	defer p.claimMu.Unlock()
	// Re-check under the claim lock: a concurrent Claim may have won the race
	// while this one waited.
	if p.held.Load() {
		return p.epoch.Load(), nil
	}

	// A claim that was lost is never retried in-process. Re-acquiring would give
	// this agent a newer epoch but not a current view of the log: whoever took
	// the partition over has been assigning offsets from a position this agent
	// cannot see, so writing here would reissue offsets that are already taken.
	// The partition stays fenced until the process restarts and recovers.
	if p.lost.Load() {
		return p.epoch.Load(), fmt.Errorf("%w: %s/%d lost its claim at epoch %d and will not re-acquire it in this process",
			ErrPartitionLost, topic, partition, p.epoch.Load())
	}

	// Bound acquisition so a wedged object store cannot hang a produce request
	// or a startup.
	claimCtx, cancel := context.WithTimeout(ctx, om.cfg.TTL)
	defer cancel()

	owner, err := om.claim(claimCtx, topic, partition, 0)
	if err != nil {
		metrics.OwnershipClaimFailures.Inc()
		return p.epoch.Load(), err
	}

	p.epoch.Store(owner.Epoch)
	p.held.Store(true)
	p.lost.Store(false)
	p.lastRenew.Store(time.Now().UnixNano())

	metrics.PartitionsOwned.Set(float64(om.count()))
	log.Printf("Ownership: %s/%d claimed by %q at epoch %d (ttl %s)", topic, partition, om.agent, owner.Epoch, om.cfg.TTL)
	return owner.Epoch, nil
}

// claim performs one compare-and-swap against the partition's claim object,
// retrying while the object is being written by someone else.
// claim acquires or renews this agent's claim on a partition.
//
// heldEpoch is the epoch the caller believes it holds, or zero when it holds
// nothing yet. It is what distinguishes re-acquiring our own live record from
// losing the partition to a peer that is using the same agent id: a live claim
// above our epoch is a takeover, not a renewal.
func (om *ownershipManager) claim(ctx context.Context, topic string, partition int32, heldEpoch int64) (PartitionOwner, error) {
	key := partitionOwnerKey(topic, partition)
	var lastErr error

	for attempt := 0; attempt < claimAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return PartitionOwner{}, ctx.Err()
			case <-time.After(time.Duration(attempt) * 50 * time.Millisecond):
			}
		}

		raw, version, found, err := om.store.GetVersion(ctx, key)
		if err != nil {
			lastErr = err
			continue
		}

		var current PartitionOwner
		if found {
			// A claim record that cannot be parsed is treated as expired rather
			// than as a permanent block: the worst case is one extra renewal
			// cycle of contention, and the alternative is a corrupt record
			// wedging the partition forever.
			if jsonErr := json.Unmarshal(raw, &current); jsonErr != nil {
				log.Printf("Ownership: %s is unreadable (%v); treating it as expired", key, jsonErr)
				current = PartitionOwner{}
			}
		}

		now := time.Now()
		next := PartitionOwner{Agent: om.agent, Epoch: 1, Expires: now.Add(om.cfg.TTL).Unix()}

		switch {
		case !found:
			// Create it. The empty version means "only if absent", which is
			// what makes two agents racing to start resolve to one winner.
		case !current.Expired(now) && current.Agent != om.agent:
			agent, expires := current.Agent, time.Unix(current.Expires, 0).UTC().Format(time.RFC3339)
			return PartitionOwner{}, fmt.Errorf("%w: %s is held by %q until %s (epoch %d)",
				ErrPartitionHeld, key, agent, expires, current.Epoch)
		case !current.Expired(now) && current.Epoch > heldEpoch:
			// The claim names us and is still live, but it is at a higher epoch
			// than the one we hold. That is not us re-acquiring our own record:
			// it is another process using the same agent id, and it has already
			// taken the partition from us.
			//
			// Renewing here would hand the partition back and forth forever. Two
			// agents sharing an id would each bump the epoch past the other on
			// every renewal tick, both would keep believing they owned the
			// partition, and both would acknowledge writes -- with diverged log
			// end offsets. An identity collision is not a condition to recover
			// from; it is a misconfiguration to refuse.
			//
			// An *expired* claim above our epoch is still fair game, which is
			// the case where the holder died and we are genuinely taking over.
			return PartitionOwner{}, fmt.Errorf("%w: %s is held at epoch %d by an agent using our id %q "+
				"(we hold epoch %d); every agent must have a distinct KIMISTORE_AGENT_ID",
				ErrPartitionHeld, key, current.Epoch, om.agent, heldEpoch)
		default:
			// Renew our own claim, or take over one that has expired. The epoch
			// only ever moves forward, so a takeover is always visibly newer
			// than what it replaces.
			next.Epoch = current.Epoch + 1
			if current.Epoch < 1 {
				next.Epoch = 1
			}
		}

		body, err := json.Marshal(next)
		if err != nil {
			return PartitionOwner{}, err
		}

		if _, err := om.store.PutVersion(ctx, key, body, version); err != nil {
			if errors.Is(err, ErrVersionMismatch) {
				lastErr = ErrVersionMismatch
				continue // someone else wrote first; read again and decide
			}
			return PartitionOwner{}, fmt.Errorf("ownership: could not write %s: %w", key, err)
		}
		return next, nil
	}

	if lastErr == nil {
		lastErr = ErrPartitionHeld
	}
	return PartitionOwner{}, fmt.Errorf("could not acquire %s after %d attempts: %w", key, claimAttempts, lastErr)
}

// renewLoop keeps every claim alive until the engine shuts down.
func (om *ownershipManager) renewLoop(ctx context.Context) {
	defer om.wg.Done()

	interval := om.cfg.TTL / 3
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-om.quit:
			return
		case <-ticker.C:
			om.renewAll(ctx)
		}
	}
}

// renewAll makes one renewal attempt per claimed partition.
//
// A partition that cannot be renewed is not a reason to abandon the others: an
// agent that loses one partition still owns the rest, and fencing it out of
// that one partition is the correct and useful outcome.
func (om *ownershipManager) renewAll(ctx context.Context) {
	for _, p := range om.snapshot() {
		om.renewPartition(ctx, p)
	}
}

func (om *ownershipManager) renewPartition(ctx context.Context, p *partitionOwnership) {
	p.claimMu.Lock()
	defer p.claimMu.Unlock()

	if !p.held.Load() {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, om.cfg.TTL)
	defer cancel()

	owner, err := om.claim(ctx, p.topic, p.partition, p.epoch.Load())
	if err != nil {
		metrics.OwnershipRenewalFailures.Inc()

		age := time.Since(time.Unix(0, p.lastRenew.Load()))
		if age > om.cfg.TTL {
			if p.held.CompareAndSwap(true, false) {
				p.lost.Store(true)
				log.Printf("Ownership: LOST %s/%d after failing to renew for %s; %v. Writes to it are refused "+
					"because another writer may have taken the partition.",
					p.topic, p.partition, age.Truncate(time.Second), err)
				if om.cfg.OnLost != nil {
					om.cfg.OnLost(p.topic, p.partition)
				}
			}
			metrics.PartitionsOwned.Set(float64(om.count()))
			return
		}
		log.Printf("Ownership: renewal of %s/%d failed (%v); still within the %s window",
			p.topic, p.partition, err, om.cfg.TTL)
		return
	}

	p.epoch.Store(owner.Epoch)
	p.lastRenew.Store(time.Now().UnixNano())
	p.lost.Store(false)
	if !p.held.Swap(true) {
		log.Printf("Ownership: re-acquired %s/%d at epoch %d", p.topic, p.partition, owner.Epoch)
	}
	metrics.PartitionsOwned.Set(float64(om.count()))
}

// release hands every claim over so the next owner of those partitions does not
// have to wait out the TTL.
//
// Like the writer lease it writes a released tombstone rather than deleting
// the object, and for the same reason: the epoch has to keep moving forward
// across a clean handover, because it is stamped into segment names and
// manifests. Deleting the claim would let the next agent start at epoch 1 while
// durable records still say epoch 7, and it would conclude it had been
// superseded by a writer that does not exist.
func (om *ownershipManager) release(ctx context.Context, store ObjectStore) {
	if om == nil {
		return
	}
	om.wg.Wait() // stop renewing before deciding whether we still hold anything

	cond, ok := store.(ConditionalObjectStore)
	if !ok {
		return
	}

	for _, p := range om.snapshot() {
		key := partitionOwnerKey(p.topic, p.partition)
		raw, version, found, err := cond.GetVersion(ctx, key)
		if err != nil || !found {
			continue
		}
		var current PartitionOwner
		if json.Unmarshal(raw, &current) != nil || current.Agent != om.agent {
			continue // not ours any more; leave the new holder's claim alone
		}

		released := PartitionOwner{Agent: "", Epoch: current.Epoch, Expires: 0}
		body, err := json.Marshal(released)
		if err != nil {
			continue
		}
		if _, err := om.store.PutVersion(ctx, key, body, version); err != nil {
			// A failed release is not a problem: the claim expires on schedule,
			// so this costs the next owner at most the TTL.
			log.Printf("Ownership: could not release %s (%v); it will expire in up to %s", key, err, om.cfg.TTL)
			continue
		}
		p.held.Store(false)
		log.Printf("Ownership: released %s at epoch %d", key, current.Epoch)
	}
	metrics.PartitionsOwned.Set(0)
}

// verifyClaim confirms against object storage that this agent still holds the
// partition, rather than trusting the local view.
//
// The local view goes stale: a claim can be taken the instant it ages out, and
// this agent only learns about it at its next renewal, up to a third of a TTL
// later. Anything that writes durable state on behalf of a partition has to ask
// the object store, because "I still think I own this" is exactly the belief that
// produces two writers.
func (om *ownershipManager) verifyClaim(ctx context.Context, topic string, partition int32) error {
	p := om.partition(topic, partition)
	if p == nil || !p.held.Load() {
		return fmt.Errorf("%w: %s/%d", ErrPartitionNotOwned, topic, partition)
	}
	raw, _, found, err := om.store.GetVersion(ctx, partitionOwnerKey(topic, partition))
	if err != nil {
		return fmt.Errorf("ownership: could not read the claim for %s/%d: %w", topic, partition, err)
	}
	if !found {
		return fmt.Errorf("%w: %s/%d has no claim", ErrPartitionNotOwned, topic, partition)
	}
	var current PartitionOwner
	if json.Unmarshal(raw, &current) != nil || current.Agent != om.agent {
		holder := "another agent"
		if json.Unmarshal(raw, &current) == nil && current.Agent != "" {
			holder = current.Agent
		}
		return fmt.Errorf("%w: %s/%d is held by %s", ErrPartitionNotOwned, topic, partition, holder)
	}
	if current.Expires > 0 && time.Now().Unix() > current.Expires {
		return fmt.Errorf("%w: the claim for %s/%d expired %ds ago",
			ErrPartitionNotOwned, topic, partition, time.Now().Unix()-current.Expires)
	}
	// A live claim above our epoch means a peer using the same agent id has
	// already taken the partition. The agent-name check above cannot see that,
	// because the record names us.
	if held := p.epoch.Load(); current.Epoch > held {
		return fmt.Errorf("%w: %s/%d is held at epoch %d by an agent using our id %q (we hold epoch %d); "+
			"every agent must have a distinct KIMISTORE_AGENT_ID",
			ErrPartitionLost, topic, partition, current.Epoch, om.agent, held)
	}
	return nil
}

// releasePartition hands one partition over while the agent keeps running.
//
// It is the same tombstone the full release writes, and for the same reason: the
// epoch has to keep climbing so the next owner's segments sort above this one's
// and a durable record written at this epoch is never mistaken for current.
//
// The claim is marked lost locally as well, so the write path refuses the
// partition immediately rather than at the next renewal. That matters: the claim
// is gone the moment this returns, so another agent may take the partition at
// once, and an owner that kept serving until its next renewal would be writing
// into a partition it no longer holds.
func (om *ownershipManager) releasePartition(ctx context.Context, topic string, partition int32) error {
	p := om.partition(topic, partition)
	if p == nil || !p.held.Load() {
		return fmt.Errorf("%w: %s/%d", ErrPartitionNotOwned, topic, partition)
	}

	p.claimMu.Lock()
	defer p.claimMu.Unlock()

	key := partitionOwnerKey(topic, partition)
	raw, version, found, err := om.store.GetVersion(ctx, key)
	if err != nil || !found {
		if err == nil {
			err = fmt.Errorf("no claim at %s", key)
		}
		return fmt.Errorf("ownership: could not read %s: %w", key, err)
	}
	var current PartitionOwner
	if json.Unmarshal(raw, &current) != nil || current.Agent != om.agent {
		return fmt.Errorf("%w: %s is no longer held by %s", ErrPartitionNotOwned, key, om.agent)
	}

	released := PartitionOwner{Agent: "", Epoch: current.Epoch, Expires: 0}
	body, err := json.Marshal(released)
	if err != nil {
		return err
	}
	if _, err := om.store.PutVersion(ctx, key, body, version); err != nil {
		return fmt.Errorf("ownership: could not release %s: %w", key, err)
	}

	p.held.Store(false)
	p.lost.Store(true)
	p.lastRenew.Store(0)
	metrics.PartitionsOwned.Set(float64(om.count()))
	log.Printf("Ownership: released %s/%d at epoch %d", topic, partition, current.Epoch)
	return nil
}

// close stops the renewal loop without touching object storage, for tests and
// for paths that release explicitly.
func (om *ownershipManager) close() {
	if om == nil {
		return
	}
	select {
	case <-om.quit:
	default:
		close(om.quit)
	}
}

// snapshot lists the claimed partitions.
func (om *ownershipManager) snapshot() []*partitionOwnership {
	om.mu.RLock()
	defer om.mu.RUnlock()
	out := make([]*partitionOwnership, 0, len(om.parts))
	for _, p := range om.parts {
		out = append(out, p)
	}
	return out
}

// count is how many partitions this agent currently believes it owns.
func (om *ownershipManager) count() int {
	om.mu.RLock()
	defer om.mu.RUnlock()
	n := 0
	for _, p := range om.parts {
		if p.held.Load() {
			n++
		}
	}
	return n
}

// Epoch reports the ownership epoch in force for a partition, or 0 when this
// agent does not hold it. It is stamped into every segment that partition
// writes.
func (om *ownershipManager) Epoch(topic string, partition int32) int64 {
	p := om.partition(topic, partition)
	if p == nil || !p.held.Load() {
		return 0
	}
	return p.epoch.Load()
}

// Owns reports whether this agent may currently write a partition.
func (om *ownershipManager) Owns(topic string, partition int32) bool {
	p := om.partition(topic, partition)
	if p == nil {
		// Never claimed. With no ownership manager there is nothing to check;
		// with one, an unclaimed partition is somebody else's to write.
		return om == nil
	}
	return p.held.Load() && !p.lost.Load()
}

// checkWrite refuses a write to a partition this agent does not own.
//
// An unclaimed partition is not an error condition on the hot path: a lazily
// created topic or partition is claimed by the caller first. Reaching here
// means the claim was lost, so continuing risks assigning offsets that collide
// with the new owner's and overwriting the segments behind them.
func (om *ownershipManager) checkWrite(topic string, partition int32) error {
	if om == nil || !om.fenced {
		return nil
	}
	p := om.partition(topic, partition)
	if p == nil {
		return fmt.Errorf("%w: %s/%d has no claim held by this agent", ErrPartitionNotOwned, topic, partition)
	}
	if p.lost.Load() {
		return fmt.Errorf("%w: %s/%d lost its claim at epoch %d", ErrPartitionLost, topic, partition, p.epoch.Load())
	}
	if !p.held.Load() {
		return fmt.Errorf("%w: %s/%d is not held by this agent", ErrPartitionNotOwned, topic, partition)
	}
	return nil
}

// OwnedPartitions reports the partitions this agent holds a claim on, which is
// what the routing table of phase 3 publishes so clients can be sent to the
// agent that owns what they want to read and write.
func (om *ownershipManager) OwnedPartitions() []string {
	if om == nil {
		return nil
	}
	var out []string
	for _, p := range om.snapshot() {
		if p.held.Load() && !p.lost.Load() {
			out = append(out, partitionDurabilityKey(p.topic, p.partition))
		}
	}
	return out
}

// agentID is the identity claims are recorded under.
func (om *ownershipManager) agentID() string {
	if om == nil {
		return ""
	}
	return om.agent
}
