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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"kimistore/internal/metrics"
)

// LeaseConfig configures the single-writer fence.
//
// The agent's durability model depends on being the only thing appending to a
// given log: offsets are handed out from local state and segments are written
// to fixed keys derived from those offsets, so two writers on one bucket do
// not queue behind each other, they collide and overwrite each other. Nothing
// in the protocol layer detects that. The lease is what detects it.
type LeaseConfig struct {
	// Enabled turns the fence on. An engine constructed without it runs
	// unfenced, which is what unit tests and single-shot tools want.
	Enabled bool
	// Key is the object the claim lives at. It must be inside the same bucket
	// as the log, since that is what makes it mutually exclusive.
	Key string
	// Holder identifies this writer in the lease record and in the log. It
	// only has to be unique per instance.
	Holder string
	// TTL is how long an acquisition is valid without renewal. A crashed
	// writer therefore blocks the next one for at most TTL.
	TTL time.Duration
	// Require makes an object store that cannot make writes conditional a
	// startup failure instead of a degraded, unfenced run.
	Require bool
}

// DefaultLeaseKey is where the writer lease lives inside the bucket.
const DefaultLeaseKey = "_meta/lease.json"

// errSuperseded reports that the durable state being read was written by an
// agent that held the log after this one. It is deliberately not a fallback
// case: recovery has to stop rather than carry on from someone else's log
// position.
var errSuperseded = errors.New("durable state belongs to a newer writer")

// DefaultLeaseTTL is the default claim lifetime.
const DefaultLeaseTTL = 30 * time.Second

// leaseAcquireAttempts is how many times a compare-and-swap is retried before
// the contention is reported as another holder.
const leaseAcquireAttempts = 5

// DefaultWriterID names this process in the lease: hostname and pid, which is
// unique enough to tell two agents sharing a bucket apart.
func DefaultWriterID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s/%d", host, os.Getpid())
}

// leaseState is the live view of this agent's claim.
type leaseState struct {
	epoch  atomic.Int64
	held   atomic.Bool
	lost   atomic.Bool
	fenced atomic.Bool // the store supports compare-and-swap

	cfg    LeaseConfig
	key    string
	holder string
	quit   chan struct{}
	wg     sync.WaitGroup

	// lastRenew is when the lease was last confirmed, used to decide when a
	// failed renewal has aged past the TTL and the claim is really gone.
	lastRenew atomic.Int64
}

// acquireLease claims exclusive write access to the log in object storage.
//
// It returns an error when another live writer holds the claim, which the
// caller must treat as fatal: an agent that cannot fence itself must not serve
// produce requests.
func acquireLease(ctx context.Context, store ObjectStore, cfg LeaseConfig) (*leaseState, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if cfg.Key == "" {
		cfg.Key = DefaultLeaseKey
	}
	if cfg.Holder == "" {
		cfg.Holder = DefaultWriterID()
	}
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultLeaseTTL
	}

	ls := &leaseState{
		cfg:    cfg,
		key:    cfg.Key,
		holder: cfg.Holder,
		quit:   make(chan struct{}),
	}

	cond, ok := store.(ConditionalObjectStore)
	if !ok {
		if cfg.Require {
			return nil, fmt.Errorf("%w: %T cannot fence writers, and KIMISTORE_REQUIRE_LEASE is set", ErrUnsupported, store)
		}
		ls.lost.Store(true) // unfenced: the write path must not claim otherwise
		log.Printf("Lease: %T cannot make writes conditional, so %s is advisory only; "+
			"running a second agent against this bucket will corrupt the log", store, cfg.Key)
		return ls, nil
	}
	ls.fenced.Store(true)

	// Bound acquisition so a wedged object store cannot hang startup.
	acquireCtx, cancel := context.WithTimeout(ctx, cfg.TTL)
	defer cancel()

	lease, err := ls.claim(acquireCtx, cond)
	if err != nil {
		return nil, err
	}

	ls.epoch.Store(lease.Epoch)
	ls.held.Store(true)
	ls.lastRenew.Store(time.Now().UnixNano())
	metrics.LeaseOwned.Set(1)
	metrics.LeaseEpoch.Set(float64(lease.Epoch))

	log.Printf("Lease: acquired %s as %q at epoch %d (ttl %s)", cfg.Key, cfg.Holder, lease.Epoch, cfg.TTL)

	ls.wg.Add(1)
	go ls.renewLoop(ctx, cond)

	return ls, nil
}

// claim performs one compare-and-swap against the lease object, retrying while
// the object is being written by someone else.
func (ls *leaseState) claim(ctx context.Context, store ConditionalObjectStore) (Lease, error) {
	var lastErr error

	for attempt := 0; attempt < leaseAcquireAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return Lease{}, ctx.Err()
			case <-time.After(time.Duration(attempt) * 50 * time.Millisecond):
			}
		}

		raw, version, found, err := store.GetVersion(ctx, ls.key)
		if err != nil {
			lastErr = err
			continue
		}

		var current Lease
		if found {
			// A lease record that cannot be parsed is treated as expired
			// rather than as a permanent block: the worst case is one extra
			// renewal cycle of contention, and the alternative is a corrupt
			// record wedging the log forever.
			if jsonErr := json.Unmarshal(raw, &current); jsonErr != nil {
				log.Printf("Lease: %s is unreadable (%v); treating it as expired", ls.key, jsonErr)
				current = Lease{}
			}
		}

		now := time.Now()
		next := Lease{Holder: ls.holder, Epoch: 1, Expires: now.Add(ls.cfg.TTL).Unix()}

		switch {
		case !found:
			// Create it. The empty version means "only if absent", which is
			// what makes two agents racing to start resolve to one winner.
		case !current.Expired(now) && current.Holder != ls.holder:
			holder, expires := current.Holder, time.Unix(current.Expires, 0).UTC().Format(time.RFC3339)
			return Lease{}, fmt.Errorf("%w: %s is held by %q until %s (epoch %d)",
				ErrLeaseHeld, ls.key, holder, expires, current.Epoch)
		default:
			// Renew our own lease, or take over one that has expired.
			next.Epoch = current.Epoch + 1
			if !found || current.Epoch < 1 {
				next.Epoch = 1
			}
		}

		body, err := json.Marshal(next)
		if err != nil {
			return Lease{}, err
		}

		if _, err := store.PutVersion(ctx, ls.key, body, version); err != nil {
			if err == ErrVersionMismatch {
				lastErr = ErrVersionMismatch
				continue // someone else wrote first; read again and decide
			}
			return Lease{}, fmt.Errorf("lease: could not write %s: %w", ls.key, err)
		}
		return next, nil
	}

	if lastErr == nil {
		lastErr = ErrLeaseHeld
	}
	return Lease{}, fmt.Errorf("could not acquire %s after %d attempts: %w", ls.key, leaseAcquireAttempts, lastErr)
}

// renewLoop keeps the claim alive until the engine shuts down.
func (ls *leaseState) renewLoop(ctx context.Context, store ConditionalObjectStore) {
	defer ls.wg.Done()

	interval := ls.cfg.TTL / 3
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ls.quit:
			return
		case <-ticker.C:
			ls.renew(ctx, store)
		}
	}
}

// renew makes one attempt to extend the claim and decides whether the claim is
// still ours. A renewal that fails is not immediately fatal -- object stores
// have bad minutes -- but once failures have aged past the TTL the writer
// stops trusting it, because at that point another agent may legitimately have
// taken over.
func (ls *leaseState) renew(parent context.Context, store ConditionalObjectStore) {
	ctx, cancel := context.WithTimeout(parent, ls.cfg.TTL)
	defer cancel()

	lease, err := ls.claim(ctx, store)
	if err != nil {
		metrics.LeaseRenewalFailures.Inc()

		age := time.Since(time.Unix(0, ls.lastRenew.Load()))
		if age > ls.cfg.TTL {
			if ls.held.CompareAndSwap(true, false) {
				log.Printf("Lease: LOST %s after failing to renew for %s; %v. Writes are being refused "+
					"because another writer may have taken the log.", ls.key, age.Truncate(time.Second), err)
			}
			return
		}
		log.Printf("Lease: renewal of %s failed (%v); still within the %s window", ls.key, err, ls.cfg.TTL)
		return
	}

	ls.epoch.Store(lease.Epoch)
	ls.lastRenew.Store(time.Now().UnixNano())
	if !ls.held.Swap(true) {
		log.Printf("Lease: re-acquired %s at epoch %d", ls.key, lease.Epoch)
	}
	metrics.LeaseEpoch.Set(float64(lease.Epoch))
	metrics.LeaseOwned.Set(1)
}

// release hands the log over so the next writer does not have to wait out the
// TTL.
//
// It writes a released tombstone rather than deleting the object, and that
// detail is load-bearing. The epoch has to keep moving forward across a clean
// restart, because it is stamped into the checkpoint: if the claim were
// deleted, the next agent would start again at epoch 1 while the checkpoint it
// is about to read says epoch 7, and it would refuse to start. Keeping the
// record keeps the fencing token monotonic.
func (ls *leaseState) release(ctx context.Context, store ObjectStore) {
	if ls == nil {
		return
	}
	ls.wg.Wait() // stop renewing before deciding whether we still own it

	cond, ok := store.(ConditionalObjectStore)
	if !ok {
		return
	}
	raw, version, found, err := cond.GetVersion(ctx, ls.key)
	if err != nil || !found {
		return
	}
	var current Lease
	if json.Unmarshal(raw, &current) != nil || current.Holder != ls.holder {
		return // not ours any more; leave the new holder's claim alone
	}

	released := Lease{Holder: "", Epoch: current.Epoch, Expires: 0}
	body, err := json.Marshal(released)
	if err != nil {
		return
	}
	if _, err := cond.PutVersion(ctx, ls.key, body, version); err != nil {
		// A failed release is not a problem: the claim expires on schedule,
		// so this costs the next writer at most the TTL.
		log.Printf("Lease: could not release %s (%v); it will expire in up to %s", ls.key, err, ls.cfg.TTL)
		return
	}
	metrics.LeaseOwned.Set(0)
	log.Printf("Lease: released %s at epoch %d", ls.key, current.Epoch)
}

// close stops the renewal loop without touching object storage, for tests and
// for paths that release explicitly.
func (ls *leaseState) close() {
	if ls == nil {
		return
	}
	select {
	case <-ls.quit:
	default:
		close(ls.quit)
	}
}

// Epoch is the fencing token of the current acquisition, or 0 when the agent
// runs unfenced.
func (ls *leaseState) Epoch() int64 {
	if ls == nil {
		return 0
	}
	return ls.epoch.Load()
}

// Owned reports whether this agent currently believes it holds the lease.
func (ls *leaseState) Owned() bool {
	if ls == nil {
		return false
	}
	return ls.held.Load()
}

// WriterAllowed reports whether the write path may proceed. It is false only
// when a fenced agent has lost a claim it previously held, which is the one
// case where continuing risks overwriting another writer's log.
func (ls *leaseState) WriterAllowed() bool {
	if ls == nil {
		return true // no lease configured: nothing to fence
	}
	if ls.lost.Load() {
		return false // the store cannot fence at all
	}
	return ls.held.Load()
}

// checkWrite refuses a write when the writer lease is gone.
func (ls *leaseState) checkWrite() error {
	if ls == nil {
		return nil
	}
	if !ls.fenced.Load() {
		return nil
	}
	if ls.held.Load() {
		return nil
	}
	return ErrLeaseLost
}

// fencedWriter returns the writer identifier stamped into durable records, for
// operators reading a checkpoint or manifest by hand.
func (ls *leaseState) fencedWriter() string {
	if ls == nil {
		return ""
	}
	if !ls.fenced.Load() {
		return ""
	}
	return ls.holder
}
