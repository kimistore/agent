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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// ACLPrefix is where authorization rules live in the bucket, beside the
// credentials they apply to.
//
// Sharing the bucket is what makes this work across agents: an operator writes
// a rule once and every agent on that bucket enforces it, with no restart and no
// per-host edit that can drift.
const ACLPrefix = "_acl/"

// aclObject is one rule as stored. The set is split into shards rather than kept
// as a single object so that adding a rule does not rewrite every other rule,
// and so a concurrent reload cannot lose a write it did not observe.
const aclShardCount = 4

// ACLStore holds the authorization policy and keeps a cached snapshot for the
// request path.
//
// This is the reason enforcement can afford to run on every Produce and Fetch.
// Object storage is authoritative and is read on every reload, but a request is
// answered from memory: a broker that consulted the object store per produce
// would be slower than one without ACLs at all.
type ACLStore struct {
	objects ObjectStore

	mu       sync.RWMutex
	policy   *Policy
	revision string

	// interval is how often the snapshot is refreshed. It bounds how long an
	// agent can enforce a stale rule after a change elsewhere.
	interval time.Duration
	now      func() time.Time

	stopOnce sync.Once
	stop     chan struct{}
}

// ErrACLUnavailable is returned when the rules cannot be read. It is distinct
// from "no rules", because one means allow everything and the other must not.
var ErrACLUnavailable = errors.New("authorization rules unavailable")

// NewACLStore opens a store. It does not read anything yet; call Reload to
// establish the initial policy.
func NewACLStore(objects ObjectStore) (*ACLStore, error) {
	if objects == nil {
		return nil, errors.New("auth: nil object store")
	}
	s := &ACLStore{
		objects:  objects,
		interval: 30 * time.Second,
		now:      time.Now,
		stop:     make(chan struct{}),
	}
	// An empty policy until the first reload succeeds, which allows
	// everything: a broker that has just started must not refuse traffic
	// because it has not finished reading its configuration.
	s.policy, _ = NewPolicy(nil)
	return s, nil
}

// Reload re-reads every shard and swaps in the new policy.
//
// It is all-or-nothing: a shard that cannot be read leaves the previous policy
// in place rather than dropping the rules that shard held. A partially applied
// policy is worse than a stale one, because it is not just out of date, it
// disagrees with itself.
func (s *ACLStore) Reload(ctx context.Context) error {
	var acls []ACL
	var revision strings.Builder
	for i := 0; i < aclShardCount; i++ {
		key := aclShardKey(i)
		revision.WriteString(key)
		data, err := s.objects.GetObject(ctx, key)
		if err != nil {
			if isNotFound(err) {
				continue // a shard with no rules yet is not an error
			}
			return fmt.Errorf("%w: read %s: %v", ErrACLUnavailable, key, err)
		}
		var shard aclShard
		if err := json.Unmarshal(data, &shard); err != nil {
			return fmt.Errorf("%w: %s is corrupt: %v", ErrACLUnavailable, key, err)
		}
		acls = append(acls, shard.ACLs...)
	}

	policy, err := NewPolicy(acls)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrACLUnavailable, err)
	}

	s.mu.Lock()
	s.policy = policy
	s.revision = revision.String()
	s.mu.Unlock()
	return nil
}

// aclShard is the stored form of one shard.
type aclShard struct {
	ACLs []ACL `json:"acls"`
}

func aclShardKey(i int) string {
	return fmt.Sprintf("%s%d.json", ACLPrefix, i)
}

// shardFor picks a shard by hashing the rule, so a rule always lands in the
// same place and a rewrite touches only one file.
func shardFor(a ACL) int {
	h := 0
	for _, c := range a.Principal + "|" + string(a.Operation) + "|" + string(a.Resource.Kind) + "|" + a.Resource.Name {
		h = h*31 + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h % aclShardCount
}

// Add writes a rule.
//
// The write is a read-modify-write of one shard, which two agents editing
// different rules in the same shard at the same moment could interleave. Rather
// than pretend otherwise, the result is checked and retried: after writing, the
// shard is re-read and if the rule is not there the whole cycle runs again,
// because the loser's write is now on disk and the retry merges into it. The
// object store has conditional writes available, and using one here would be
// cleaner; the plain interface this store holds does not expose them.
func (s *ACLStore) Add(ctx context.Context, a ACL) error {
	if _, err := NewPolicy([]ACL{a}); err != nil {
		return err
	}
	key := aclShardKey(shardFor(a))

	const attempts = 3
	for attempt := 0; attempt < attempts; attempt++ {
		shard, err := s.readShard(ctx, key)
		if err != nil {
			return err
		}

		found := false
		for i := range shard.ACLs {
			if shard.ACLs[i] == a {
				found = true
				break
			}
		}
		if !found {
			shard.ACLs = append(shard.ACLs, a)
		}
		data, err := json.Marshal(shard)
		if err != nil {
			return err
		}
		if err := s.objects.PutObject(ctx, key, data); err != nil {
			return err
		}

		// Confirm the rule survived. If a concurrent writer raced us, our copy
		// may have overwritten theirs or been overwritten by theirs; re-reading
		// and retrying settles both cases, because the retry starts from
		// whatever is actually stored.
		check, err := s.readShard(ctx, key)
		if err != nil {
			return err
		}
		if containsACL(check.ACLs, a) {
			// Refresh from object storage rather than trusting the local view,
			// so this agent ends up enforcing what is actually stored.
			return s.Reload(ctx)
		}
	}
	return fmt.Errorf("adding %s did not stick after %d attempts; another agent may be editing the same shard", describeACL(a), attempts)
}

func containsACL(list []ACL, want ACL) bool {
	for _, a := range list {
		if a == want {
			return true
		}
	}
	return false
}

// readShard reads one shard. A shard with no rules yet is not an error.
func (s *ACLStore) readShard(ctx context.Context, key string) (aclShard, error) {
	data, err := s.objects.GetObject(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return aclShard{}, nil
		}
		return aclShard{}, fmt.Errorf("read %s: %w", key, err)
	}
	var shard aclShard
	if err := json.Unmarshal(data, &shard); err != nil {
		return aclShard{}, fmt.Errorf("%s is corrupt: %w", key, err)
	}
	return shard, nil
}

// Remove deletes a rule that matches exactly.
func (s *ACLStore) Remove(ctx context.Context, a ACL) error {
	key := aclShardKey(shardFor(a))
	shard, err := s.readShard(ctx, key)
	if err != nil {
		return err
	}
	out := shard.ACLs[:0]
	removed := false
	for _, existing := range shard.ACLs {
		if existing == a {
			removed = true
			continue
		}
		out = append(out, existing)
	}
	if !removed {
		return fmt.Errorf("no such rule: %s", describeACL(a))
	}
	shard.ACLs = out
	data, err := json.Marshal(shard)
	if err != nil {
		return err
	}
	if err := s.objects.PutObject(ctx, key, data); err != nil {
		return err
	}
	return s.Reload(ctx)
}

// Policy returns the current cached policy. This is the request path.
func (s *ACLStore) Policy() *Policy {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.policy
}

// Enabled reports whether any rules are configured. A disabled store allows
// everything, which is what keeps this additive for existing deployments.
func (s *ACLStore) Enabled() bool {
	return !s.Policy().Empty()
}

// Start begins refreshing the policy in the background.
func (s *ACLStore) Start(ctx context.Context) {
	go func() {
		t := time.NewTicker(s.interval)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if err := s.Reload(ctx); err != nil {
					// Keep serving the previous policy. A broker that emptied
					// its rules because the bucket was briefly unreachable
					// would allow everything, which is the worst possible
					// failure direction for this feature.
					fmt.Fprintf(os.Stderr,
						"ACL: reload failed, continuing with the previous rules: %v\n", err)
				}
			}
		}
	}()
}

// Close stops the refresh loop.
func (s *ACLStore) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
}

func describeACL(a ACL) string {
	name := string(a.Resource.Kind)
	if a.Resource.Name != "" {
		name += ":" + a.Resource.Name
	}
	return fmt.Sprintf("%s %s %s on %s", a.Permission, a.Principal, a.Operation, name)
}

// DescribeACL renders a rule for operator-facing output.
func DescribeACL(a ACL) string { return describeACL(a) }
