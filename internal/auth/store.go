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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Prefix is where SCRAM credentials live in the bucket.
//
// It is a control-plane prefix like _offsets/ and _owners/, so credentials sit
// beside the log's own bookkeeping and are shared by every agent pointed at the
// bucket. That sharing is the reason to keep them here rather than in a local
// file: rotating a credential is one object write, and every agent picks it up,
// with no restart and no per-host edit that can drift.
const Prefix = "_scram/"

// ErrNoSuchUser is returned when a username has no stored credential. It is
// deliberately not surfaced to the client: the wire response is the same as for
// a wrong password, so an unauthenticated peer cannot enumerate accounts.
var ErrNoSuchUser = errors.New("no such user")

// ErrStoreUnavailable is returned when the credential store cannot be read. It
// is also not surfaced differently to the client. The distinction matters only
// to the server's own logs, because the two failures have different causes and
// an operator needs to tell a misconfigured bucket from a typo'd username.
var ErrStoreUnavailable = errors.New("credential store unavailable")

// ObjectStore is the narrow object-storage surface the credential store needs.
// It is satisfied by *storage.StorageEngine.
type ObjectStore interface {
	GetObject(ctx context.Context, key string) ([]byte, error)
	PutObject(ctx context.Context, key string, data []byte) error
	ListObjectKeys(ctx context.Context, prefix string) ([]string, error)
	DeleteObject(ctx context.Context, key string) error
}

// Credential is one user's stored credentials: the username in the clear, so
// operators can list them, and a verifier per mechanism so the same account can
// offer whichever the client negotiates.
//
// The username is not a secret, but it is also not something to leak through an
// object key: keys show up in listings, access logs and audit trails, and a
// principal name is exactly the sort of thing that should not be scattered. The
// key is therefore a hash of it and the name lives inside the object.
type Credential struct {
	Username  string               `json:"username"`
	Verifiers map[string]*Verifier `json:"verifiers"`
	UpdatedAt string               `json:"updated_at,omitempty"`
	Note      string               `json:"note,omitempty"`
}

// Store reads and writes credentials in object storage.
type Store struct {
	objects ObjectStore
	// decoy is verified against when the requested user does not exist, so a
	// missing account costs the same as a wrong password. See VerifierFor.
	decoy *Verifier
}

// NewStore wraps an object store. It derives one throwaway credential used only
// to equalise the cost of a lookup miss.
func NewStore(objects ObjectStore) (*Store, error) {
	if objects == nil {
		return nil, errors.New("auth: nil object store")
	}
	decoy, err := NewVerifier(MechanismSCRAMSHA256, "", decoyPassword, MinIterations)
	if err != nil {
		return nil, fmt.Errorf("auth: build decoy credential: %w", err)
	}
	return &Store{objects: objects, decoy: decoy}, nil
}

// decoyPassword is arbitrary and never leaves the process. It only has to be a
// plausible password so the derivation runs the same number of times.
const decoyPassword = "decoy-credential-for-constant-cost-lookups"

// keyFor is the object key for a username.
//
// SHA-256 of the username, hex encoded. Hashing rather than escaping avoids a
// whole class of bug: SCRAM usernames may contain "/", and any escaping scheme
// has to agree with itself on write, on read and on list, or a credential
// becomes unreachable. A hash cannot drift, and collisions are not a practical
// concern for a credential set.
func keyFor(username string) string {
	sum := sha256.Sum256([]byte(username))
	return Prefix + hex.EncodeToString(sum[:]) + ".json"
}

// Put stores a credential, replacing any existing one for the same username.
//
// The whole object is rewritten, so adding a second mechanism to an existing
// user does not require a read-modify-write the caller could get wrong.
func (s *Store) Put(ctx context.Context, cred *Credential) error {
	if cred == nil || cred.Username == "" {
		return errors.New("auth: credential needs a username")
	}
	if len(cred.Verifiers) == 0 {
		return errors.New("auth: credential needs at least one verifier")
	}
	for mechanism, v := range cred.Verifiers {
		if err := v.validate(); err != nil {
			return fmt.Errorf("auth: %s verifier for %q: %w", mechanism, cred.Username, err)
		}
	}
	data, err := json.Marshal(cred)
	if err != nil {
		return fmt.Errorf("auth: encode credential: %w", err)
	}
	return s.objects.PutObject(ctx, keyFor(cred.Username), data)
}

// Get returns the stored credential for a username.
func (s *Store) Get(ctx context.Context, username string) (*Credential, error) {
	data, err := s.objects.GetObject(ctx, keyFor(username))
	if err != nil {
		// A missing object is a missing user; anything else is the store
		// failing. Both fail the authentication, but only one is the
		// operator's bucket being wrong.
		if isNotFound(err) {
			return nil, ErrNoSuchUser
		}
		return nil, fmt.Errorf("%w: read credential for %q: %v", ErrStoreUnavailable, username, err)
	}
	var cred Credential
	if err := json.Unmarshal(data, &cred); err != nil {
		return nil, fmt.Errorf("%w: credential for %q is corrupt: %v", ErrStoreUnavailable, username, err)
	}
	if cred.Username == "" {
		// Trust the object key, not the body, when they disagree: the key is
		// what the lookup was derived from.
		cred.Username = username
	}
	return &cred, nil
}

// Delete removes a user's credential.
func (s *Store) Delete(ctx context.Context, username string) error {
	return s.objects.DeleteObject(ctx, keyFor(username))
}

// List returns every stored username, sorted.
//
// Every credential object is read to recover the username, because the key is a
// hash. That is fine at the scale this is for -- operator tooling and an audit,
// not a request path.
func (s *Store) List(ctx context.Context) ([]string, error) {
	keys, err := s.objects.ListObjectKeys(ctx, Prefix)
	if err != nil {
		return nil, fmt.Errorf("%w: list credentials: %v", ErrStoreUnavailable, err)
	}
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		if !strings.HasSuffix(key, ".json") {
			continue
		}
		data, err := s.objects.GetObject(ctx, key)
		if err != nil {
			// One unreadable object should not hide the rest of the set from
			// the operator who has to repair it.
			continue
		}
		var cred Credential
		if err := json.Unmarshal(data, &cred); err != nil || cred.Username == "" {
			continue
		}
		names = append(names, cred.Username)
	}
	sort.Strings(names)
	return names, nil
}

// VerifierFor returns the verifier for a username and mechanism.
//
// A miss returns the decoy credential rather than an error, and the caller
// carries on with it. That is the point: with the decoy, a request for a
// username that does not exist performs the same PBKDF2 work as one for an
// existing username with a wrong password, so response time does not disclose
// which accounts are real. The decoy cannot be satisfied, because no proof can
// match a key derived from a password nobody holds.
//
// An unreadable store also yields the decoy, so that a bucket problem does not
// turn into a distinguishable fast failure.
func (s *Store) VerifierFor(ctx context.Context, username, mechanism string) (*Verifier, error) {
	cred, err := s.Get(ctx, username)
	if err != nil {
		return s.decoy, nil
	}
	v, ok := cred.Verifiers[mechanism]
	if !ok || v == nil {
		return s.decoy, nil
	}
	return v, nil
}

// Exists reports whether a username has any stored credential, without the
// constant-cost behaviour. Operator tooling wants the truth, not a decoy.
func (s *Store) Exists(ctx context.Context, username string) bool {
	_, err := s.Get(ctx, username)
	return err == nil
}

// isNotFound recognises a missing object across the error shapes the storage
// layer can produce.
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") ||
		strings.Contains(msg, "nosuchkey") ||
		strings.Contains(msg, "no such") ||
		strings.Contains(msg, "404")
}
