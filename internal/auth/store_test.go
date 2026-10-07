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
	"errors"
	"strings"
	"testing"
)

// fakeObjects is an in-memory ObjectStore. It is defined here rather than
// reusing the storage package's mock because that mock is unexported, and
// because these tests need failure injection the log-oriented mock has no use
// for.
type fakeObjects struct {
	data     map[string][]byte
	getErr   error
	putErr   error
	listErr  error
	gets     int
	putCount int
	delCount int
}

func newFakeObjects() *fakeObjects {
	return &fakeObjects{data: map[string][]byte{}}
}

func (f *fakeObjects) GetObject(_ context.Context, key string) ([]byte, error) {
	f.gets++
	if f.getErr != nil {
		return nil, f.getErr
	}
	b, ok := f.data[key]
	if !ok {
		return nil, errors.New("not found")
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

func (f *fakeObjects) PutObject(_ context.Context, key string, data []byte) error {
	f.putCount++
	if f.putErr != nil {
		return f.putErr
	}
	f.data[key] = data
	return nil
}

func (f *fakeObjects) ListObjectKeys(_ context.Context, prefix string) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []string
	for k := range f.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (f *fakeObjects) DeleteObject(_ context.Context, key string) error {
	f.delCount++
	delete(f.data, key)
	return nil
}

func TestStore_RoundTrip(t *testing.T) {
	s, err := NewStore(newFakeObjects())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	v, err := NewVerifier(MechanismSCRAMSHA256, "alice", "s3cret", MinIterations)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, &Credential{Username: "alice", Verifiers: map[string]*Verifier{MechanismSCRAMSHA256: v}}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	cred, err := s.Get(ctx, "alice")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if cred.Username != "alice" {
		t.Errorf("username = %q, want alice", cred.Username)
	}
	got := cred.Verifiers[MechanismSCRAMSHA256]
	if got == nil {
		t.Fatal("stored credential has no SCRAM-SHA-256 verifier")
	}
	if string(got.StoredKey) != string(v.StoredKey) || string(got.ServerKey) != string(v.ServerKey) {
		t.Error("stored keys did not survive the round trip")
	}
	if string(got.Salt) != string(v.Salt) {
		t.Error("salt did not survive the round trip")
	}
}

func TestStore_UsernameIsNotInTheKey(t *testing.T) {
	// The username is a principal name. It must not leak through object keys,
	// which show up in listings and access logs.
	f := newFakeObjects()
	s, _ := NewStore(f)
	ctx := context.Background()

	v, _ := NewVerifier(MechanismSCRAMSHA256, "alice", "pw", MinIterations)
	if err := s.Put(ctx, &Credential{Username: "alice", Verifiers: map[string]*Verifier{MechanismSCRAMSHA256: v}}); err != nil {
		t.Fatal(err)
	}
	for key := range f.data {
		if strings.Contains(key, "alice") {
			t.Errorf("object key %q contains the username", key)
		}
	}
	// It must still be recoverable, which is the other half of the trade.
	names, err := s.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "alice" {
		t.Errorf("List = %v, want [alice]", names)
	}
}

func TestStore_UsernameWithPathSeparators(t *testing.T) {
	// A username containing "/" would escape its prefix and collide with
	// another prefix's namespace if the key were built by concatenation.
	for _, name := range []string{"a/b", "../../etc/passwd", "team/ops", "alice"} {
		t.Run(name, func(t *testing.T) {
			s, _ := NewStore(newFakeObjects())
			ctx := context.Background()
			v, _ := NewVerifier(MechanismSCRAMSHA256, name, "pw", MinIterations)
			if err := s.Put(ctx, &Credential{Username: name, Verifiers: map[string]*Verifier{MechanismSCRAMSHA256: v}}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Get(ctx, name); err != nil {
				t.Fatalf("credential for %q is not reachable: %v", name, err)
			}
		})
	}
}

func TestStore_MissingUserYieldsDecoy(t *testing.T) {
	// A miss must cost the same as a wrong password, or response time discloses
	// which accounts exist.
	s, _ := NewStore(newFakeObjects())
	ctx := context.Background()

	v, err := s.VerifierFor(ctx, "nobody", MechanismSCRAMSHA256)
	if err != nil {
		t.Fatalf("VerifierFor returned an error for a miss; it must not: %v", err)
	}
	if v == nil {
		t.Fatal("VerifierFor returned nil for a miss")
	}
	// The decoy must be a real, valid credential, otherwise the work is skipped.
	if err := v.validate(); err != nil {
		t.Errorf("decoy is not a valid verifier, so the lookup would be fast: %v", err)
	}
	if string(v.StoredKey) == "" {
		t.Error("decoy has no stored key")
	}
}

func TestStore_UnreadableStoreAlsoYieldsDecoy(t *testing.T) {
	// A broken bucket must not become a distinguishable fast failure.
	f := newFakeObjects()
	f.getErr = errors.New("dial tcp: connection refused")
	s, _ := NewStore(f)
	ctx := context.Background()

	v, err := s.VerifierFor(ctx, "alice", MechanismSCRAMSHA256)
	if err != nil {
		t.Fatalf("VerifierFor errored: %v", err)
	}
	if err := v.validate(); err != nil {
		t.Errorf("store failure did not fall back to a valid decoy: %v", err)
	}

	// The operator-facing call must still report the store failure, because
	// that is the difference between "no such user" and "your bucket is wrong".
	if _, err := s.Get(ctx, "alice"); !errors.Is(err, ErrStoreUnavailable) {
		t.Errorf("Get error = %v, want ErrStoreUnavailable", err)
	}
}

func TestStore_GetDistinguishesMissingFromBroken(t *testing.T) {
	f := newFakeObjects()
	s, _ := NewStore(f)
	ctx := context.Background()

	if _, err := s.Get(ctx, "nobody"); !errors.Is(err, ErrNoSuchUser) {
		t.Errorf("Get error = %v, want ErrNoSuchUser", err)
	}

	f.getErr = errors.New("connection reset")
	if _, err := s.Get(ctx, "alice"); !errors.Is(err, ErrStoreUnavailable) {
		t.Errorf("Get error = %v, want ErrStoreUnavailable", err)
	}
}

func TestStore_PutRejectsIncompleteCredentials(t *testing.T) {
	s, _ := NewStore(newFakeObjects())
	ctx := context.Background()

	if err := s.Put(ctx, nil); err == nil {
		t.Error("accepted a nil credential")
	}
	if err := s.Put(ctx, &Credential{Verifiers: map[string]*Verifier{}}); err == nil {
		t.Error("accepted a credential with no username")
	}
	v, _ := NewVerifier(MechanismSCRAMSHA256, "alice", "pw", MinIterations)
	if err := s.Put(ctx, &Credential{Username: "alice"}); err == nil {
		t.Error("accepted a credential with no verifiers")
	}
	// A verifier with a mangled iteration count must not be stored: it would
	// load as a credential that either cannot verify or verifies too cheaply.
	weak := &Verifier{Mechanism: MechanismSCRAMSHA256, Salt: v.Salt, Iterations: 1, StoredKey: v.StoredKey, ServerKey: v.ServerKey}
	if err := s.Put(ctx, &Credential{Username: "alice", Verifiers: map[string]*Verifier{MechanismSCRAMSHA256: weak}}); err == nil {
		t.Error("accepted a verifier below the iteration floor")
	}
}

func TestStore_MultipleMechanismsInOneCredential(t *testing.T) {
	// One object per user, so adding a mechanism must not need a
	// read-modify-write the caller could get wrong.
	s, _ := NewStore(newFakeObjects())
	ctx := context.Background()

	v256, _ := NewVerifier(MechanismSCRAMSHA256, "alice", "pw", MinIterations)
	if err := s.Put(ctx, &Credential{Username: "alice", Verifiers: map[string]*Verifier{MechanismSCRAMSHA256: v256}}); err != nil {
		t.Fatal(err)
	}
	cred, err := s.Get(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	v512, _ := NewVerifier(MechanismSCRAMSHA512, "alice", "pw", MinIterations)
	cred.Verifiers[MechanismSCRAMSHA512] = v512
	if err := s.Put(ctx, cred); err != nil {
		t.Fatal(err)
	}

	cred, err = s.Get(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if len(cred.Verifiers) != 2 {
		t.Errorf("stored %d verifiers, want 2", len(cred.Verifiers))
	}
	// Each mechanism must be offered under its own key.
	for _, m := range []string{MechanismSCRAMSHA256, MechanismSCRAMSHA512} {
		got, err := s.VerifierFor(ctx, "alice", m)
		if err != nil {
			t.Fatal(err)
		}
		if got.Mechanism != m {
			t.Errorf("VerifierFor(%s) returned a %s verifier", m, got.Mechanism)
		}
	}
	// And a mechanism the user has no credential for still yields the decoy.
	if got, _ := s.VerifierFor(ctx, "alice", MechanismPlain); got.Mechanism == MechanismPlain {
		t.Error("PLAIN was answered from the SCRAM store")
	}
}

func TestStore_Delete(t *testing.T) {
	f := newFakeObjects()
	s, _ := NewStore(f)
	ctx := context.Background()
	v, _ := NewVerifier(MechanismSCRAMSHA256, "alice", "pw", MinIterations)
	if err := s.Put(ctx, &Credential{Username: "alice", Verifiers: map[string]*Verifier{MechanismSCRAMSHA256: v}}); err != nil {
		t.Fatal(err)
	}
	if !s.Exists(ctx, "alice") {
		t.Fatal("credential does not exist after Put")
	}
	if err := s.Delete(ctx, "alice"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if s.Exists(ctx, "alice") {
		t.Error("credential still exists after Delete")
	}
}

func TestStore_CorruptObjectIsReportedAsStoreProblem(t *testing.T) {
	// A truncated object must not be read as "no verifiers" and quietly
	// authenticate nobody; it must look like the store is broken.
	f := newFakeObjects()
	s, _ := NewStore(f)
	ctx := context.Background()
	f.data[keyFor("alice")] = []byte("{not json")

	if _, err := s.Get(ctx, "alice"); !errors.Is(err, ErrStoreUnavailable) {
		t.Errorf("Get error = %v, want ErrStoreUnavailable", err)
	}
	// And a corrupt object must not become the decoy path silently: the
	// exchange still fails, which is the safe outcome.
	v, err := s.VerifierFor(ctx, "alice", MechanismSCRAMSHA256)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.validate(); err != nil {
		t.Errorf("corrupt object did not fall back to the decoy: %v", err)
	}
}
