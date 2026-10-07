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
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// clientFinalMessage builds a client-final the way a real client does, given the
// challenge the server sent. Keeping this in the test rather than reusing
// server-side helpers is the point: an implementation that shared its own code
// with the client could agree with itself while both were wrong.
func clientFinalMessage(t *testing.T, mechanism, username, password, gs2Header, clientNonce string, challenge []byte) string {
	t.Helper()

	sf, err := ParseServerFirst(string(challenge))
	if err != nil {
		t.Fatalf("parse challenge: %v", err)
	}
	hashLen, err := keyLength(mechanism)
	if err != nil {
		t.Fatal(err)
	}
	salted, err := pbkdf2(mechanism, []byte(password), sf.Salt, sf.Iterations, hashLen)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := hmacBytes(mechanism, salted, []byte("Client Key"))
	if err != nil {
		t.Fatal(err)
	}
	storedKey, err := hashBytes(mechanism, clientKey)
	if err != nil {
		t.Fatal(err)
	}

	gs2 := gs2Header
	if gs2 == "" {
		gs2 = "n,,"
	}
	withoutProof := "c=" + base64.StdEncoding.EncodeToString([]byte(gs2)) + ",r=" + sf.Nonce

	// The bare client-first the client signed is the part after the gs2 header.
	bare := "n=" + strings.ReplaceAll(username, ",", "=2C") + ",r=" + clientNonce
	authMessage := AuthMessage(bare, string(challenge), withoutProof)

	clientSig, err := hmacBytes(mechanism, storedKey, []byte(authMessage))
	if err != nil {
		t.Fatal(err)
	}
	proof := make([]byte, hashLen)
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSig[i]
	}
	return withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
}

func newTestStore(t *testing.T, users map[string]string) *Store {
	t.Helper()
	objects := newFakeObjects()
	store, err := NewStore(objects)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for user, password := range users {
		v, err := NewVerifier(MechanismSCRAMSHA256, user, password, MinIterations)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, &Credential{Username: user, Verifiers: map[string]*Verifier{MechanismSCRAMSHA256: v}}); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

func TestExchange_Authenticates(t *testing.T) {
	for _, mechanism := range []string{MechanismSCRAMSHA256, MechanismSCRAMSHA512} {
		t.Run(mechanism, func(t *testing.T) {
			store := newTestStore(t, map[string]string{"alice": "s3cret"})
			ctx := context.Background()

			// The stored credential has to offer this mechanism for it to be
			// negotiable at all.
			v, _ := NewVerifier(mechanism, "alice", "s3cret", MinIterations)
			if err := store.Put(ctx, &Credential{Username: "alice", Verifiers: map[string]*Verifier{mechanism: v}}); err != nil {
				t.Fatal(err)
			}

			ex, err := StartExchange(mechanism)
			if err != nil {
				t.Fatal(err)
			}
			clientNonce := "client-nonce-abc"
			challenge, err := ex.First(ctx, []byte("n,,n=alice,r="+clientNonce), store)
			if err != nil {
				t.Fatalf("First: %v", err)
			}
			if !strings.HasPrefix(string(challenge), "r="+clientNonce) {
				t.Errorf("challenge does not start with the client nonce: %q", challenge)
			}

			final := clientFinalMessage(t, mechanism, "alice", "s3cret", "", clientNonce, challenge)
			serverFinal, err := ex.Final([]byte(final))
			if err != nil {
				t.Fatalf("Final: %v", err)
			}
			if !strings.HasPrefix(serverFinal, "v=") {
				t.Errorf("server-final = %q, want a v= signature", serverFinal)
			}
			if !ex.Complete() {
				t.Error("exchange reports incomplete after a valid proof")
			}
			if ex.Username() != "alice" {
				t.Errorf("username = %q, want alice", ex.Username())
			}
		})
	}
}

func TestExchange_WrongPasswordFails(t *testing.T) {
	store := newTestStore(t, map[string]string{"alice": "s3cret"})
	ctx := context.Background()

	ex, _ := StartExchange(MechanismSCRAMSHA256)
	clientNonce := "cn"
	challenge, err := ex.First(ctx, []byte("n,,n=alice,r="+clientNonce), store)
	if err != nil {
		t.Fatal(err)
	}
	final := clientFinalMessage(t, MechanismSCRAMSHA256, "alice", "wrong", "", clientNonce, challenge)
	if _, err := ex.Final([]byte(final)); !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("Final error = %v, want ErrInvalidCredentials", err)
	}
	if ex.Complete() {
		t.Error("exchange reports complete after a bad proof")
	}
	if ex.Done() != true {
		t.Error("a failed exchange must be Done so no further messages are honoured")
	}
}

func TestExchange_UnknownUserFails(t *testing.T) {
	// The exchange must still run to completion against the decoy, and fail
	// only at the proof. Failing at First instead would make a missing account
	// distinguishable from a wrong password.
	store := newTestStore(t, map[string]string{"alice": "s3cret"})
	ctx := context.Background()

	ex, _ := StartExchange(MechanismSCRAMSHA256)
	clientNonce := "cn"
	challenge, err := ex.First(ctx, []byte("n,,n=nobody,r="+clientNonce), store)
	if err != nil {
		t.Fatalf("First must not fail for an unknown user: %v", err)
	}
	final := clientFinalMessage(t, MechanismSCRAMSHA256, "nobody", "anything", "", clientNonce, challenge)
	if _, err := ex.Final([]byte(final)); !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("Final error = %v, want ErrInvalidCredentials", err)
	}
}

func TestExchange_MessageOrderEnforced(t *testing.T) {
	store := newTestStore(t, map[string]string{"alice": "pw"})
	ctx := context.Background()

	t.Run("final before first", func(t *testing.T) {
		ex, _ := StartExchange(MechanismSCRAMSHA256)
		if _, err := ex.Final([]byte("c=biws,r=x,p=abc")); !errors.Is(err, ErrExchangeOrder) {
			t.Errorf("error = %v, want ErrExchangeOrder", err)
		}
	})

	t.Run("first twice", func(t *testing.T) {
		ex, _ := StartExchange(MechanismSCRAMSHA256)
		if _, err := ex.First(ctx, []byte("n,,n=alice,r=cn"), store); err != nil {
			t.Fatal(err)
		}
		if _, err := ex.First(ctx, []byte("n,,n=alice,r=cn2"), store); !errors.Is(err, ErrExchangeOrder) {
			t.Errorf("error = %v, want ErrExchangeOrder", err)
		}
	})

	t.Run("after completion", func(t *testing.T) {
		ex, _ := StartExchange(MechanismSCRAMSHA256)
		challenge, _ := ex.First(ctx, []byte("n,,n=alice,r=cn"), store)
		final := clientFinalMessage(t, MechanismSCRAMSHA256, "alice", "pw", "", "cn", challenge)
		if _, err := ex.Final([]byte(final)); err != nil {
			t.Fatal(err)
		}
		if _, err := ex.Final([]byte(final)); !errors.Is(err, ErrExchangeDone) {
			t.Errorf("error = %v, want ErrExchangeDone", err)
		}
	})
}

func TestExchange_NonceIsFreshAndUnpredictable(t *testing.T) {
	// Two challenges for the same client nonce must differ, or a captured
	// client-final could be replayed.
	store := newTestStore(t, map[string]string{"alice": "pw"})
	ctx := context.Background()

	var seen []string
	for i := 0; i < 4; i++ {
		ex, _ := StartExchange(MechanismSCRAMSHA256)
		challenge, err := ex.First(ctx, []byte("n,,n=alice,r=same"), store)
		if err != nil {
			t.Fatal(err)
		}
		for _, prev := range seen {
			if string(challenge) == prev {
				t.Fatalf("challenge repeated across exchanges: %q", challenge)
			}
		}
		seen = append(seen, string(challenge))
	}
}

func TestExchange_VerifierHeldForTheWholeExchange(t *testing.T) {
	// A credential rotated between the challenge and the proof must not be
	// re-read: the proof has to be checked against the key the client actually
	// answered.
	objects := newFakeObjects()
	store, _ := NewStore(objects)
	ctx := context.Background()

	v1, _ := NewVerifier(MechanismSCRAMSHA256, "alice", "original", MinIterations)
	if err := store.Put(ctx, &Credential{Username: "alice", Verifiers: map[string]*Verifier{MechanismSCRAMSHA256: v1}}); err != nil {
		t.Fatal(err)
	}

	ex, _ := StartExchange(MechanismSCRAMSHA256)
	challenge, err := ex.First(ctx, []byte("n,,n=alice,r=cn"), store)
	if err != nil {
		t.Fatal(err)
	}
	final := clientFinalMessage(t, MechanismSCRAMSHA256, "alice", "original", "", "cn", challenge)

	// Rotate the credential mid-exchange, and break the store as well so any
	// re-read would be caught.
	v2, _ := NewVerifier(MechanismSCRAMSHA256, "alice", "rotated", MinIterations)
	if err := store.Put(ctx, &Credential{Username: "alice", Verifiers: map[string]*Verifier{MechanismSCRAMSHA256: v2}}); err != nil {
		t.Fatal(err)
	}
	objects.getErr = fmt.Errorf("store is down")

	if _, err := ex.Final([]byte(final)); err != nil {
		t.Errorf("a valid proof was rejected after the credential was rotated mid-exchange: %v", err)
	}
}

func TestExchange_UnsupportedMechanismRejected(t *testing.T) {
	if _, err := StartExchange(MechanismPlain); !errors.Is(err, ErrUnsupportedMechanism) {
		t.Errorf("StartExchange(PLAIN) error = %v, want ErrUnsupportedMechanism", err)
	}
	if _, err := StartExchange("SCRAM-SHA-1"); !errors.Is(err, ErrUnsupportedMechanism) {
		t.Errorf("StartExchange(SCRAM-SHA-1) error = %v, want ErrUnsupportedMechanism", err)
	}
}

func TestExchange_ChannelBindingHeaderIsEchoed(t *testing.T) {
	// A client that binds must see its own gs2 header echoed back in c=, or it
	// will refuse the exchange as having lost the binding.
	store := newTestStore(t, map[string]string{"alice": "pw"})
	ctx := context.Background()

	ex, _ := StartExchange(MechanismSCRAMSHA256)
	challenge, err := ex.First(ctx, []byte("p=tls-server-end-point,,n=alice,r=cn"), store)
	if err != nil {
		t.Fatalf("First with a channel-binding flag: %v", err)
	}
	final := clientFinalMessage(t, MechanismSCRAMSHA256, "alice", "pw", "p=tls-server-end-point,,", "cn", challenge)
	if _, err := ex.Final([]byte(final)); err != nil {
		t.Errorf("Final with channel binding: %v", err)
	}
}
