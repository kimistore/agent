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
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

func TestSCRAM_RFC7677Vector(t *testing.T) {
	// RFC 7677 section 3. The whole exchange is reproduced from the stored
	// verifier upward, which is the only order the broker ever runs it in: the
	// password is not available at authentication time, so anything that
	// derives the salted password during verification is wrong by
	// construction even if it agrees here.
	const (
		username   = "user"
		password   = "pencil"
		saltB64    = "W22ZaJ0SNY7soEsUEjb6gQ=="
		iterations = 4096

		clientFirst = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
		serverFirst = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
		clientFinal = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
		serverFinal = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
	)

	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		t.Fatalf("decode salt: %v", err)
	}
	storedKey, serverKey := deriveKeys(t, MechanismSCRAMSHA256, password, salt, iterations)
	v := &Verifier{
		Mechanism:  MechanismSCRAMSHA256,
		Salt:       salt,
		Iterations: iterations,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
	}

	cf, err := ParseClientFirst([]byte(clientFirst))
	if err != nil {
		t.Fatalf("parse client-first: %v", err)
	}
	if cf.Username != username {
		t.Errorf("username = %q, want %q", cf.Username, username)
	}
	if cf.Bare != "n=user,r=rOprNGfwEbeRWgbNEkqO" {
		t.Errorf("bare = %q, want the message after the gs2 header", cf.Bare)
	}

	sf, err := ParseServerFirst(serverFirst)
	if err != nil {
		t.Fatalf("parse server-first: %v", err)
	}

	// The server signature has to be recomputed, not copied from the RFC: it
	// is a function of the stored ServerKey and the auth message, so taking
	// the published string as the expected value would test nothing.
	wantProof := clientFinal[strings.LastIndex(clientFinal, ",p=")+len(",p="):]
	proof, err := base64.StdEncoding.DecodeString(wantProof)
	if err != nil {
		t.Fatalf("decode proof: %v", err)
	}

	cfFinal, err := ParseClientFinal([]byte(clientFinal), cf.GS2Header, sf.Nonce)
	if err != nil {
		t.Fatalf("parse client-final: %v", err)
	}

	authMessage := AuthMessage(cf.Bare, serverFirst, cfFinal.WithoutProof)
	sig, err := v.VerifyClientProof(authMessage, proof)
	if err != nil {
		t.Fatalf("VerifyClientProof: %v", err)
	}

	got := ServerFinal(sig)
	if got != serverFinal {
		t.Errorf("server-final\n got %s\nwant %s", got, serverFinal)
	}
}

// deriveKeys reproduces NewVerifier's key derivation from a pinned salt and
// password. NewVerifier cannot be used for the RFC vector because it draws a
// random salt, and the published exchange depends on the RFC's own salt.
//
// This is the derivation the broker must never perform at authentication time,
// so tests are the only place it belongs.
func deriveKeys(t *testing.T, mechanism, password string, salt []byte, iterations int) (storedKey, serverKey []byte) {
	t.Helper()
	hashLen, err := keyLength(mechanism)
	if err != nil {
		t.Fatal(err)
	}
	salted, err := pbkdf2(mechanism, []byte(normalizePassword(password)), salt, iterations, hashLen)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := hmacBytes(mechanism, salted, []byte("Client Key"))
	if err != nil {
		t.Fatal(err)
	}
	storedKey, err = hashBytes(mechanism, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err = hmacBytes(mechanism, salted, []byte("Server Key"))
	if err != nil {
		t.Fatal(err)
	}
	return storedKey, serverKey
}

// TestSCRAM_RFC7677StoredKey pins the derivation independently of the exchange
// check above. If PBKDF2 were wrong, the recovered ClientKey would hash to
// something other than the stored key and the proof check would fail for a
// reason that is hard to read; asserting the shape here localises it.
func TestSCRAM_RFC7677StoredKey(t *testing.T) {
	salt, err := base64.StdEncoding.DecodeString("W22ZaJ0SNY7soEsUEjb6gQ==")
	if err != nil {
		t.Fatal(err)
	}
	hashLen, _ := keyLength(MechanismSCRAMSHA256)

	stored, server := deriveKeys(t, MechanismSCRAMSHA256, "pencil", salt, 4096)
	if len(stored) != hashLen || len(server) != hashLen {
		t.Fatalf("key lengths %d/%d, want %d", len(stored), len(server), hashLen)
	}

	// StoredKey and ServerKey must differ: they are derived from the same
	// salted password under different labels, and a collision would mean the
	// two proofs are not independent.
	if string(stored) == string(server) {
		t.Error("stored key equals server key; the labels are not being applied")
	}

	// The digest is what H(ClientKey) must equal, so recompute it the short way
	// and require agreement.
	salted, _ := pbkdf2(MechanismSCRAMSHA256, []byte("pencil"), salt, 4096, hashLen)
	clientKey, _ := hmacBytes(MechanismSCRAMSHA256, salted, []byte("Client Key"))
	short, err := hashBytes(MechanismSCRAMSHA256, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored) != string(short) {
		t.Error("StoredKey is not H(ClientKey)")
	}

	// The keys must depend on the password: a derivation that ignored it would
	// still satisfy the length checks above and would accept any credential.
	otherStored, _ := deriveKeys(t, MechanismSCRAMSHA256, "penc1l", salt, 4096)
	if string(stored) == string(otherStored) {
		t.Error("derivation does not depend on the password")
	}
}

// TestSCRAM_RoundTripSHA512 exercises the same exchange at SHA-512, which has no
// published vector, so it is verified by construction: derive the verifier, run
// the exchange against it, and require success plus a proof of the right size.
func TestSCRAM_RoundTripSHA512(t *testing.T) {
	for _, mechanism := range []string{MechanismSCRAMSHA256, MechanismSCRAMSHA512} {
		t.Run(mechanism, func(t *testing.T) {
			v, err := NewVerifier(mechanism, "alice", "correct horse battery staple", 0)
			if err != nil {
				t.Fatalf("NewVerifier: %v", err)
			}
			if v.Iterations != MinIterations {
				t.Errorf("iterations = %d, want the floor %d", v.Iterations, MinIterations)
			}
			hashLen, _ := keyLength(mechanism)
			if len(v.StoredKey) != hashLen || len(v.ServerKey) != hashLen {
				t.Fatalf("key lengths %d/%d, want %d", len(v.StoredKey), len(v.ServerKey), hashLen)
			}

			serverFirst, combined, err := v.ServerFirst("clientnonce", "servernonce")
			if err != nil {
				t.Fatalf("ServerFirst: %v", err)
			}
			if combined != "clientnonceservernonce" {
				t.Errorf("combined nonce = %q", combined)
			}

			// Build the client side the way a real client would: recover the
			// salted password from the challenge, then prove it.
			clientFirst := "n,,n=alice,r=clientnonce"
			cf, err := ParseClientFirst([]byte(clientFirst))
			if err != nil {
				t.Fatalf("ParseClientFirst: %v", err)
			}
			sf, err := ParseServerFirst(string(serverFirst))
			if err != nil {
				t.Fatalf("ParseServerFirst: %v", err)
			}

			salted, err := pbkdf2(mechanism, []byte("correct horse battery staple"), sf.Salt, sf.Iterations, hashLen)
			if err != nil {
				t.Fatal(err)
			}
			clientKey, _ := hmacBytes(mechanism, salted, []byte("Client Key"))
			storedKey, _ := hashBytes(mechanism, clientKey)

			withoutProof := "c=" + base64.StdEncoding.EncodeToString([]byte("n,,")) + ",r=" + combined
			authMessage := AuthMessage(cf.Bare, string(serverFirst), withoutProof)
			clientSig, _ := hmacBytes(mechanism, storedKey, []byte(authMessage))
			proof := make([]byte, hashLen)
			for i := range proof {
				proof[i] = clientKey[i] ^ clientSig[i]
			}
			final := withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)

			cfFinal, err := ParseClientFinal([]byte(final), cf.GS2Header, combined)
			if err != nil {
				t.Fatalf("ParseClientFinal: %v", err)
			}
			sig, err := v.VerifyClientProof(AuthMessage(cf.Bare, string(serverFirst), cfFinal.WithoutProof), cfFinal.Proof)
			if err != nil {
				t.Fatalf("VerifyClientProof: %v", err)
			}

			// The server signature must be what an independently derived client
			// expects, or the client will reject the server.
			wantServer, _ := hmacBytes(mechanism, v.ServerKey, []byte(AuthMessage(cf.Bare, string(serverFirst), cfFinal.WithoutProof)))
			if string(sig) != string(wantServer) {
				t.Error("server signature does not match an independent derivation")
			}
		})
	}
}

func TestSCRAM_WrongPasswordRejected(t *testing.T) {
	v, err := NewVerifier(MechanismSCRAMSHA256, "alice", "pencil", MinIterations)
	if err != nil {
		t.Fatal(err)
	}
	// A proof computed against the wrong password must not verify. Using the
	// same message shape keeps the only difference the secret.
	hashLen, _ := keyLength(MechanismSCRAMSHA256)
	salted, _ := pbkdf2(MechanismSCRAMSHA256, []byte("wrong"), v.Salt, v.Iterations, hashLen)
	clientKey, _ := hmacBytes(MechanismSCRAMSHA256, salted, []byte("Client Key"))
	storedKey, _ := hashBytes(MechanismSCRAMSHA256, clientKey)

	authMessage := "n=alice,r=cn+sns,c=biws,r=cn+sns"
	clientSig, _ := hmacBytes(MechanismSCRAMSHA256, storedKey, []byte(authMessage))
	proof := make([]byte, hashLen)
	for i := range proof {
		proof[i] = clientKey[i] ^ clientSig[i]
	}

	if _, err := v.VerifyClientProof(authMessage, proof); !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestSCRAM_RejectsMalformedFinalMessages(t *testing.T) {
	// The security-relevant cases: a final message that does not quote the
	// server nonce, and one whose gs2 header was rewritten. Both are replay
	// and downgrade attempts, so they must be refused before the proof is even
	// considered.
	const goodProof = "p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
	tests := []struct {
		name    string
		msg     string
		gs2     string
		nonce   string
		wantErr bool
	}{
		{
			name:    "nonce does not quote the server nonce",
			msg:     "c=biws,r=someothernonce," + goodProof,
			gs2:     "n,,",
			nonce:   "cn+sns",
			wantErr: true,
		},
		{
			name:    "gs2 header rewritten",
			msg:     "c=" + base64.StdEncoding.EncodeToString([]byte("y,,")) + ",r=cn+sns," + goodProof,
			gs2:     "n,,",
			nonce:   "cn+sns",
			wantErr: true,
		},
		{
			name:    "no proof",
			msg:     "c=biws,r=cn+sns",
			gs2:     "n,,",
			nonce:   "cn+sns",
			wantErr: true,
		},
		{
			name:    "proof not last",
			msg:     "c=biws,r=cn+sns,p=abc,m=more",
			gs2:     "n,,",
			nonce:   "cn+sns",
			wantErr: true,
		},
		{
			name:    "valid shape",
			msg:     "c=biws,r=cn+sns," + goodProof,
			gs2:     "n,,",
			nonce:   "cn+sns",
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseClientFinal([]byte(tc.msg), tc.gs2, tc.nonce)
			if tc.wantErr && err == nil {
				t.Fatal("accepted a message that should be refused")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("refused a well-formed message: %v", err)
			}
		})
	}
}

func TestSCRAM_EscapedUsernameRoundTrips(t *testing.T) {
	// SCRAM escapes "=" and "," in the username; a user with either in their
	// name must still authenticate.
	const raw = "ops,team=x"
	cf, err := ParseClientFirst([]byte("n,,n=ops=2Cteam=3Dx,r=cn"))
	if err != nil {
		t.Fatalf("ParseClientFirst: %v", err)
	}
	if cf.Username != raw {
		t.Errorf("username = %q, want %q", cf.Username, raw)
	}
	if cf.Bare != "n=ops=2Cteam=3Dx,r=cn" {
		t.Errorf("bare message was altered: %q", cf.Bare)
	}
}

func TestSCRAM_UsernameWithCommaInAuthMessageIsNotRebuilt(t *testing.T) {
	// AuthMessage signs the client's bare message byte for byte. If the server
	// reconstructed it from parsed fields it would emit an unescaped comma and
	// the signatures would diverge from the client's.
	cf, err := ParseClientFirst([]byte("n,,n=ops=2Cteam=3Dx,r=cn"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(cf.Bare, "ops,team") {
		t.Error("bare message lost its escaping, so AuthMessage would not match the client")
	}
}
