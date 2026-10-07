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

// Package auth implements the server side of the SASL mechanisms the broker
// offers.
//
// It exists separately from internal/protocol because SASL is not a request in
// the usual sense: it is a multi-round-trip conversation whose intermediate
// state belongs to the connection, not to the API key being dispatched. Keeping
// the mechanism here means the protocol layer only has to own the handshake and
// hand each message to a state machine, and it lets the mechanism be tested
// against published test vectors without a socket.
//
// SASL/PLAIN puts the password on the wire in a reversible encoding, so it is
// only safe under TLS. SCRAM-SHA-256 never transmits the password at all: the
// client proves knowledge of it with a challenge-response keyed on a
// per-credential salt and iteration count. That is the difference between a
// credential that leaks on capture and one that does not, and it is why SCRAM
// is the mechanism real Kafka clients negotiate by default.
package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"strconv"
)

// randRead is indirected so a test can pin the salt, which a published test
// vector needs: the expected derived keys depend on it.
var randRead = rand.Read

// Mechanism names, as they appear on the wire in SaslHandshake.
const (
	MechanismPlain       = "PLAIN"
	MechanismSCRAMSHA256 = "SCRAM-SHA-256"
	MechanismSCRAMSHA512 = "SCRAM-SHA-512"
)

// ErrUnsupportedMechanism is returned for a mechanism this build cannot serve.
// It maps onto the Kafka protocol's UnsupportedSaslMechanism error rather than
// a generic failure, so a client can tell "you do not speak this" from "you
// spoke it wrong".
var ErrUnsupportedMechanism = errors.New("unsupported SASL mechanism")

// ErrInvalidCredentials is returned when a SCRAM exchange does not prove
// knowledge of the stored password. It deliberately does not distinguish a
// missing user from a wrong proof: the server does not get to tell a prober
// which usernames exist.
var ErrInvalidCredentials = errors.New("authentication failed")

// Iteration bounds.
//
// The server chooses the iteration count and stores it, so a client cannot
// downgrade it. These bounds are therefore about refusing to load a corrupted
// or hostile credential object rather than about negotiating. The floor is
// RFC 7677's own recommendation; the ceiling stops a malformed record from
// turning one login into an effectively unbounded amount of CPU.
const (
	MinIterations = 4096
	MaxIterations = 1 << 20
)

// newHash returns the digest for a SCRAM mechanism name, and the hash
// constructor keyed by that digest.
func newHash(mechanism string) (func() hash.Hash, error) {
	switch mechanism {
	case MechanismSCRAMSHA256:
		return sha256.New, nil
	case MechanismSCRAMSHA512:
		return sha512.New, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedMechanism, mechanism)
	}
}

// hashBytes hashes b with the mechanism's digest.
func hashBytes(mechanism string, b []byte) ([]byte, error) {
	newHash, err := newHash(mechanism)
	if err != nil {
		return nil, err
	}
	h := newHash()
	h.Write(b)
	return h.Sum(nil), nil
}

// hmacBytes computes HMAC(key, msg) under the mechanism's digest.
func hmacBytes(mechanism string, key, msg []byte) ([]byte, error) {
	newHash, err := newHash(mechanism)
	if err != nil {
		return nil, err
	}
	mac := hmac.New(newHash, key)
	mac.Write(msg)
	return mac.Sum(nil), nil
}

// pbkdf2 is PBKDF2-HMAC (RFC 8018) over the mechanism's digest.
//
// This is written out rather than pulled from golang.org/x/crypto because the
// project ships a single static binary and advertises having no dependencies,
// and because PBKDF2 here is the shortest possible expression of a published
// construction: HMAC is already in the standard library and only the iteration
// loop is missing. The whole chain is checked end to end against the RFC 7677
// test vector in scram_test.go, so an error in here shows up as a failing
// vector rather than as a subtly weakened handshake.
//
// A non-positive iteration count is rejected rather than silently normalised:
// that would turn a corrupt credential into a fast, trivially brute-forced one.
func pbkdf2(mechanism string, password, salt []byte, iterations, keyLen int) ([]byte, error) {
	newHash, err := newHash(mechanism)
	if err != nil {
		return nil, err
	}
	if iterations <= 0 {
		return nil, errors.New("pbkdf2: iteration count must be positive")
	}
	if keyLen <= 0 {
		return nil, errors.New("pbkdf2: key length must be positive")
	}

	prf := hmac.New(newHash, password)
	hashLen := prf.Size()
	blocks := (keyLen + hashLen - 1) / hashLen

	out := make([]byte, 0, blocks*hashLen)
	buf := make([]byte, 4)
	u := make([]byte, hashLen)
	t := make([]byte, hashLen)

	for block := 1; block <= blocks; block++ {
		// U_1 = PRF(password, salt || INT_BE(block))
		prf.Reset()
		prf.Write(salt)
		buf[0] = byte(block >> 24)
		buf[1] = byte(block >> 16)
		buf[2] = byte(block >> 8)
		buf[3] = byte(block)
		prf.Write(buf)
		u = prf.Sum(u[:0])

		copy(t, u)
		for i := 1; i < iterations; i++ {
			prf.Reset()
			prf.Write(u)
			u = prf.Sum(u[:0])
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen], nil
}

// Verifier is a stored SCRAM credential: everything needed to verify a proof,
// and deliberately nothing needed to reproduce the password.
//
// Storing StoredKey and ServerKey rather than the salted password is the whole
// point. Verification derives neither the password nor the salted password, so
// a leaked bucket yields verifiers that are expensive to attack offline but do
// not hand an attacker the plaintext, and a compromised verifier for one user
// says nothing about that user's password on any other system.
type Verifier struct {
	Mechanism  string `json:"mechanism"`
	Salt       []byte `json:"salt"`
	Iterations int    `json:"iterations"`
	StoredKey  []byte `json:"stored_key"`
	ServerKey  []byte `json:"server_key"`
}

// NewVerifier derives a verifier from a plaintext password.
//
// iterations <= 0 selects MinIterations. The password is normalised the way
// SASLprep requires; for the ASCII range that is the identity function, which
// is what every realistic credential is, and the full Unicode stringprep
// profile is deliberately not implemented.
func NewVerifier(mechanism, username, password string, iterations int) (*Verifier, error) {
	newHash, err := newHash(mechanism)
	if err != nil {
		return nil, err
	}
	if iterations <= 0 {
		iterations = MinIterations
	}
	if iterations < MinIterations || iterations > MaxIterations {
		return nil, fmt.Errorf("iterations %d outside [%d,%d]", iterations, MinIterations, MaxIterations)
	}

	// A fresh salt per credential: reusing one across users lets a single
	// precomputation attack cover all of them, and lets identical passwords
	// across users be spotted without reading anything.
	salt := make([]byte, 16)
	if _, err := randRead(salt); err != nil {
		return nil, fmt.Errorf("generate salt: %w", err)
	}
	hashLen := newHash().Size()

	salted, err := pbkdf2(mechanism, []byte(normalizePassword(password)), salt, iterations, hashLen)
	if err != nil {
		return nil, err
	}

	clientKey, err := hmacBytes(mechanism, salted, []byte("Client Key"))
	if err != nil {
		return nil, err
	}
	storedKey, err := hashBytes(mechanism, clientKey)
	if err != nil {
		return nil, err
	}
	serverKey, err := hmacBytes(mechanism, salted, []byte("Server Key"))
	if err != nil {
		return nil, err
	}

	_ = username // reserved: channel-binding and authzid handling are per-mechanism
	return &Verifier{
		Mechanism:  mechanism,
		Salt:       salt,
		Iterations: iterations,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
	}, nil
}

// validate refuses to verify against a record that could not have come from
// NewVerifier. A credential object that survived a truncation or a hand edit
// would otherwise fail closed with a confusing error, or worse, be interpreted
// as a match.
func (v *Verifier) validate() error {
	if v == nil {
		return errors.New("nil verifier")
	}
	if _, err := newHash(v.Mechanism); err != nil {
		return err
	}
	if v.Iterations < MinIterations || v.Iterations > MaxIterations {
		return fmt.Errorf("stored iterations %d outside [%d,%d]", v.Iterations, MinIterations, MaxIterations)
	}
	hashLen, err := keyLength(v.Mechanism)
	if err != nil {
		return err
	}
	if len(v.Salt) == 0 {
		return errors.New("stored credential has an empty salt")
	}
	if len(v.StoredKey) != hashLen {
		return fmt.Errorf("stored key is %d bytes, want %d", len(v.StoredKey), hashLen)
	}
	if len(v.ServerKey) != hashLen {
		return fmt.Errorf("server key is %d bytes, want %d", len(v.ServerKey), hashLen)
	}
	return nil
}

// keyLength is the digest size for a mechanism.
func keyLength(mechanism string) (int, error) {
	newHash, err := newHash(mechanism)
	if err != nil {
		return 0, err
	}
	return newHash().Size(), nil
}

// ServerFirst builds the server's challenge: a combined nonce, the salt, and
// the iteration count the client must use.
//
// The nonce is the client's nonce with the server's own appended, never
// generated independently. That is what makes the final exchange verifiable:
// the client cannot be replayed against a different session, and the server
// only accepts a final message quoting a nonce it actually issued.
func (v *Verifier) ServerFirst(clientNonce, serverNonce string) (msg []byte, combinedNonce string, err error) {
	if err := v.validate(); err != nil {
		return nil, "", err
	}
	if clientNonce == "" || serverNonce == "" {
		return nil, "", errors.New("SCRAM: non-empty nonces required")
	}
	combinedNonce = clientNonce + serverNonce
	msg = []byte(fmt.Sprintf("r=%s,s=%s,i=%d",
		combinedNonce,
		base64.StdEncoding.EncodeToString(v.Salt),
		v.Iterations,
	))
	return msg, combinedNonce, nil
}

// ServerSignature proves to the client that the server holds the ServerKey,
// which is what stops a man in the middle from substituting a different server
// that merely replayed the conversation.
func (v *Verifier) ServerSignature(authMessage string) ([]byte, error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	return hmacBytes(v.Mechanism, v.ServerKey, []byte(authMessage))
}

// VerifyClientProof checks the client's proof and returns the server signature
// to send back.
//
// The check recovers ClientKey as proof XOR ClientSignature and compares
// H(ClientKey) to the stored key. That is what makes it sound: it verifies
// knowledge of the salted password without ever deriving it, and it compares
// digests rather than the password.
func (v *Verifier) VerifyClientProof(authMessage string, proof []byte) (serverSignature []byte, err error) {
	if err := v.validate(); err != nil {
		return nil, err
	}
	hashLen := len(v.StoredKey)
	if len(proof) != hashLen {
		return nil, ErrInvalidCredentials
	}

	clientSignature, err := hmacBytes(v.Mechanism, v.StoredKey, []byte(authMessage))
	if err != nil {
		return nil, err
	}
	clientKey := make([]byte, hashLen)
	for i := range proof {
		clientKey[i] = proof[i] ^ clientSignature[i]
	}

	recovered, err := hashBytes(v.Mechanism, clientKey)
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare(recovered, v.StoredKey) != 1 {
		return nil, ErrInvalidCredentials
	}

	// Only now, having proved the client is not guessing, spend the work that
	// the client is entitled to expect: the server signature.
	return hmacBytes(v.Mechanism, v.ServerKey, []byte(authMessage))
}

// normalizePassword applies SASLprep to a password.
//
// The full profile is Unicode normalisation plus prohibited and bidi rules. For
// the ASCII range -- which SASLprep maps to itself -- this is the identity,
// and refusing to pretend to do more is deliberate: a partial stringprep is
// worse than none, because two implementations can disagree about a byte and
// silently lock a user out of their own account.
func normalizePassword(password string) string {
	return password
}

// parseIterations reads the i= attribute of a server-first message, applying
// the same bounds as NewVerifier.
func parseIterations(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("scram: malformed iteration count %q", s)
	}
	if n < MinIterations || n > MaxIterations {
		return 0, fmt.Errorf("scram: iteration count %d outside [%d,%d]", n, MinIterations, MaxIterations)
	}
	return n, nil
}
