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
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
)

// Exchange drives one SCRAM authentication across the several SaslAuthenticate
// requests that make it up.
//
// SCRAM is not a request, it is a conversation, so it cannot live in a handler
// that returns after one exchange. This type owns the per-connection state and
// is driven one message at a time:
//
//	client-first   -> server-first   (challenge)
//	client-final   -> server-final   (server signature)
//
// It is safe for concurrent use because a broker serves each connection from one
// goroutine, but the connection registry is shared and a test may drive it from
// anywhere. The mutex costs nothing next to the PBKDF2 it protects.
type Exchange struct {
	mu sync.Mutex

	mechanism string
	stage     exchangeStage
	username  string
	// clientFirst is client-first-message-bare, the first third of AuthMessage.
	clientFirst string
	// gs2Header is echoed back for the client to recognise its own binding, and
	// is compared against the client's final message to catch a rewrite.
	gs2Header string
	// serverFirst is the challenge exactly as it went out. AuthMessage has to
	// quote it byte for byte, so it is retained rather than rebuilt: a rebuild
	// would depend on the attribute order and encoding happening to match, and
	// a mismatch there is a silent authentication failure for every client.
	serverFirst   []byte
	combinedNonce string
	// verifier is fetched once, when the client-first arrives, and held for the
	// rest of the exchange. Re-reading it between messages would mean a
	// credential rotated mid-exchange could validate a proof against a key the
	// client never proved: either a rotation that does not take effect, or a
	// read that fails and silently downgrades to the decoy.
	verifier *Verifier
}

// exchangeStage is how far the conversation has got. Enforcing the order is not
// bookkeeping for its own sake: a client that sends client-final without having
// sent client-first has no nonce to check a proof against, and a client that
// sends client-first twice is trying to make the server sign two different
// conversations with one credential.
type exchangeStage int

const (
	stageInitial exchangeStage = iota
	stageChallenged
	stageComplete
	stageFailed
)

// ErrExchangeOrder is returned when messages arrive out of sequence.
var ErrExchangeOrder = errors.New("SCRAM message out of order")

// ErrExchangeDone is returned once the exchange has already succeeded or
// failed, so a client cannot keep talking to a finished exchange.
var ErrExchangeDone = errors.New("SCRAM exchange already finished")

// StartExchange begins a SCRAM exchange for a mechanism.
func StartExchange(mechanism string) (*Exchange, error) {
	if _, err := newHash(mechanism); err != nil {
		return nil, err
	}
	return &Exchange{mechanism: mechanism, stage: stageInitial}, nil
}

// Mechanism returns the mechanism this exchange is for.
func (e *Exchange) Mechanism() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mechanism
}

// Username returns the username the client offered, once it has.
func (e *Exchange) Username() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.username
}

// Done reports whether the exchange has finished, successfully or not.
func (e *Exchange) Done() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stage == stageComplete || e.stage == stageFailed
}

// Challenged reports whether the server has sent its challenge, and so whether
// the client's next message is a client-final rather than a client-first.
//
// This is not the same question as Done. Between the challenge and the proof the
// exchange is neither untouched nor finished, and a caller deciding which message
// arrives next has to be able to tell those apart: routing a client-final back
// through First looks like a client sending two client-firsts, and fails an
// authentication that was about to succeed.
func (e *Exchange) Challenged() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stage == stageChallenged
}

// Complete reports whether authentication succeeded.
func (e *Exchange) Complete() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stage == stageComplete
}

// First consumes client-first-message and returns the server's challenge.
//
// The store is consulted here, once, and the credential is held for the rest of
// the exchange. A miss resolves to the decoy credential, so this path looks the
// same whether or not the user exists.
func (e *Exchange) First(ctx context.Context, msg []byte, store *Store) (challenge []byte, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	switch e.stage {
	case stageInitial:
	case stageFailed, stageComplete:
		return nil, ErrExchangeDone
	default:
		return nil, fmt.Errorf("%w: client-first after the challenge", ErrExchangeOrder)
	}

	cf, err := ParseClientFirst(msg)
	if err != nil {
		e.stage = stageFailed
		return nil, err
	}

	serverNonce, err := newNonce()
	if err != nil {
		e.stage = stageFailed
		return nil, err
	}

	// The verifier is fetched before the challenge so a store failure is a
	// failure rather than a challenge the client can complete against a decoy
	// and be told it is merely wrong.
	verifier, verr := store.VerifierFor(ctx, cf.Username, e.mechanism)
	if verr != nil {
		e.stage = stageFailed
		return nil, verr
	}
	if verifier == nil || verifier.Mechanism != e.mechanism {
		e.stage = stageFailed
		return nil, fmt.Errorf("%w: no usable credential for %q", ErrInvalidCredentials, cf.Username)
	}

	challenge, combined, err := verifier.ServerFirst(cf.ClientNonce, serverNonce)
	if err != nil {
		e.stage = stageFailed
		return nil, err
	}

	e.stage = stageChallenged
	e.username = cf.Username
	e.clientFirst = cf.Bare
	e.gs2Header = cf.GS2Header
	e.serverFirst = challenge
	e.combinedNonce = combined
	e.verifier = verifier
	return challenge, nil
}

// Final consumes client-final-message and returns the server's final message.
//
// On success the returned string is server-final-message carrying the server
// signature; a client that cannot verify it is talking to something that is not
// the broker.
func (e *Exchange) Final(msg []byte) (serverFinal string, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	switch e.stage {
	case stageChallenged:
	case stageInitial:
		e.stage = stageFailed
		return "", fmt.Errorf("%w: client-final before the challenge", ErrExchangeOrder)
	case stageComplete, stageFailed:
		return "", ErrExchangeDone
	}

	cf, err := ParseClientFinal(msg, e.gs2Header, e.combinedNonce)
	if err != nil {
		e.stage = stageFailed
		return "", err
	}

	// The AuthMessage is assembled from the bytes that actually went on the
	// wire in each direction, so the two sides cannot disagree about it.
	authMessage := AuthMessage(e.clientFirst, string(e.serverFirst), cf.WithoutProof)
	sig, err := e.verifier.VerifyClientProof(authMessage, cf.Proof)
	if err != nil {
		e.stage = stageFailed
		return "", err
	}

	e.stage = stageComplete
	return ServerFinal(sig), nil
}

// newNonce returns a fresh server nonce.
//
// 24 random bytes from crypto/rand, base64. The length is not a protocol
// requirement but a floor on entropy: SCRAM's security rests on the nonce being
// unpredictable, so a short or predictable nonce is a real weakness rather than
// a saving.
func newNonce() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("auth: generate nonce: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf), nil
}
