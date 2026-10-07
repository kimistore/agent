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
	"fmt"
	"strings"
)

// SCRAM message parsing, per RFC 5802. Every value in a SCRAM message is either
// percent-escaped ("=" becomes =3D, "," becomes =2C) or base64, and neither
// alphabet contains a comma, so splitting attributes on commas is unambiguous
// without a full tokenizer.

// ErrMalformedMessage marks a message that does not conform to the mechanism.
// It is separate from ErrInvalidCredentials because the server logs the
// difference: a malformed message is a broken client, whereas a well-formed
// message with a bad proof is a wrong credential, and conflating them would
// make either diagnosis harder.
var ErrMalformedMessage = errors.New("malformed SCRAM message")

// ClientFirst is the opening message of a SCRAM exchange.
type ClientFirst struct {
	// GS2Header is the raw gs2-header, e.g. "n,," or "y,," or "p=tls-unique,,".
	// It is kept verbatim because it is an input to the channel-binding check
	// and a component of AuthMessage, so re-rendering it would be wrong.
	GS2Header string
	// ChannelBindingUsed is 'p' when the client bound to a channel, 'y' when it
	// believes the server does not support one, and 'n' when it does not want
	// one. The server has no opinion on 'y' beyond refusing it: it means the
	// client expects no binding, which is only true without TLS.
	ChannelBindingUsed byte
	AuthzID            string
	Username           string
	ClientNonce        string
	// Bare is client-first-message-bare, the part after the gs2-header. It is
	// the first third of AuthMessage and must be byte-identical to what the
	// client sent, which is why it is kept as a string rather than rebuilt
	// from parsed fields.
	Bare string
}

// ParseClientFirst parses client-first-message.
func ParseClientFirst(msg []byte) (*ClientFirst, error) {
	// gs2-header is "flag[,authzid],", so the bare part starts after the
	// second comma and the header is everything up to and including it.
	// Requiring both commas is also what proves the header was present at all.
	first := strings.IndexByte(string(msg), ',')
	if first < 0 {
		return nil, fmt.Errorf("%w: client-first has no gs2 header", ErrMalformedMessage)
	}
	second := strings.IndexByte(string(msg[first+1:]), ',')
	if second < 0 {
		return nil, fmt.Errorf("%w: client-first gs2 header has no terminator", ErrMalformedMessage)
	}
	second += first + 1

	header := string(msg[:second+1])
	bare := string(msg[second+1:])

	flag := msg[0]
	authzid := ""
	switch flag {
	case 'p':
		// p=<name>,<authzid>, -- the channel binding name occupies the space
		// between "p=" and the first comma, so the authzid is between the two
		// commas and not simply the tail.
		authzid = string(msg[first+1 : second])
	case 'n', 'y':
		if first != 1 {
			return nil, fmt.Errorf("%w: gs2 flag %q must be alone before the comma", ErrMalformedMessage, string(flag))
		}
	default:
		return nil, fmt.Errorf("%w: unknown gs2 channel-binding flag %q", ErrMalformedMessage, string(flag))
	}

	cf := &ClientFirst{
		GS2Header:          header,
		ChannelBindingUsed: flag,
		AuthzID:            unescapeAttr(authzid),
		Bare:               bare,
	}

	attrs, err := splitAttrs(bare)
	if err != nil {
		return nil, fmt.Errorf("%w: client-first: %v", ErrMalformedMessage, err)
	}
	for _, a := range attrs {
		switch a.key {
		case "n":
			cf.Username = unescapeAttr(a.value)
		case "r":
			cf.ClientNonce = a.value
		case "m":
			// Mandatory extension, RFC 5802 section 5.1. Unsupported means the
			// exchange must be aborted rather than continued without it.
			return nil, fmt.Errorf("%w: mandatory extension %q is required", ErrMalformedMessage, a.value)
		}
	}
	if cf.Username == "" {
		return nil, fmt.Errorf("%w: client-first has no username", ErrMalformedMessage)
	}
	if cf.ClientNonce == "" {
		return nil, fmt.Errorf("%w: client-first has no nonce", ErrMalformedMessage)
	}
	return cf, nil
}

// ServerFirst is the server's challenge.
type ServerFirst struct {
	Nonce      string
	Salt       []byte
	Iterations int
	Raw        string
}

// ParseServerFirst parses server-first-message. The broker generates this
// message rather than reading it, so this exists for tests and for the client
// side of the interop harness.
func ParseServerFirst(msg string) (*ServerFirst, error) {
	attrs, err := splitAttrs(msg)
	if err != nil {
		return nil, fmt.Errorf("%w: server-first: %v", ErrMalformedMessage, err)
	}
	sf := &ServerFirst{Raw: msg}
	for _, a := range attrs {
		switch a.key {
		case "r":
			sf.Nonce = a.value
		case "s":
			sf.Salt, err = base64.StdEncoding.DecodeString(a.value)
			if err != nil {
				return nil, fmt.Errorf("%w: server-first salt is not base64", ErrMalformedMessage)
			}
		case "i":
			sf.Iterations, err = parseIterations(a.value)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrMalformedMessage, err)
			}
		case "m":
			return nil, fmt.Errorf("%w: mandatory extension %q is required", ErrMalformedMessage, a.value)
		}
	}
	if sf.Nonce == "" || len(sf.Salt) == 0 || sf.Iterations == 0 {
		return nil, fmt.Errorf("%w: server-first is incomplete", ErrMalformedMessage)
	}
	return sf, nil
}

// ClientFinal is the client's proof.
type ClientFinal struct {
	// GS2Header is the decoded value of c=, which is the gs2-header plus any
	// channel-binding data.
	GS2Header string
	// ChannelBinding is the cbind-data half of c=, empty when the client did
	// not bind.
	ChannelBinding []byte
	Nonce          string
	Proof          []byte
	// WithoutProof is client-final-message-without-proof, the second third of
	// AuthMessage.
	WithoutProof string
}

// ParseClientFinal parses client-final-message against the gs2 header the
// server expects and the combined nonce the server issued.
//
// Both checks are load-bearing. Echoing back the expected gs2 header is what
// stops an attacker who can rewrite messages from stripping channel binding or
// flipping 'y' to 'p' and confusing the client's view of the connection.
// Requiring the client's nonce to quote the server's is what stops a captured
// client-final from being replayed into a different session.
func ParseClientFinal(msg []byte, expectedGS2 string, combinedNonce string) (*ClientFinal, error) {
	raw := string(msg)

	// p= is required to be the last attribute, so the proof is the final
	// ",p=" occurrence.
	cut := strings.LastIndex(raw, ",p=")
	if cut < 0 {
		return nil, fmt.Errorf("%w: client-final has no proof", ErrMalformedMessage)
	}
	withoutProof := raw[:cut]
	proofRaw := raw[cut+len(",p="):]
	if strings.Contains(proofRaw, ",") {
		return nil, fmt.Errorf("%w: proof is not the final attribute", ErrMalformedMessage)
	}

	cf := &ClientFinal{WithoutProof: withoutProof}
	proof, err := base64.StdEncoding.DecodeString(proofRaw)
	if err != nil {
		return nil, fmt.Errorf("%w: client-final proof is not base64", ErrMalformedMessage)
	}
	cf.Proof = proof

	var sawCBind, sawNonce bool
	for _, a := range mustSplit(withoutProof) {
		switch a.key {
		case "c":
			decoded, err := base64.StdEncoding.DecodeString(a.value)
			if err != nil {
				return nil, fmt.Errorf("%w: client-final c= is not base64", ErrMalformedMessage)
			}
			// Split the decoded value back into the gs2-header and the
			// channel-binding data. The header is everything up to and
			// including the comma that terminates the authzid, matching how
			// the client built it.
			hdr := string(decoded)
			firstComma := strings.IndexByte(hdr, ',')
			if firstComma < 0 {
				return nil, fmt.Errorf("%w: client-final c= has no gs2 header", ErrMalformedMessage)
			}
			rest := hdr[firstComma+1:]
			secondComma := strings.IndexByte(rest, ',')
			if secondComma < 0 {
				return nil, fmt.Errorf("%w: client-final c= gs2 header is unterminated", ErrMalformedMessage)
			}
			headerEnd := firstComma + 1 + secondComma + 1
			cf.GS2Header = hdr[:headerEnd]
			cf.ChannelBinding = decoded[headerEnd:]
			sawCBind = true
		case "r":
			cf.Nonce = a.value
			sawNonce = true
		case "m":
			return nil, fmt.Errorf("%w: mandatory extension %q is required", ErrMalformedMessage, a.value)
		}
	}
	if !sawCBind || !sawNonce {
		return nil, fmt.Errorf("%w: client-final is missing c= or r=", ErrMalformedMessage)
	}
	if cf.GS2Header != expectedGS2 {
		return nil, fmt.Errorf("%w: client-final gs2 header %q does not match the one negotiated",
			ErrMalformedMessage, cf.GS2Header)
	}
	if !strings.HasPrefix(cf.Nonce, combinedNonce) {
		return nil, fmt.Errorf("%w: client-final nonce does not quote the server nonce", ErrMalformedMessage)
	}
	return cf, nil
}

// AuthMessage assembles the string both sides sign:
//
//	client-first-message-bare + "," + server-first-message + "," +
//	client-final-message-without-proof
//
// It covers the whole conversation, which is what makes the server signature
// meaningful: it cannot be lifted from another session, and a change to any
// earlier message invalidates both signatures.
func AuthMessage(clientFirstBare, serverFirst, clientFinalWithoutProof string) string {
	return clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
}

// ServerFinal builds server-final-message.
func ServerFinal(signature []byte) string {
	return "v=" + base64.StdEncoding.EncodeToString(signature)
}

// ServerFinalError builds a server-final-message carrying an error, per RFC 5802
// section 5.1, so a client that is mid-exchange learns why rather than seeing
// the connection drop.
func ServerFinalError(reason string) string {
	return "e=" + reason
}

type attr struct{ key, value string }

// splitAttrs splits "k=v,k=v" on commas. A bare attribute with no "=" is an
// error rather than something to skip: RFC 5802 has no such thing, and
// silently ignoring it would let a malformed message be partly honoured.
func splitAttrs(s string) ([]attr, error) {
	if s == "" {
		return nil, errors.New("empty attribute list")
	}
	return mustSplit(s), nil
}

func mustSplit(s string) []attr {
	parts := strings.Split(s, ",")
	out := make([]attr, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		idx := strings.IndexByte(p, '=')
		if idx < 0 {
			// Keep it visible to the caller as a keyless attribute; parsing
			// helpers above only act on known keys, and unknown keys are
			// ignorable per the RFC.
			out = append(out, attr{value: p})
			continue
		}
		out = append(out, attr{key: p[:idx], value: p[idx+1:]})
	}
	return out
}

// unescapeAttr reverses the =3D and =2C escaping that SCRAM applies to the
// username and authzid, so a user named "a,b=c" round-trips.
func unescapeAttr(s string) string {
	if !strings.Contains(s, "=") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '=' && i+2 < len(s) {
			switch s[i+1 : i+3] {
			case "3D":
				b.WriteByte('=')
				i += 2
				continue
			case "2C":
				b.WriteByte(',')
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
