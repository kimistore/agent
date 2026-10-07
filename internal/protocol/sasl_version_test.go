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

package protocol

import (
	"testing"
)

// saslAuthenticate runs one SaslAuthenticate request against the handler and
// returns the response bytes, so the tests can assert on the wire shape rather
// than on handler behaviour.
func saslAuthenticate(t *testing.T, version int16, authBytes []byte, cfg ServerConfig) ([]byte, *Session) {
	t.Helper()
	enc := NewEncoder()
	enc.PutBytes(authBytes)

	session := &Session{}
	resp, err := handleSaslAuthenticate(NewDecoder(enc.Bytes()), NewEncoder(), version, session, cfg)
	if err != nil {
		t.Fatalf("handleSaslAuthenticate(v%d): %v", version, err)
	}
	return resp, session
}

// TestSaslAuthenticate_ResponseShapePerVersion pins the response length for each
// version.
//
// The risk this guards is specific: a v1 client reads a session_lifetime_ms
// that a v0-shaped response does not contain, so a truncated response shows up
// client-side as a short read rather than as a protocol error. Asserting exact
// lengths catches that here instead of in production.
func TestSaslAuthenticate_ResponseShapePerVersion(t *testing.T) {
	cfg := testConfig()
	cfg.Auth = AuthConfig{Username: "u", Password: "p"}
	const plainPayload = "\x00u\x00p"

	// v0: error_code(2) + error_message(2 + 0) + auth_bytes(4, null)
	const v0Len = 2 + 2 + 4
	// v1 adds session_lifetime_ms(8).
	const v1Len = v0Len + 8

	t.Run("v0 has no session lifetime", func(t *testing.T) {
		resp, _ := saslAuthenticate(t, 0, []byte(plainPayload), cfg)
		if len(resp) != v0Len {
			t.Errorf("v0 response is %d bytes, want %d", len(resp), v0Len)
		}
	})

	t.Run("v1 appends a zero session lifetime", func(t *testing.T) {
		resp, _ := saslAuthenticate(t, 1, []byte(plainPayload), cfg)
		if len(resp) != v1Len {
			t.Fatalf("v1 response is %d bytes, want %d; a short response reads as a truncated message to a client", len(resp), v1Len)
		}
		// The trailing int64 must be zero: no expiry. Anything positive would
		// tell the client to expect mid-connection reauthentication, which the
		// broker does not implement.
		lifetime := int64(0)
		for _, b := range resp[len(resp)-8:] {
			lifetime = lifetime<<8 | int64(b)
		}
		if lifetime != 0 {
			t.Errorf("session_lifetime_ms = %d, want 0 (session does not expire)", lifetime)
		}
	})
}

// TestSaslAuthenticate_EveryExitPathMatchesTheVersion checks that the success,
// bad-credential and malformed-payload paths all answer in the version the
// client asked for.
//
// Each of these used to write the response inline, so it was possible to
// answer one of them in the wrong shape. They now share a writer; this test
// exists to keep them sharing it.
func TestSaslAuthenticate_EveryExitPathMatchesTheVersion(t *testing.T) {
	cfg := testConfig()
	cfg.Auth = AuthConfig{Username: "u", Password: "p"}

	cases := []struct {
		name    string
		version int16
		payload string
		// wantMsg is the error_message the path writes. The response length
		// includes it, so it has to be part of the expectation rather than
		// assumed empty.
		wantMsg string
	}{
		{name: "success v0", version: 0, payload: "\x00u\x00p", wantMsg: ""},
		{name: "success v1", version: 1, payload: "\x00u\x00p", wantMsg: ""},
		{name: "wrong password v0", version: 0, payload: "\x00u\x00wrong", wantMsg: "Authentication failed"},
		{name: "wrong password v1", version: 1, payload: "\x00u\x00wrong", wantMsg: "Authentication failed"},
		{name: "malformed payload v0", version: 0, payload: "no-separators", wantMsg: "Invalid SASL PLAIN payload"},
		{name: "malformed payload v1", version: 1, payload: "no-separators", wantMsg: "Invalid SASL PLAIN payload"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, _ := saslAuthenticate(t, tc.version, []byte(tc.payload), cfg)
			// error_code(2) + error_message(2+len) + auth_bytes(4) [+ lifetime(8)]
			want := 2 + (2 + len(tc.wantMsg)) + 4
			if tc.version >= 1 {
				want += 8
			}
			if len(resp) != want {
				t.Errorf("response is %d bytes, want %d", len(resp), want)
			}
			dec := NewDecoder(resp)
			code, err := dec.Int16()
			if err != nil {
				t.Fatalf("read error code: %v", err)
			}
			msg, err := dec.String()
			if err != nil {
				t.Fatalf("read error message: %v", err)
			}
			if msg != tc.wantMsg {
				t.Errorf("error message = %q, want %q", msg, tc.wantMsg)
			}
			if _, err := dec.Bytes(); err != nil {
				t.Fatalf("read auth bytes: %v", err)
			}
			if tc.version >= 1 {
				lt, err := dec.Int64()
				if err != nil {
					t.Fatalf("read session lifetime: %v", err)
				}
				if lt != 0 {
					t.Errorf("session lifetime = %d, want 0", lt)
				}
			}
			// Nothing may follow the declared shape: trailing bytes would mean
			// the client reads them as the start of the next frame.
			if dec.remaining() != 0 {
				t.Errorf("%d trailing bytes after the response", dec.remaining())
			}
			if code != ErrNone && code != ErrSaslAuthenticationFailed {
				t.Errorf("unexpected error code %d", code)
			}
		})
	}
}

// TestSaslAuthenticate_AdvertisesV1 keeps the advertised ceiling and the emitted
// shape in step. Advertising v1 while answering v0 is the mismatch that would
// let a client pick v1 and then fail on a short read.
func TestSaslAuthenticate_AdvertisesV1(t *testing.T) {
	if got := supportedAPIVersions[ApiKeySaslAuthenticate]; got < 1 {
		t.Fatalf("SaslAuthenticate ceiling is %d, want at least 1", got)
	}

	// The advertised table is what a client negotiates against, so it has to
	// actually offer what the handler can produce.
	cfg := testConfig()
	cfg.Auth = AuthConfig{Username: "u", Password: "p"}
	for v := int16(0); v <= supportedAPIVersions[ApiKeySaslAuthenticate]; v++ {
		resp, session := saslAuthenticate(t, v, []byte("\x00u\x00p"), cfg)
		if !session.Authenticated {
			t.Errorf("v%d: a correct PLAIN credential did not authenticate", v)
		}
		want := 8
		if v >= 1 {
			want = 16
		}
		if len(resp) != want {
			t.Errorf("v%d: response is %d bytes, want %d", v, len(resp), want)
		}
	}
}
