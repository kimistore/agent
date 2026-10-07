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
	"context"
	"errors"
	"io"
	"testing"

	"kimistore/internal/auth"
	"kimistore/internal/storage"
)

// TestAuthConfig_EmptyStoreDoesNotRequireAuth is a regression test.
//
// The bug it pins: the credential store opens successfully whether or not it
// holds anything, so treating a non-nil store as "SCRAM is configured" made
// Required() true on a broker with no SASL credentials at all. Required() gates
// every request on an authenticated session, so that broker began rejecting
// everything -- including the topic auto-creation a client needs before it can
// produce. It surfaced as a Mimir e2e failure, where Mimir could not
// autocreate its ingest topic.
func TestAuthConfig_EmptyStoreDoesNotRequireAuth(t *testing.T) {
	engine, err := storage.NewStorageEngine(t.TempDir(), discardStoreForAuth{}, "test", storage.RetentionConfig{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	defer engine.Close()

	store, err := auth.NewStore(engine)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	// The store opens fine and holds nothing.
	names, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("expected an empty store, got %v", names)
	}

	// A store attached but not enabled must not enable authentication.
	cfg := AuthConfig{Credentials: store}
	if cfg.Required() {
		t.Error("an attached but disabled store made the broker require authentication")
	}
	if mechs := cfg.Mechanisms(); len(mechs) != 0 {
		t.Errorf("Mechanisms() = %v, want none", mechs)
	}

	// And the same holds for the configuration a deployment with no SASL at all
	// ends up with.
	if (AuthConfig{}).Required() {
		t.Error("an empty AuthConfig requires authentication")
	}
}

// TestAuthConfig_MechanismsReflectWhatCanBeServed pins the advertisement.
func TestAuthConfig_MechanismsReflectWhatCanBeServed(t *testing.T) {
	tests := []struct {
		name string
		cfg  AuthConfig
		want []string
	}{
		{
			name: "nothing configured",
			cfg:  AuthConfig{},
			want: nil,
		},
		{
			name: "plain only",
			cfg:  AuthConfig{Username: "u", Password: "p"},
			want: []string{auth.MechanismPlain},
		},
		{
			// The store handle alone is not consent to serve SCRAM.
			name: "store present but not enabled",
			cfg:  AuthConfig{Credentials: nil},
			want: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.cfg.Mechanisms()
			if len(got) != len(tc.want) {
				t.Fatalf("Mechanisms() = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("Mechanisms()[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestAuthConfig_Supports checks the lookup the handshake uses.
func TestAuthConfig_Supports(t *testing.T) {
	cfg := AuthConfig{Username: "u", Password: "p"}
	if !cfg.Supports(auth.MechanismPlain) {
		t.Error("PLAIN should be supported when a credential is configured")
	}
	// SCRAM needs the flag as well as a store, so this must be false here even
	// though the broker might have a store attached.
	if cfg.Supports(auth.MechanismSCRAMSHA256) {
		t.Error("SCRAM reported as supported without SCRAMEnabled")
	}
	if cfg.Supports("SCRAM-SHA-1") {
		t.Error("an unimplemented mechanism was reported as supported")
	}
	if cfg.Supports("") {
		t.Error("the empty mechanism was reported as supported")
	}
}

// discardStoreForAuth is a no-op object store; the credential store only needs
// to open successfully here.
type discardStoreForAuth struct{}

func (discardStoreForAuth) Put(context.Context, string, io.Reader) error { return nil }

func (discardStoreForAuth) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("not found")
}

func (discardStoreForAuth) List(context.Context, string) ([]storage.ObjectMetadata, error) {
	return nil, nil
}

func (discardStoreForAuth) Delete(context.Context, string) error { return nil }

func (discardStoreForAuth) GetRange(context.Context, string, int64, int64) (io.ReadCloser, error) {
	return nil, errors.New("unsupported")
}
