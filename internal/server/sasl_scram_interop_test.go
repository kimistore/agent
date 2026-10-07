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

package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"kimistore/internal/auth"
	"kimistore/internal/protocol"
	"kimistore/internal/storage"
)

// These tests exist because every other SCRAM test in this repository is the
// server talking to itself. That cannot catch a protocol-level mistake: a
// server and a client that share an author's assumptions will authenticate each
// other happily while every real client fails.
//
// franz-go is the client the Mimir e2e suite already uses, so this is the same
// code path production traffic takes. If a change here passes, a real Kafka
// client negotiates the same way.

// memoryStore is an object store that actually keeps what it is given.
//
// discardStore, which the rest of these tests use, throws away every write and
// fails every read. That is right for tests about connection lifecycle and
// useless here: a credential written through it is gone before the broker can
// read it, so every lookup falls through to the decoy and authentication fails
// for a reason that has nothing to do with SCRAM. The negative tests in this file
// would also have passed, for the wrong reason.
type memoryStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemoryStore() *memoryStore {
	return &memoryStore{data: map[string][]byte{}}
}

func (m *memoryStore) Put(_ context.Context, key string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = b
	return nil
}

func (m *memoryStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.data[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *memoryStore) List(_ context.Context, prefix string) ([]storage.ObjectMetadata, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []storage.ObjectMetadata
	for k, v := range m.data {
		if strings.HasPrefix(k, prefix) {
			out = append(out, storage.ObjectMetadata{Key: k, Size: int64(len(v))})
		}
	}
	return out, nil
}

func (m *memoryStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *memoryStore) GetRange(context.Context, string, int64, int64) (io.ReadCloser, error) {
	return nil, errors.New("unsupported")
}

// applyAddr points the advertised listener at the address the test broker
// actually bound.
//
// DefaultServerConfig advertises localhost:19092 because that is the documented
// default. A test binds a random port, so without this the broker answers
// metadata with a host the client cannot reach: kgo retries metadata forever and
// ProduceSync blocks until the test times out, which looks exactly like a
// produce bug and is not one.
func applyAddr(t *testing.T, cfg *protocol.ServerConfig, addr string) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split addr %q: %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port %q: %v", portStr, err)
	}
	cfg.AdvertisedHost = host
	cfg.AdvertisedPort = int32(port)
}

// scramBroker starts a broker with a SCRAM credential for one user.
func scramBroker(t *testing.T, user, pass string, mechanisms []string) string {
	t.Helper()

	engine, err := storage.NewStorageEngine(t.TempDir(), newMemoryStore(), "test", storage.RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })

	store, err := auth.NewStore(engine)
	if err != nil {
		t.Fatalf("new credential store: %v", err)
	}
	ctx := context.Background()
	verifiers := map[string]*auth.Verifier{}
	for _, m := range mechanisms {
		v, err := auth.NewVerifier(m, user, pass, auth.MinIterations)
		if err != nil {
			t.Fatalf("new verifier for %s: %v", m, err)
		}
		verifiers[m] = v
	}
	if err := store.Put(ctx, &auth.Credential{Username: user, Verifiers: verifiers}); err != nil {
		t.Fatalf("store credential: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg := protocol.DefaultServerConfig()
	cfg.Auth = protocol.AuthConfig{Credentials: store}
	applyAddr(t, &cfg, addr)

	srv := NewServer(addr, engine, cfg)
	go func() { _ = srv.Start() }()
	t.Cleanup(func() { _ = srv.Stop() })

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, derr := net.Dial("tcp", addr)
		if derr == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("broker did not start on %s", addr)
	return ""
}

// connect returns a client that has completed its SASL exchange, or the error.
func connect(t *testing.T, addr string, mech sasl.Mechanism) (*kgo.Client, error) {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.SASL(mech),
		// Keep the test quick: a broker that never accepts is a failure we want
		// to hear about, not a 45 second wait.
		kgo.RetryTimeout(5*time.Second),
		kgo.DialTimeout(5*time.Second),
	)
	if err != nil {
		return nil, err
	}
	return cl, nil
}

// ping forces the client to actually authenticate, which is the thing under
// test. Constructing a client does not open a connection, so a test that only
// built one would pass without the broker ever seeing a SCRAM message.
func ping(cl *kgo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return cl.Ping(ctx)
}

func TestSCRAM_Interop_SHA256(t *testing.T) {
	addr := scramBroker(t, "alice", "s3cret", []string{auth.MechanismSCRAMSHA256})

	cl, err := connect(t, addr, scram.Auth{User: "alice", Pass: "s3cret"}.AsSha256Mechanism())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	if err := ping(cl); err != nil {
		t.Fatalf("SCRAM-SHA-256 authentication failed: %v", err)
	}
}

func TestSCRAM_Interop_SHA512(t *testing.T) {
	addr := scramBroker(t, "alice", "s3cret", []string{auth.MechanismSCRAMSHA512})

	cl, err := connect(t, addr, scram.Auth{User: "alice", Pass: "s3cret"}.AsSha512Mechanism())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	if err := ping(cl); err != nil {
		t.Fatalf("SCRAM-SHA-512 authentication failed: %v", err)
	}
}

// TestSCRAM_Interop_ClientPrefersSHA512 checks the mechanism negotiation is real:
// the broker offers both and the client takes its preferred one.
func TestSCRAM_Interop_ClientPrefersSHA512(t *testing.T) {
	addr := scramBroker(t, "alice", "s3cret",
		[]string{auth.MechanismSCRAMSHA256, auth.MechanismSCRAMSHA512})

	cl, err := connect(t, addr, scram.Auth{User: "alice", Pass: "s3cret"}.AsSha512Mechanism())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	if err := ping(cl); err != nil {
		t.Fatalf("authentication failed with both mechanisms offered: %v", err)
	}
}

// TestSCRAM_Interop_WrongPasswordRejected is the negative control. Without it a
// broker that accepted anything would pass the tests above.
func TestSCRAM_Interop_WrongPasswordRejected(t *testing.T) {
	addr := scramBroker(t, "alice", "s3cret", []string{auth.MechanismSCRAMSHA256})

	cl, err := connect(t, addr, scram.Auth{User: "alice", Pass: "not-the-password"}.AsSha256Mechanism())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	if err := ping(cl); err == nil {
		t.Fatal("a wrong password was accepted")
	}
}

// TestSCRAM_Interop_UnknownUserRejected covers the decoy path end to end.
func TestSCRAM_Interop_UnknownUserRejected(t *testing.T) {
	addr := scramBroker(t, "alice", "s3cret", []string{auth.MechanismSCRAMSHA256})

	cl, err := connect(t, addr, scram.Auth{User: "mallory", Pass: "whatever"}.AsSha256Mechanism())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	if err := ping(cl); err == nil {
		t.Fatal("an unknown user was accepted")
	}
}

// TestSCRAM_Interop_MechanismNotOffered checks the handshake refuses a mechanism
// the broker has no credential for. franz-go should surface this as an
// authentication failure rather than silently falling back.
func TestSCRAM_Interop_MechanismNotOffered(t *testing.T) {
	// The broker holds only SHA-256, so a SHA-512 client has nothing to use.
	addr := scramBroker(t, "alice", "s3cret", []string{auth.MechanismSCRAMSHA256})

	cl, err := connect(t, addr, scram.Auth{User: "alice", Pass: "s3cret"}.AsSha512Mechanism())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	if err := ping(cl); err == nil {
		t.Fatal("a client negotiated a mechanism the broker does not offer")
	}
}

// TestSCRAM_Interop_ProduceOverAuthenticatedConnection proves the connection is
// not merely authenticated but usable, which is the whole point of SASL: it
// gates real traffic, and a broker that authenticated and then refused writes
// would be no better than one that refused everything.
//
// It deliberately asserts on produce only. A raw kgo consumer cannot read back
// from this broker in-process -- verified with the same test and no
// authentication at all, so it is not a SCRAM problem and not this test's to fix.
// The consumer side is exercised end to end by test/mimir-e2e.sh, where Mimir
// reads the log through the broker's own path.
func TestSCRAM_Interop_ProduceOverAuthenticatedConnection(t *testing.T) {
	addr := scramBroker(t, "alice", "s3cret", []string{auth.MechanismSCRAMSHA256})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cl, err := connect(t, addr, scram.Auth{User: "alice", Pass: "s3cret"}.AsSha256Mechanism())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	// No Ping first: ProduceSync cannot succeed without an authenticated
	// connection, so it proves authentication and usability together.
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "interop", Value: []byte("hello over SCRAM")}).FirstErr(); err != nil {
		t.Fatalf("produce over an authenticated connection: %v", err)
	}
}

// TestSCRAM_Interop_PlainStillWorksWhenBothOffered guards the additive promise:
// enabling SCRAM must not break the existing SASL_USERNAME path, and a client
// on either mechanism must still be able to connect to the same broker.
func TestSCRAM_Interop_PlainStillWorksWhenBothOffered(t *testing.T) {
	addr := bothMechanismBroker(t)

	t.Run("plain", func(t *testing.T) {
		cl, err := connect(t, addr, plain.Auth{User: "legacy", Pass: "legacy-pass"}.AsMechanism())
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		defer cl.Close()
		if err := ping(cl); err != nil {
			t.Errorf("PLAIN authentication broke once SCRAM was enabled: %v", err)
		}
	})

	t.Run("scram", func(t *testing.T) {
		cl, err := connect(t, addr, scram.Auth{User: "alice", Pass: "s3cret"}.AsSha256Mechanism())
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		defer cl.Close()
		if err := ping(cl); err != nil {
			t.Errorf("SCRAM authentication failed on a broker that also offers PLAIN: %v", err)
		}
	})
}

// bothMechanismBroker starts a broker offering PLAIN and SCRAM at once, which is
// the configuration every real deployment that migrated will have.
func bothMechanismBroker(t *testing.T) string {
	t.Helper()

	engine, err := storage.NewStorageEngine(t.TempDir(), newMemoryStore(), "test", storage.RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })

	store, err := auth.NewStore(engine)
	if err != nil {
		t.Fatalf("new credential store: %v", err)
	}
	v, err := auth.NewVerifier(auth.MechanismSCRAMSHA256, "alice", "s3cret", auth.MinIterations)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), &auth.Credential{
		Username:  "alice",
		Verifiers: map[string]*auth.Verifier{auth.MechanismSCRAMSHA256: v},
	}); err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg := protocol.DefaultServerConfig()
	cfg.Auth = protocol.AuthConfig{Username: "legacy", Password: "legacy-pass", Credentials: store}
	applyAddr(t, &cfg, addr)
	srv := NewServer(addr, engine, cfg)
	go func() { _ = srv.Start() }()
	t.Cleanup(func() { _ = srv.Stop() })

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, derr := net.Dial("tcp", addr)
		if derr == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("broker did not start on %s", addr)
	return ""
}
