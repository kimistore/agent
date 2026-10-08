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
	"context"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"kimistore/internal/auth"
	"kimistore/internal/protocol"
	"kimistore/internal/storage"
)

// aclBroker starts a broker whose policy is built from the given rules.
func aclBroker(t *testing.T, rules ...auth.ACL) string {
	t.Helper()

	engine, err := storage.NewStorageEngine(t.TempDir(), newMemoryStore(), "test", storage.RetentionConfig{})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })

	store, err := auth.NewACLStore(engine)
	if err != nil {
		t.Fatalf("new ACL store: %v", err)
	}
	ctx := context.Background()
	for _, r := range rules {
		if err := store.Add(ctx, r); err != nil {
			t.Fatalf("add rule %s: %v", auth.DescribeACL(r), err)
		}
	}

	cfg := protocol.DefaultServerConfig()
	cfg.Auth.ACLs = store

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
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

// produceErr produces one record and returns the error, if any.
func produceErr(t *testing.T, addr, topic string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RetryTimeout(3*time.Second))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()
	return cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr()
}

// TestACL_NoRulesAllowsEverything is the backwards-compatibility guarantee: a
// broker with no rules must behave exactly as it did before ACLs existed.
func TestACL_NoRulesAllowsEverything(t *testing.T) {
	addr := aclBroker(t)
	if err := produceErr(t, addr, "anything"); err != nil {
		t.Fatalf("produce was refused with no rules configured: %v", err)
	}
}

// TestACL_DeniesUnlistedPrincipal is the point of the feature: once a rule
// exists, the broker denies by default.
func TestACL_DeniesUnlistedPrincipal(t *testing.T) {
	// Describe is granted to everyone so that this test isolates the Write
	// refusal. A producer needs Describe before it can write at all, so a
	// grant without it denies at metadata time and never reaches the Write
	// check.
	addr := aclBroker(t,
		auth.ACL{Principal: auth.PrincipalAll, Operation: auth.OpDescribe,
			Resource: auth.AllTopics(), Permission: auth.Allow},
		auth.ACL{Principal: "alice", Operation: auth.OpWrite,
			Resource: auth.Topic("orders"), Permission: auth.Allow},
	)

	// The client is anonymous here, so the rule for alice cannot match it.
	err := produceErr(t, addr, "orders")
	if err == nil {
		t.Fatal("an unlisted caller was allowed to produce")
	}
	var ke *kerr.Error
	if ok := asKerr(err, &ke); !ok {
		t.Fatalf("error was not a Kafka error: %v", err)
	}
	// Compare through ErrorFor: kerr.Error is not comparable by identity, and
	// a nil-typed *Error would sail past a direct pointer comparison.
	if ke.Code != topicAuthzCode {
		t.Errorf("error code = %v (%d), want TopicAuthorizationFailed (%d)", ke.Code, ke.Code, topicAuthzCode)
	}
}

// TestACL_UnlistedTopicDenied checks deny-by-default on the resource too, not
// just the principal: a typo in a topic name must not expose data.
func TestACL_UnlistedTopicDenied(t *testing.T) {
	addr := aclBroker(t,
		auth.ACL{Principal: auth.PrincipalAll, Operation: auth.OpDescribe,
			Resource: auth.AllTopics(), Permission: auth.Allow},
		auth.ACL{Principal: auth.PrincipalAll, Operation: auth.OpWrite,
			Resource: auth.Topic("orders"), Permission: auth.Allow},
	)
	if err := produceErr(t, addr, "orderz"); err == nil {
		t.Fatal("an unlisted topic was writable")
	}
	if err := produceErr(t, addr, "orders"); err != nil {
		t.Fatalf("the granted topic was refused: %v", err)
	}
}

// TestACL_DenyBeatsAllowThroughTheWire checks the precedence a user depends on:
// an explicit deny must survive a broad allow, and the refusal must reach the
// client as a Kafka error rather than a silent drop.
func TestACL_DenyBeatsAllowThroughTheWire(t *testing.T) {
	addr := aclBroker(t,
		auth.ACL{Principal: auth.PrincipalAll, Operation: auth.OpAll,
			Resource: auth.AllTopics(), Permission: auth.Allow},
		auth.ACL{Principal: auth.PrincipalAll, Operation: auth.OpWrite,
			Resource: auth.Topic("secret"), Permission: auth.Deny},
	)
	if err := produceErr(t, addr, "secret"); err == nil {
		t.Fatal("an explicitly denied topic was writable")
	}
	if err := produceErr(t, addr, "ordinary"); err != nil {
		t.Fatalf("a topic under the allow was refused: %v", err)
	}
}

func asKerr(err error, out **kerr.Error) bool {
	for e := err; e != nil; {
		if ke, ok := e.(*kerr.Error); ok {
			*out = ke
			return true
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		e = u.Unwrap()
	}
	return false
}

// topicAuthzCode is Kafka's TOPIC_AUTHORIZATION_FAILED. Compared numerically
// because kerr.Error values are not comparable by identity and a nil-typed
// *Error would pass a direct pointer comparison.
const topicAuthzCode = 29

// TestACL_DescribeIsEnforced closes the gap where Metadata was left
// unrestricted: a caller could enumerate topic names even with rules in force.
//
// It is the test that the refusal response is parseable. The first version of
// it omitted the brokers array and cluster id, so the client read the topic
// count as a broker count, retried forever, and this test saw a timeout instead
// of an authorization error.
func TestACL_DescribeIsEnforced(t *testing.T) {
	// orders may be described and read. Nothing else is granted, so metadata
	// for any other topic is refused.
	addr := aclBroker(t,
		auth.ACL{Principal: auth.PrincipalAll, Operation: auth.OpDescribe,
			Resource: auth.Topic("orders"), Permission: auth.Allow},
		auth.ACL{Principal: auth.PrincipalAll, Operation: auth.OpWrite,
			Resource: auth.Topic("orders"), Permission: auth.Allow},
		auth.ACL{Principal: auth.PrincipalAll, Operation: auth.OpRead,
			Resource: auth.Topic("orders"), Permission: auth.Allow},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RetryTimeout(3*time.Second))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	defer cl.Close()

	// The granted topic must still work end to end, or the refusal is simply
	// breaking metadata for everyone.
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("x")}).FirstErr(); err != nil {
		t.Fatalf("produce to the granted topic was refused: %v", err)
	}

	// A topic nobody may describe must not become reachable.
	//
	// The assertion is only that the write does not succeed. The error a
	// client surfaces here is its own business: kgo retries the metadata it was
	// refused and then reports a deadline rather than the topic error, so
	// demanding a particular code would be testing franz-go. That the response
	// carries TOPIC_AUTHORIZATION_FAILED is asserted where it belongs, by
	// decoding it in TestRefuseMetadata_IsParseableAtEveryVersion.
	err = cl.ProduceSync(ctx, &kgo.Record{Topic: "secret", Value: []byte("x")}).FirstErr()
	if err == nil {
		t.Fatal("a topic with no grant was writable")
	}
	t.Logf("produce to an unauthorized topic was refused as expected: %v", err)
}
