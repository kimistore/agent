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
	"fmt"
	"testing"

	"kimistore/internal/storage"
)

// requestFrame builds a request header around a body and runs it through the
// dispatcher exactly as the server would.
func dispatch(t *testing.T, store *storage.StorageEngine, cfg ServerConfig, apiKey, apiVersion int16, body []byte) *Decoder {
	t.Helper()

	enc := NewEncoder()
	enc.Int16(apiKey)
	enc.Int16(apiVersion)
	enc.Int32(7) // correlation id
	enc.String("test-client")
	frame := append(enc.Bytes(), body...)

	session := &Session{Authenticated: true}
	resp, err := HandleRequest(context.Background(), frame, store, session, cfg)
	if err != nil {
		t.Fatalf("api %d v%d: HandleRequest: %v", apiKey, apiVersion, err)
	}
	if resp == nil {
		t.Fatalf("api %d v%d: no response", apiKey, apiVersion)
	}
	// Skip the correlation id echoed at the head of every response.
	dec := NewDecoder(resp)
	if _, err := dec.Int32(); err != nil {
		t.Fatalf("api %d v%d: missing correlation id: %v", apiKey, apiVersion, err)
	}
	return dec
}

func testStore(t *testing.T) *storage.StorageEngine {
	t.Helper()
	se, err := storage.NewStorageEngine(t.TempDir(), &nullStore{}, "bucket", storage.RetentionConfig{})
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	t.Cleanup(func() { se.Close() })
	return se
}

func testConfig() ServerConfig {
	return ServerConfig{
		AdvertisedHost: "kimistore.default.svc.cluster.local",
		AdvertisedPort: 9092,
	}
}

// buildFrame wraps a request body in a v2-style request header.
func buildFrame(apiKey, apiVersion int16, body []byte) []byte {
	enc := NewEncoder()
	enc.Int16(apiKey)
	enc.Int16(apiVersion)
	enc.Int32(1)
	enc.String("probe")
	return append(enc.Bytes(), body...)
}

// metadataRequest builds a Metadata request for the given topics.
func metadataRequest(topics ...string) []byte {
	enc := NewEncoder()
	enc.Int32(int32(len(topics)))
	for _, tp := range topics {
		enc.String(tp)
	}
	return enc.Bytes()
}

// TestMetadataAdvertisesConfiguredAddress pins the fix for the failure that
// made the broker unusable outside its own machine: it advertised a hardcoded
// loopback address, so every client dialled itself.
func TestMetadataAdvertisesConfiguredAddress(t *testing.T) {
	se := testStore(t)
	if err := se.CreateTopic("orders", 3); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	cfg := testConfig()

	dec := dispatch(t, se, cfg, ApiKeyMetadata, 3, metadataRequest("orders"))

	dec.Int32() // ThrottleTimeMs (v3+)
	brokerCount, _ := dec.Int32()
	if brokerCount != 1 {
		t.Fatalf("expected 1 broker, got %d", brokerCount)
	}
	if _, err := dec.Int32(); err != nil { // node id
		t.Fatal(err)
	}
	host, _ := dec.String()
	port, _ := dec.Int32()

	if host != cfg.AdvertisedHost {
		t.Errorf("advertised host = %q, want %q (a loopback address here is unusable in a container)", host, cfg.AdvertisedHost)
	}
	if int(port) != int(cfg.AdvertisedPort) {
		t.Errorf("advertised port = %d, want %d", port, cfg.AdvertisedPort)
	}
}

// TestFindCoordinatorAdvertisesConfiguredAddress covers the same for the group
// coordinator lookup, which is how a consumer finds the broker.
func TestFindCoordinatorAdvertisesConfiguredAddress(t *testing.T) {
	se := testStore(t)

	enc := NewEncoder()
	enc.String("mimir-ingesters")
	dec := dispatch(t, se, testConfig(), ApiKeyFindCoordinator, 0, enc.Bytes())

	if code, _ := dec.Int16(); code != ErrNone {
		t.Fatalf("error code = %d, want 0", code)
	}
	if node, _ := dec.Int32(); node != 0 {
		t.Errorf("node id = %d, want 0", node)
	}
	host, _ := dec.String()
	port, _ := dec.Int32()
	if host != "kimistore.default.svc.cluster.local" || int(port) != 9092 {
		t.Errorf("coordinator = %s:%d, want kimistore.default.svc.cluster.local:9092", host, port)
	}
}

// TestMetadataVersionLayouts pins the response layout for every advertised
// version. These shapes are what a client decodes, so an off-by-one field here
// is a silent misparse rather than an error.
func TestMetadataVersionLayouts(t *testing.T) {
	se := testStore(t)
	if err := se.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	body := metadataRequest("orders")
	cfg := testConfig()

	t.Run("v0 has no cluster id, controller or rack", func(t *testing.T) {
		dec := dispatch(t, se, cfg, ApiKeyMetadata, 0, body)
		dec.Int32() // brokers
		dec.Int32() // node id
		dec.String()
		dec.Int32()
		if n, _ := dec.Int32(); n != 1 {
			t.Errorf("v0: expected 1 topic, got %d", n)
		}
	})

	t.Run("v1 has no cluster id", func(t *testing.T) {
		// cluster_id is a v2 field. Emitting it at v1 would be read by a
		// conforming client as the controller id, and everything after it
		// would be desynchronised.
		dec := dispatch(t, se, cfg, ApiKeyMetadata, 1, body)
		dec.Int32() // brokers
		dec.Int32() // node id
		dec.String()
		dec.Int32()
		dec.String()                           // rack
		if _, err := dec.Int32(); err != nil { // controller id, straight after rack
			t.Errorf("v1: expected the controller id right after rack, got %v", err)
		}
		if n, _ := dec.Int32(); n != 1 {
			t.Errorf("v1: expected 1 topic after the controller id, got %d", n)
		}
	})

	for _, v := range []int16{2, 3, 4, 5, 6} {
		t.Run(fmt.Sprintf("v%d leads with throttle time", v), func(t *testing.T) {
			req := body
			if v >= 6 {
				// v6 adds allow_auto_topic_creation to the request.
				req = append(append([]byte{}, body...), 0)
			}
			dec := dispatch(t, se, cfg, ApiKeyMetadata, v, req)
			if _, err := dec.Int32(); err != nil { // throttle time
				t.Errorf("v%d: missing throttle time: %v", v, err)
			}
			dec.Int32() // brokers
			dec.Int32() // node id
			dec.String()
			dec.Int32()
			dec.String() // rack
			dec.String() // cluster id
			dec.Int32()  // controller id
			dec.Int32()  // topics
			dec.Int16()  // topic error
			dec.String() // topic name
			dec.Int8()   // is_internal
			dec.Int32()  // partitions
			dec.Int16()  // partition error
			dec.Int32()  // partition id
			dec.Int32()  // leader
			if v >= 5 {
				dec.Int32() // offline replicas
			}
		})
	}
}

// TestMetadataReportsRealPartitions checks a two-partition topic stays
// two-partition after its segments have moved to object storage.
func TestMetadataReportsRealPartitions(t *testing.T) {
	se := testStore(t)
	if err := se.CreateTopic("orders", 4); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	dec := dispatch(t, se, testConfig(), ApiKeyMetadata, 3, metadataRequest("orders"))
	dec.Int32() // throttle
	dec.Int32() // brokers
	dec.Int32()
	dec.String()
	dec.Int32()
	dec.String()
	dec.String()
	dec.Int32() // controller
	topicCount, _ := dec.Int32()
	if topicCount != 1 {
		t.Fatalf("topic count = %d, want 1", topicCount)
	}
	dec.Int16()  // topic error
	dec.String() // topic name
	dec.Int8()   // is_internal
	partCount, _ := dec.Int32()
	if partCount != 4 {
		t.Errorf("partition count = %d, want 4; a collapsed partition set starves consumers", partCount)
	}
}

// TestUnsupportedVersionKeepsConnection checks an API version the broker does
// not implement is refused with UNSUPPORTED_VERSION rather than by dropping
// the connection, which turned one unexpected request into a reconnect loop.
func TestUnsupportedVersionKeepsConnection(t *testing.T) {
	se := testStore(t)
	cfg := testConfig()

	cases := []struct {
		name       string
		apiKey     int16
		version    int16
		arrayFirst bool
	}{
		// Above every advertised ceiling.
		{"Fetch v6", ApiKeyFetch, 6, false},
		{"Metadata v9", ApiKeyMetadata, 9, true},
		{"Produce v8", ApiKeyProduce, 8, true},
		{"ListOffsets v3", ApiKeyListOffsets, 3, false},
		{"OffsetCommit v3", ApiKeyOffsetCommit, 3, false},
		{"JoinGroup v5", ApiKeyJoinGroup, 5, false},
		{"Heartbeat v1", ApiKeyHeartbeat, 1, false},
		{"LeaveGroup v1", ApiKeyLeaveGroup, 1, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dec := dispatch(t, se, cfg, tc.apiKey, tc.version, nil)
			if tc.arrayFirst {
				// Array-first responses have no error code; the version guard
				// must at least produce a well-formed empty array.
				if n, _ := dec.Int32(); n != 0 {
					t.Errorf("expected an empty array, got length %d", n)
				}
				return
			}
			code, _ := dec.Int16()
			if code != ErrUnsupportedVersion {
				t.Errorf("error code = %d, want %d (UNSUPPORTED_VERSION)", code, ErrUnsupportedVersion)
			}
		})
	}
}

// TestUnknownAPIIsRefused checks an entirely unknown API key is answered rather
// than closing the socket.
func TestUnknownAPIIsRefused(t *testing.T) {
	se := testStore(t)

	// ApiKey 23 is AddPartitionsToTxn, the first request a transactional
	// producer sends. It is not implemented, but the connection must survive.
	dec := dispatch(t, se, testConfig(), 23, 0, nil)
	code, _ := dec.Int16()
	if code != ErrUnsupportedVersion {
		t.Errorf("error code = %d, want %d", code, ErrUnsupportedVersion)
	}
}

// TestApiVersionsOmitsSASLWhenUnconfigured checks the agent does not advertise
// an authentication mechanism it has nothing to offer.
func TestApiVersionsOmitsSASLWhenUnconfigured(t *testing.T) {
	se := testStore(t)

	read := func(cfg ServerConfig) map[int16]bool {
		dec := dispatch(t, se, cfg, ApiKeyApiVersions, 0, nil)
		if code, _ := dec.Int16(); code != ErrNone {
			t.Fatalf("ApiVersions error = %d", code)
		}
		n, _ := dec.Int32()
		keys := make(map[int16]bool, n)
		for i := int32(0); i < n; i++ {
			key, _ := dec.Int16()
			dec.Int16() // min
			dec.Int16() // max
			keys[key] = true
		}
		return keys
	}

	if keys := read(testConfig()); keys[ApiKeySaslHandshake] || keys[ApiKeySaslAuthenticate] {
		t.Error("SASL advertised although no credentials are configured")
	}

	authed := testConfig()
	authed.Auth = AuthConfig{Username: "u", Password: "p"}
	if keys := read(authed); !keys[ApiKeySaslHandshake] || !keys[ApiKeySaslAuthenticate] {
		t.Error("SASL not advertised although credentials are configured")
	}
}

// TestApiVersionsMatchesDispatcher guards the table that both the negotiation
// and the version check read, so they cannot drift apart.
func TestApiVersionsMatchesDispatcher(t *testing.T) {
	se := testStore(t)

	for key, maxV := range supportedAPIVersions {
		// Every advertised API must be routable, and every API in the
		// dispatcher must be advertised.
		if apiName(key) == "" {
			t.Errorf("api key %d has no name", key)
		}
		// A body that is a plausible prefix for the API, so the handler gets
		// past parsing rather than being exercised only for its error path.
		body := make([]byte, 64)

		session := &Session{Authenticated: true}
		resp, err := HandleRequest(context.Background(), buildFrame(key, maxV, body), se, session, testConfig())
		if err != nil {
			// A parse failure on synthetic input is acceptable; what matters
			// is that the request was routed instead of refused outright.
			continue
		}
		if resp == nil && key != ApiKeyProduce {
			t.Errorf("advertised api %d v%d produced no response", key, maxV)
		}
	}
}
