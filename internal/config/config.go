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

// Package config resolves the agent's runtime configuration from the
// environment.
package config

import (
	"errors"

	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the agent's startup configuration.
type Config struct {
	// ListenAddr is the address the broker socket binds to.
	ListenAddr string
	// MetricsAddr is the address the Prometheus endpoint binds to.
	MetricsAddr string

	// AdvertisedHost and AdvertisedPort are what the broker reports in
	// Metadata and FindCoordinator.
	//
	// These must be the address clients can actually reach. Hardcoding
	// "localhost" makes the agent unusable anywhere it is not running on the
	// client's own machine: a client resolves the leader from Metadata and
	// then dials its own loopback, which is the single most common way a
	// broker like this fails in a container.
	AdvertisedHost string
	AdvertisedPort int32

	WALDir   string
	S3Bucket string
	S3Region string

	SASLUsername string
	SASLPassword string

	// S3Timeout bounds a single object-store request. It is the backstop
	// under the engine's own operation timeout, which is what most request
	// paths actually observe.
	S3Timeout time.Duration

	// FlushInterval is how often a partition with an acks=all producer waiting
	// on it is sealed and uploaded. It is the coalescing window for object
	// store PUTs and the upper bound on the ack latency D2 adds.
	FlushInterval time.Duration

	Retention RetentionConfig

	// Lease configures the single-writer fence. The agent claims the log in
	// object storage before it serves anything and refuses to start if
	// another live writer holds it, because two writers on one bucket do not
	// queue behind each other: they assign the same offsets and overwrite
	// each other's segments.
	//
	// Only used when Ownership is disabled. The two are alternative fences, not
	// complementary ones: the bucket-global claim refuses every agent but the
	// first, which is what per-partition ownership exists to stop doing.
	Lease LeaseConfig

	// Ownership configures the per-partition fence, which is on by default.
	// Each (topic, partition) is claimed separately, so a second agent can
	// serve the partitions this one does not hold, and a stale owner cannot
	// corrupt the ones it does.
	Ownership OwnershipConfig

	// Assignment is the runtime shape of deterministic partition assignment.
	Assignment AssignmentConfig

	// AgentID is this agent's stable durable identity. It namespaces the
	// checkpoint object (see storage.WithAgentID) so two agents sharing a
	// bucket cannot overwrite each other's. Empty selects the hostname, which
	// is stable across a restart of the same agent.
	AgentID string

	// RequireLease makes an object store that cannot enforce conditional writes a
	// startup failure instead of a warning. The name predates partition ownership;
	// it applies to whichever fence is in force.
	RequireLease bool

	// NodeID is the broker id clients see in Metadata and FindCoordinator. It
	// must be stable across restarts and unique across agents, because that is
	// what a client uses to remember where a partition's leader is. Empty
	// derives one from AgentID, which is stable but not guaranteed unique across
	// agents -- set it explicitly when two agents could share an id.
	NodeID int32

	// AutoCreateTopics mirrors Kafka's auto.create.topics.enable. A client
	// that asks about a topic that does not exist gets one created for it,
	// which is what a producer depends on before its first write: a Metadata
	// response with no partitions leaves the client with no leader to send
	// to, and it fails with "unknown partition leader" rather than producing.
	AutoCreateTopics bool
	// AutoCreatePartitions mirrors Kafka's num.partitions and is how many
	// partitions an auto-created topic gets.
	AutoCreatePartitions int32
}

// LeaseConfig is the runtime shape of the writer lease. A zero value means
// "let the storage layer choose the defaults".
// OwnershipConfig is the runtime shape of per-partition ownership.
type OwnershipConfig struct {
	// Enabled turns per-partition claims on. It is on by default, and turns the
	// bucket-global writer lease off.
	Enabled bool
	// Agent identifies this agent in its claims. Empty selects the agent id
	// (hostname, or KIMISTORE_AGENT_ID), which must be stable across restarts.
	Agent string
	// TTL is how long a claim survives without renewal, and therefore how long
	// a crashed agent blocks its replacement for that one partition.
	TTL time.Duration
}

// AssignmentConfig is the runtime shape of deterministic partition assignment.
//
// It is off by default. Without it every agent races to claim every partition
// and the first one to start keeps all of them, so scaling out adds cost and
// failure surface without adding throughput. Assignment bounds an agent's reach
// so a fleet actually shares the work.
//
// It is opt-in because turning it on changes which agent owns what, which is a
// visible change to an existing deployment rather than a pure optimisation.
type AssignmentConfig struct {
	// Enabled turns assignment on. It requires per-partition ownership, because
	// the claim in the object store is what settles a disagreement between two
	// agents that computed different assignments.
	Enabled bool
	// Settle is how long the live agent set must hold still before this agent
	// rebalances. Without it a rolling deploy, where the live set churns as each
	// agent restarts, would release and re-acquire partitions continuously.
	Settle time.Duration
}

type LeaseConfig struct {
	// Enabled turns the fence on. It is on by default when ownership is off.
	Enabled bool
	// Key is the object the claim lives at, inside the log's bucket. Empty
	// selects the storage layer's default.
	Key string
	// Holder identifies this writer. Empty selects hostname/pid.
	Holder string
	// TTL is how long an acquisition survives without renewal, and therefore
	// how long a crashed agent blocks its replacement.
	TTL time.Duration
}

type RetentionConfig struct {
	RetentionBytes int64
	RetentionTime  time.Duration
	CheckInterval  time.Duration
}

// FromEnv builds a Config from the environment, applying defaults.
func FromEnv() Config {
	c := Config{
		ListenAddr:  env("KIMISTORE_LISTEN_ADDR", ":19092"),
		MetricsAddr: env("KIMISTORE_METRICS_ADDR", ":9091"),
		WALDir:      env("KIMISTORE_WAL_DIR", "./data/wal"),
		S3Bucket:    env("S3_BUCKET", "kimistore"),
		S3Region:    env("AWS_REGION", "us-east-1"),

		SASLUsername: os.Getenv("SASL_USERNAME"),
		SASLPassword: os.Getenv("SASL_PASSWORD"),

		AutoCreateTopics:     envBool("KIMISTORE_AUTO_CREATE_TOPICS", true),
		AutoCreatePartitions: int32(envInt64("KIMISTORE_AUTO_CREATE_PARTITIONS", 1)),

		S3Timeout:    time.Duration(envInt64("KIMISTORE_S3_TIMEOUT_MS", 30_000)) * time.Millisecond,
		RequireLease: envBool("KIMISTORE_REQUIRE_LEASE", true),
		AgentID:      os.Getenv("KIMISTORE_AGENT_ID"),

		FlushInterval: time.Duration(envInt64("KIMISTORE_FLUSH_INTERVAL_MS", 1_000)) * time.Millisecond,

		Lease: LeaseConfig{
			// Only consulted when ownership is off, so this defaults to the
			// inverse of it rather than to a separate switch: turning ownership
			// on is what turns the bucket-global claim off.
			Enabled: !envBool("KIMISTORE_PARTITION_OWNERSHIP", true) &&
				envBool("KIMISTORE_WRITER_LEASE", true),
			Key:    os.Getenv("KIMISTORE_LEASE_KEY"),
			Holder: os.Getenv("KIMISTORE_WRITER_ID"),
			// Twice the object-store timeout by default: a renewal that is
			// merely slow must not look like a lost claim, and a crashed
			// agent should not block its replacement for much longer than a
			// healthy one takes to renew.
			TTL: time.Duration(envInt64("KIMISTORE_LEASE_TTL_MS", 60_000)) * time.Millisecond,
		},

		Ownership: OwnershipConfig{
			Enabled: envBool("KIMISTORE_PARTITION_OWNERSHIP", true),
			Agent:   os.Getenv("KIMISTORE_AGENT_ID"),
			// Same reasoning as the lease TTL: a slow renewal must not read as
			// a lost partition, and a crashed agent should hand its partitions
			// over promptly.
			TTL: time.Duration(envInt64("KIMISTORE_OWNERSHIP_TTL_MS", 60_000)) * time.Millisecond,
		},

		Retention: RetentionConfig{
			RetentionBytes: envInt64("KIMISTORE_RETENTION_BYTES", -1),
			RetentionTime:  time.Duration(envInt64("KIMISTORE_RETENTION_MS", 0)) * time.Millisecond,
			CheckInterval:  time.Duration(envInt64("KIMISTORE_RETENTION_CHECK_MS", 300_000)) * time.Millisecond,
		},
	}

	c.AdvertisedHost = os.Getenv("KIMISTORE_ADVERTISED_HOST")
	if c.AdvertisedHost == "" {
		c.AdvertisedHost = defaultAdvertisedHost()
	}
	c.AdvertisedPort = int32(envInt64("KIMISTORE_ADVERTISED_PORT", int64(defaultPort(c.ListenAddr))))

	c.NodeID = c.resolveNodeID()

	return c
}

// resolveNodeID picks the broker id clients will use.
//
// It must be stable, because a client remembers a partition's leader by node id
// across Metadata refreshes: an id that changes on restart makes every client
// treat the agent as a new broker, and an id shared by two agents makes them
// indistinguishable. Derived from the agent id it is stable and practically
// collision-free for the handful of agents a landing zone runs, which is why it
// is the default; KIMISTORE_NODE_ID exists for the case where that is not good
// enough.
func (c *Config) resolveNodeID() int32 {
	if raw := os.Getenv("KIMISTORE_NODE_ID"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || v < 0 {
			log.Printf("Invalid KIMISTORE_NODE_ID=%q, deriving one from the agent id instead", raw)
		} else {
			return int32(v)
		}
	}
	return DeriveNodeID(agentIdentity(c.AgentID))
}

// agentIdentity is the id claims and routing records are namespaced by. It must
// match what the storage layer uses, so it is derived the same way.
func agentIdentity(agentID string) string {
	if strings.TrimSpace(agentID) == "" {
		return defaultHostname()
	}
	return agentID
}

func defaultHostname() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "unknown-agent"
}

// DeriveNodeID maps an agent identity onto a broker id.
//
// FNV-1a keeps it stable across restarts and dependency-free. The result is
// masked to 31 bits so it can never be negative: Kafka uses -1 to mean "no
// leader", and a broker id that could collide with that sentinel would make a
// perfectly good broker look dead in the partitions it owns.
func DeriveNodeID(agentID string) int32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(agentID); i++ {
		h ^= uint32(agentID[i])
		h *= prime32
	}
	return int32(h & 0x7FFFFFFF)
}

// defaultAdvertisedHost prefers the hostname the pod actually has, which is
// what a co-located client will resolve. Falling back to the listen address's
// own host keeps a loopback setup working out of the box.
func defaultAdvertisedHost() string {
	if h := defaultHostname(); h != "" && h != "unknown-agent" {
		return h
	}
	if h, _, err := net.SplitHostPort(env("KIMISTORE_LISTEN_ADDR", ":19092")); err == nil && h != "" {
		return h
	}
	return "localhost"
}

// defaultPort reads the port out of a listen address such as ":19092" or
// "0.0.0.0:19092".
func defaultPort(listenAddr string) int {
	if _, port, err := net.SplitHostPort(listenAddr); err == nil {
		if p, err := strconv.Atoi(port); err == nil {
			return p
		}
	}
	return 19092
}

// Validate rejects a configuration that would advertise an address the agent
// cannot serve, which is otherwise discovered only by a client failing to
// connect.
func (c *Config) Validate() error {
	if c.AdvertisedPort <= 0 || c.AdvertisedPort > 65535 {
		// Port 0 is also what an ephemeral listen address produces, which is a
		// different mistake: the port is only known after bind, and the
		// advertised value has to be set explicitly in that case.
		if defaultPort(c.ListenAddr) == 0 {
			return fmt.Errorf("listen address %q picks a port at bind time; set KIMISTORE_ADVERTISED_PORT so clients know where to dial", c.ListenAddr)
		}
		return fmt.Errorf("advertised port %d is out of range", c.AdvertisedPort)
	}
	if strings.TrimSpace(c.AdvertisedHost) == "" {
		return fmt.Errorf("advertised host is empty; set KIMISTORE_ADVERTISED_HOST")
	}
	// A claim TTL at or below the object-store timeout would let a slow
	// renewal look like a lost claim, and would let a replacement start while
	// the previous agent is merely slow rather than gone. This holds for both
	// fences, so it is checked against whichever is in force.
	ttl, label := c.Lease.TTL, "lease"
	c.Assignment.Enabled = env("KIMISTORE_ASSIGNMENT", "") == "rendezvous"
	c.Assignment.Settle = time.Duration(envInt64("KIMISTORE_ASSIGNMENT_SETTLE_MS",
		int64(c.Ownership.TTL/time.Millisecond))) * time.Millisecond
	if c.Assignment.Enabled {
		if !c.Ownership.Enabled {
			return errors.New("KIMISTORE_ASSIGNMENT=rendezvous needs KIMISTORE_PARTITION_OWNERSHIP: " +
				"the claim in the object store is what settles a disagreement about the assignment")
		}
		if c.Assignment.Settle <= 0 {
			return errors.New("KIMISTORE_ASSIGNMENT_SETTLE_MS must be greater than zero")
		}
	}

	if c.Ownership.Enabled {
		ttl, label = c.Ownership.TTL, "partition ownership"
	}
	if ttl <= c.S3Timeout {
		return fmt.Errorf("%s TTL %s must exceed the S3 timeout %s, or a slow renewal will look like a lost claim",
			label, ttl, c.S3Timeout)
	}
	return nil
}

// Log writes the resolved configuration at startup.
func (c Config) Log() {
	log.Printf("Config: listen=%s advertised=%s:%d metrics=%s wal=%s bucket=%s s3Timeout=%s flushInterval=%s",
		c.ListenAddr, c.AdvertisedHost, c.AdvertisedPort, c.MetricsAddr, c.WALDir, c.S3Bucket, c.S3Timeout, c.FlushInterval)
	if c.Ownership.Enabled {
		log.Printf("Ownership: enabled agent=%s ttl=%s (per-partition claims; the bucket-global writer lease is not acquired)",
			orDefault(c.Ownership.Agent, orDefault(c.AgentID, "hostname")), c.Ownership.TTL)
	} else {
		log.Printf("Lease: enabled=%v key=%q holder=%s ttl=%s requireConditionalWrites=%v",
			c.Lease.Enabled, orDefault(c.Lease.Key, "(default)"), orDefault(c.Lease.Holder, "hostname/pid"), c.Lease.TTL, c.RequireLease)
	}
	if c.Assignment.Enabled {
		log.Printf("Assignment: enabled mode=rendezvous settle=%s (an agent claims only the partitions "+
			"rendezvous hashing gives it, and drains the rest when the live set changes)", c.Assignment.Settle)
	}
	log.Printf("Agent: id=%s nodeId=%d (namespaces the checkpoint object; the id clients see) advertised=%s:%d",
		orDefault(c.AgentID, "hostname"), c.NodeID, c.AdvertisedHost, c.AdvertisedPort)
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envInt64(name string, def int64) int64 {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		log.Printf("Invalid %s=%q, using default %d", name, raw, def)
		return def
	}
	return v
}

// envBool reads a boolean environment variable, falling back to def.
func envBool(name string, def bool) bool {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		log.Printf("Invalid %s=%q, using default %v", name, raw, def)
		return def
	}
	return v
}
