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

package config

import (
	"strings"
	"testing"
	"time"
)

// clearEnv unsets everything FromEnv reads, so a developer's shell cannot
// change what a test sees.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"KIMISTORE_LISTEN_ADDR", "KIMISTORE_METRICS_ADDR", "KIMISTORE_WAL_DIR",
		"S3_BUCKET", "AWS_REGION", "S3_ENDPOINT", "SASL_USERNAME", "SASL_PASSWORD",
		"KIMISTORE_ADVERTISED_HOST", "KIMISTORE_ADVERTISED_PORT",
		"KIMISTORE_AUTO_CREATE_TOPICS", "KIMISTORE_AUTO_CREATE_PARTITIONS",
		"KIMISTORE_RETENTION_BYTES", "KIMISTORE_RETENTION_MS", "KIMISTORE_RETENTION_CHECK_MS",
		"KIMISTORE_S3_TIMEOUT_MS", "KIMISTORE_WRITER_LEASE", "KIMISTORE_LEASE_KEY",
		"KIMISTORE_WRITER_ID", "KIMISTORE_LEASE_TTL_MS", "KIMISTORE_REQUIRE_LEASE",
		"KIMISTORE_PARTITION_OWNERSHIP", "KIMISTORE_OWNERSHIP_TTL_MS", "KIMISTORE_AGENT_ID",
	} {
		t.Setenv(name, "")
	}
}

// The defaults have to be internally consistent, not just individually
// plausible: an agent that rejects its own default configuration refuses to
// start at all, which is a far worse failure than a slow one.
func TestFromEnv_DefaultsAreValid(t *testing.T) {
	clearEnv(t)

	cfg := FromEnv()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("the default configuration does not validate: %v", err)
	}

	if !cfg.Ownership.Enabled {
		t.Error("per-partition ownership must be on by default; it is what stops a second agent corrupting the log")
	}
	if cfg.Lease.Enabled {
		t.Error("the bucket-global lease must be off when ownership is on: one fence at a time")
	}
	if !cfg.RequireLease {
		t.Error("a store that cannot fence writers should be fatal by default")
	}
	if cfg.Ownership.TTL <= cfg.S3Timeout {
		t.Errorf("default ownership TTL %s must exceed the default S3 timeout %s, or a slow renewal reads as a lost claim",
			cfg.Ownership.TTL, cfg.S3Timeout)
	}
	if cfg.AdvertisedPort != 19092 {
		t.Errorf("advertised port = %d, want the listen port", cfg.AdvertisedPort)
	}
	if cfg.AdvertisedHost == "" {
		t.Error("advertised host must never be empty")
	}
}

// A claim TTL at or below the storage timeout turns every slow renewal into a
// lost claim, and lets a replacement start while the previous agent is alive.
func TestValidate_RejectsClaimTTLShorterThanTheStorageTimeout(t *testing.T) {
	clearEnv(t)

	cfg := FromEnv()
	cfg.Ownership.TTL = cfg.S3Timeout

	err := cfg.Validate()
	if err == nil {
		t.Fatal("an ownership TTL equal to the storage timeout should be rejected")
	}
	if !strings.Contains(err.Error(), "partition ownership TTL") {
		t.Fatalf("the error should name the fence in force, got %v", err)
	}

	// The bucket-global lease is subject to the same rule when it is the fence.
	clearEnv(t)
	t.Setenv("KIMISTORE_PARTITION_OWNERSHIP", "false")
	cfg = FromEnv()
	cfg.Lease.TTL = cfg.S3Timeout
	if err := cfg.Validate(); err == nil {
		t.Fatal("a lease TTL equal to the storage timeout should be rejected")
	}
	if err := cfg.Validate(); !strings.Contains(err.Error(), "lease TTL") {
		t.Fatalf("the error should name the lease, got %v", err)
	}
}

// The two fences are alternatives. Turning ownership on has to turn the
// bucket-global claim off, or a second agent would be refused outright and
// per-partition ownership would buy nothing.
func TestFromEnv_OwnershipReplacesTheBucketLease(t *testing.T) {
	clearEnv(t)
	cfg := FromEnv()
	if cfg.Ownership.Enabled != true || cfg.Lease.Enabled != false {
		t.Fatalf("defaults are ownership=%v lease=%v, want ownership on and the bucket lease off",
			cfg.Ownership.Enabled, cfg.Lease.Enabled)
	}

	// Opting out restores the previous behaviour exactly.
	clearEnv(t)
	t.Setenv("KIMISTORE_PARTITION_OWNERSHIP", "false")
	cfg = FromEnv()
	if cfg.Ownership.Enabled {
		t.Error("KIMISTORE_PARTITION_OWNERSHIP=false must disable ownership")
	}
	if !cfg.Lease.Enabled {
		t.Error("with ownership off the bucket-global writer lease must come back")
	}

	// And the bucket lease can still be switched off independently.
	clearEnv(t)
	t.Setenv("KIMISTORE_PARTITION_OWNERSHIP", "false")
	t.Setenv("KIMISTORE_WRITER_LEASE", "false")
	if cfg = FromEnv(); cfg.Lease.Enabled {
		t.Error("KIMISTORE_WRITER_LEASE=false must be honoured")
	}
}

func TestValidate_RejectsUnusableAdvertisedAddress(t *testing.T) {
	clearEnv(t)

	cfg := FromEnv()
	cfg.AdvertisedPort = 0
	if err := cfg.Validate(); err == nil {
		t.Error("port 0 cannot be advertised")
	}

	cfg = FromEnv()
	cfg.AdvertisedHost = "   "
	if err := cfg.Validate(); err == nil {
		t.Error("a blank advertised host cannot be advertised")
	}
}

func TestFromEnv_ReadsTheEnvironment(t *testing.T) {
	clearEnv(t)

	t.Setenv("KIMISTORE_LISTEN_ADDR", "0.0.0.0:19099")
	t.Setenv("KIMISTORE_WAL_DIR", "/var/lib/kimi")
	t.Setenv("KIMISTORE_ADVERTISED_HOST", "ingest.example.com")
	t.Setenv("KIMISTORE_S3_TIMEOUT_MS", "5000")
	t.Setenv("KIMISTORE_PARTITION_OWNERSHIP", "false")
	t.Setenv("KIMISTORE_WRITER_LEASE", "false")
	t.Setenv("KIMISTORE_WRITER_ID", "writer-a")
	t.Setenv("KIMISTORE_LEASE_TTL_MS", "20000")
	t.Setenv("KIMISTORE_REQUIRE_LEASE", "false")
	t.Setenv("KIMISTORE_AUTO_CREATE_PARTITIONS", "12")
	t.Setenv("KIMISTORE_RETENTION_MS", "60000")

	cfg := FromEnv()
	if cfg.WALDir != "/var/lib/kimi" {
		t.Errorf("WALDir = %q", cfg.WALDir)
	}
	if cfg.AdvertisedHost != "ingest.example.com" {
		t.Errorf("AdvertisedHost = %q", cfg.AdvertisedHost)
	}
	// The advertised port defaults to the listen port, which is what clients
	// actually dial.
	if cfg.AdvertisedPort != 19099 {
		t.Errorf("AdvertisedPort = %d, want 19099", cfg.AdvertisedPort)
	}
	if cfg.S3Timeout != 5*time.Second {
		t.Errorf("S3Timeout = %s, want 5s", cfg.S3Timeout)
	}
	if cfg.Ownership.Enabled {
		t.Error("KIMISTORE_PARTITION_OWNERSHIP=false must disable ownership")
	}
	if cfg.Lease.Enabled {
		t.Error("KIMISTORE_WRITER_LEASE=false must disable the lease")
	}
	if cfg.Lease.Holder != "writer-a" {
		t.Errorf("Lease.Holder = %q", cfg.Lease.Holder)
	}
	if cfg.Lease.TTL != 20*time.Second {
		t.Errorf("Lease.TTL = %s, want 20s", cfg.Lease.TTL)
	}
	if cfg.RequireLease {
		t.Error("KIMISTORE_REQUIRE_LEASE=false must be honoured")
	}
	if cfg.AutoCreatePartitions != 12 {
		t.Errorf("AutoCreatePartitions = %d, want 12", cfg.AutoCreatePartitions)
	}
	if cfg.Retention.RetentionTime != time.Minute {
		t.Errorf("RetentionTime = %s, want 1m", cfg.Retention.RetentionTime)
	}
}

// A typo in an integer must not take the agent down or, worse, silently
// become a plausible value.
func TestFromEnv_InvalidValuesFallBackWithDefaults(t *testing.T) {
	clearEnv(t)

	t.Setenv("KIMISTORE_S3_TIMEOUT_MS", "not-a-number")
	t.Setenv("KIMISTORE_WRITER_LEASE", "yes-please")
	t.Setenv("KIMISTORE_PARTITION_OWNERSHIP", "yes-please")
	t.Setenv("KIMISTORE_AUTO_CREATE_PARTITIONS", "3.5")

	cfg := FromEnv()
	if cfg.S3Timeout != 30*time.Second {
		t.Errorf("S3Timeout = %s, want the 30s default", cfg.S3Timeout)
	}
	if !cfg.Ownership.Enabled {
		t.Error("an unparseable boolean should fall back to the default, which is on")
	}
	if cfg.AutoCreatePartitions != 1 {
		t.Errorf("AutoCreatePartitions = %d, want the default 1", cfg.AutoCreatePartitions)
	}
}

func TestFromEnv_AdvertisedPortFromListenAddress(t *testing.T) {
	for _, addr := range []string{":19092", "0.0.0.0:19093"} {
		clearEnv(t)
		t.Setenv("KIMISTORE_LISTEN_ADDR", addr)
		cfg := FromEnv()
		if cfg.AdvertisedPort == 0 {
			t.Errorf("listen address %q produced an unusable advertised port %d", addr, cfg.AdvertisedPort)
		}
		if err := cfg.Validate(); err != nil {
			t.Errorf("listen address %q should be usable: %v", addr, err)
		}
	}

	// An ephemeral port is only known after bind, so it cannot be advertised by
	// derivation. That has to be a clear message rather than "port 0 out of
	// range", because it is a configuration mistake with an easy fix.
	clearEnv(t)
	t.Setenv("KIMISTORE_LISTEN_ADDR", "127.0.0.1:0")
	ephemeral := FromEnv()
	err := ephemeral.Validate()
	if err == nil {
		t.Fatal("an ephemeral listen port with no advertised port should be rejected")
	}
	if !strings.Contains(err.Error(), "KIMISTORE_ADVERTISED_PORT") {
		t.Fatalf("the error should point at the setting to fix, got %v", err)
	}

	clearEnv(t)
	t.Setenv("KIMISTORE_LISTEN_ADDR", "not-an-address")
	if got := FromEnv().AdvertisedPort; got != 19092 {
		t.Errorf("an unparseable listen address should fall back to 19092, got %d", got)
	}
}
