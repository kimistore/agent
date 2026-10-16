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

// The byte budget must be writable the way a person says a size.
//
// KIMISTORE_RETENTION_BYTES defaults to -1, which means "no limit". So a value
// that fails to parse does not fail safe -- it silently removes the budget it was
// written to set. That is what KIMISTORE_RETENTION_BYTES=30G used to do: the
// agent logged one line and then stored without limit, which looks exactly like
// a working retention policy until the disk fills.
func TestParseByteSize(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		// A bare integer stays a bare count of bytes, so values written before
		// units existed keep their meaning across an upgrade.
		{in: "0", want: 0},
		{in: "30000000000", want: 30_000_000_000},
		{in: "-1", want: -1},

		// Plain SI letters are decimal.
		{in: "30G", want: 30_000_000_000},
		{in: "30g", want: 30_000_000_000},
		{in: "30GB", want: 30_000_000_000},
		{in: "1k", want: 1000},
		{in: "1M", want: 1_000_000},
		{in: "1T", want: 1_000_000_000_000},

		// The "i" forms are binary, and the two disagree -- which is the reason
		// both spellings exist.
		{in: "30GiB", want: 30 << 30},
		{in: "30gib", want: 30 << 30},
		{in: "30gi", want: 30 << 30},
		{in: "512MiB", want: 512 << 20},
		{in: "1KiB", want: 1024},
		{in: "1TiB", want: 1 << 40},

		// Surrounding whitespace and a trailing "b" are tolerated.
		{in: "  30G  ", want: 30_000_000_000},
		{in: "42b", want: 42},
		{in: "30B", want: 30},

		// Fractions, to a thousandth.
		{in: "1.5G", want: 1_500_000_000},
		{in: "1.5GiB", want: 1_610_612_736},
		{in: "0.5M", want: 500_000},

		// Matching is case-insensitive, so the mixed-case spellings resolve to
		// the same unit as their lowercase form rather than being refused.
		{in: "30Gb", want: 30_000_000_000},
		{in: "30Gi", want: 30 << 30},

		// Refused rather than guessed, because each of these would otherwise
		// resolve to something the operator did not ask for.
		{in: "", wantErr: true},
		{in: "30X", wantErr: true},
		{in: "G", wantErr: true},   // no number
		{in: "30.", wantErr: true}, // point with no digits
		{in: "30.5.5G", wantErr: true},
		{in: "-30G", wantErr: true},   // would mean "unlimited"
		{in: "30 000", wantErr: true}, // no separator support
		{in: "30GBs", wantErr: true},  // trailing junk
	}

	for _, tc := range tests {
		got, err := parseByteSize(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseByteSize(%q) = %d, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseByteSize(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseByteSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// The wiring, not just the parser.
//
// A parser test passes whether or not the setting is routed through it, which is
// how 1.0.21 shipped a repair that was called in the wrong place and 1.0.22 had
// to move it. So this drives the real entrypoint and asserts the setting lands.
func TestFromEnv_RetentionBytesAcceptsUnits(t *testing.T) {
	tests := []struct {
		env  string
		want int64
	}{
		{env: "30G", want: 30_000_000_000},
		{env: "30GB", want: 30_000_000_000},
		{env: "30GiB", want: 30 << 30},
		{env: "30g", want: 30_000_000_000},
		{env: "1.5G", want: 1_500_000_000},
		{env: "30000000000", want: 30_000_000_000},
		{env: "0", want: 0},
		{env: "-1", want: -1},
	}

	for _, tc := range tests {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv("KIMISTORE_RETENTION_BYTES", tc.env)
			if got := FromEnv().Retention.RetentionBytes; got != tc.want {
				t.Errorf("FromEnv().Retention.RetentionBytes = %d, want %d", got, tc.want)
			}
		})
	}
}

// An unset budget must still mean "no limit", and an unreadable one must fall
// back to that rather than to zero -- zero would mean "delete everything".
func TestFromEnv_RetentionBytesFallsBackToUnlimited(t *testing.T) {
	for _, env := range []string{"", "   ", "not-a-size", "-30G"} {
		t.Setenv("KIMISTORE_RETENTION_BYTES", env)
		if got := FromEnv().Retention.RetentionBytes; got != -1 {
			t.Errorf("KIMISTORE_RETENTION_BYTES=%q gave RetentionBytes = %d, want -1 (unlimited)",
				env, got)
		}
	}
}

// The retention window must be writable as a duration.
//
// KIMISTORE_RETENTION_MS is a real setting nobody configures by hand, and its
// unit is invisible from the value alone: 604800000 for seven days. A window
// that reads as an opaque number is also one nobody can review, and the failure
// is silent in the worst direction -- a wrong value is still a valid value, so
// the policy runs and just keeps the wrong amount of data.
func TestParseDuration(t *testing.T) {
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		// Days and weeks are the units a retention window is actually written
		// in, and time.ParseDuration cannot express them at all.
		{in: "7d", want: 7 * 24 * time.Hour},
		{in: "1d", want: 24 * time.Hour},
		{in: "14d", want: 14 * 24 * time.Hour},
		{in: "1w", want: 7 * 24 * time.Hour},
		{in: "90d", want: 90 * 24 * time.Hour},
		{in: "1.5d", want: 36 * time.Hour},

		// The rest go through time.ParseDuration, so sub-day units and its
		// error messages are Go's rather than a reimplementation.
		{in: "24h", want: 24 * time.Hour},
		{in: "168h", want: 7 * 24 * time.Hour},
		{in: "90m", want: 90 * time.Minute},
		{in: "45s", want: 45 * time.Second},
		{in: "500ms", want: 500 * time.Millisecond},
		{in: "0", want: 0},

		// A bare integer is milliseconds, which is what KIMISTORE_RETENTION_MS
		// already meant. Existing environment files keep their windows.
		{in: "604800000", want: 7 * 24 * time.Hour},

		// Case and surrounding space are tolerated.
		{in: "  7D  ", want: 7 * 24 * time.Hour},

		// Refused rather than guessed. A negative would switch the policy off,
		// since anything non-positive reads as "no policy configured".
		{in: "", wantErr: true},
		{in: "-7d", wantErr: true},
		{in: "-1000", wantErr: true},
		{in: "7", want: 7 * time.Millisecond}, // integer, not days
		{in: "d", wantErr: true},
		{in: "7q", wantErr: true},
		{in: "7 days", wantErr: true},
		{in: "next tuesday", wantErr: true},
	}

	for _, tc := range tests {
		got, err := parseDuration(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseDuration(%q) = %v, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseDuration(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseDuration(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// The wiring, not just the parser. A parser test passes whether or not the
// setting is routed through it.
func TestFromEnv_RetentionTimeAcceptsDurations(t *testing.T) {
	tests := []struct {
		env  string
		want time.Duration
	}{
		{env: "7d", want: 7 * 24 * time.Hour},
		{env: "24h", want: 24 * time.Hour},
		{env: "1w", want: 7 * 24 * time.Hour},
		{env: "90d", want: 90 * 24 * time.Hour},
		{env: "604800000", want: 7 * 24 * time.Hour}, // the old key's unit
		{env: "0", want: 0},
	}

	for _, tc := range tests {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv("KIMISTORE_RETENTION_TIME", tc.env)
			t.Setenv("KIMISTORE_RETENTION_MS", "") // not set in a clean env
			if got := FromEnv().Retention.RetentionTime; got != tc.want {
				t.Errorf("FromEnv().Retention.RetentionTime = %v, want %v", got, tc.want)
			}
		})
	}
}

// KIMISTORE_RETENTION_MS must keep working, and must lose to the readable key
// when both are present rather than silently taking precedence.
func TestFromEnv_RetentionTimePrefersReadableKey(t *testing.T) {
	t.Setenv("KIMISTORE_RETENTION_MS", "3600000") // 1h, the only setting until 1.0.25
	if got := FromEnv().Retention.RetentionTime; got != time.Hour {
		t.Errorf("RETENTION_MS alone = %v, want 1h; existing environment files must "+
			"keep their window", got)
	}

	t.Setenv("KIMISTORE_RETENTION_TIME", "7d")
	if got := FromEnv().Retention.RetentionTime; got != 7*24*time.Hour {
		t.Errorf("with both set = %v, want 7d; the readable key must win", got)
	}
}

// An unreadable window falls back to no policy, not to a short one.
func TestFromEnv_RetentionTimeFallsBackToDisabled(t *testing.T) {
	for _, env := range []string{"", "   ", "not-a-duration", "-7d"} {
		t.Setenv("KIMISTORE_RETENTION_TIME", env)
		t.Setenv("KIMISTORE_RETENTION_MS", "")
		if got := FromEnv().Retention.RetentionTime; got != 0 {
			t.Errorf("KIMISTORE_RETENTION_TIME=%q gave RetentionTime = %v, want 0 (no policy)",
				env, got)
		}
	}
}
