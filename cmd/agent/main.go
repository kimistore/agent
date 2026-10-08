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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"kimistore/internal/auth"
	"kimistore/internal/config"
	"kimistore/internal/metrics"
	"kimistore/internal/protocol"
	"kimistore/internal/server"
	"kimistore/internal/storage"
	"kimistore/internal/storage/s3"
	"kimistore/internal/version"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// versionFlag prints the build and exits. The flag is checked before any
// configuration or storage setup so that "what version is this" needs neither
// a valid environment nor a reachable object store.
var versionFlag = flag.Bool("version", false, "Print the build version and exit")

func main() {
	flag.Parse()
	if *versionFlag {
		fmt.Println(version.String())
		return
	}

	log.Println("Starting Kimistore Agent...")
	log.Println(version.Line())

	// Setup Storage
	ctx := context.Background()

	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}
	cfg.Log()

	store, err := s3.NewStoreWithTimeout(ctx, cfg.S3Bucket, cfg.S3Region, cfg.S3Timeout)
	if err != nil {
		log.Fatalf("Failed to init S3: %v", err)
	}

	walDir := cfg.WALDir
	retentionCfg := storage.RetentionConfig{
		RetentionBytes: cfg.Retention.RetentionBytes,
		RetentionTime:  cfg.Retention.RetentionTime,
		CheckInterval:  cfg.Retention.CheckInterval,
	}
	if retentionCfg.RetentionTime > 0 || retentionCfg.RetentionBytes > 0 {
		log.Printf("Retention enabled: time=%s bytes=%d checkEvery=%s",
			retentionCfg.RetentionTime, retentionCfg.RetentionBytes, retentionCfg.CheckInterval)
	} else {
		log.Println("Retention disabled (set KIMISTORE_RETENTION_MS and/or KIMISTORE_RETENTION_BYTES to enable)")
	}

	engine, err := storage.NewStorageEngine(walDir, store, cfg.S3Bucket, retentionCfg,
		storage.WithLease(storage.LeaseConfig{
			Enabled: cfg.Lease.Enabled,
			Key:     cfg.Lease.Key,
			Holder:  cfg.Lease.Holder,
			TTL:     cfg.Lease.TTL,
			Require: cfg.RequireLease,
		}),
		storage.WithOwnership(storage.OwnershipConfig{
			Enabled: cfg.Ownership.Enabled,
			Agent:   cfg.Ownership.Agent,
			TTL:     cfg.Ownership.TTL,
			Require: cfg.RequireLease,
		}),
		storage.WithOperationTimeout(cfg.S3Timeout),
		storage.WithAgentID(cfg.AgentID),
		storage.WithFlushInterval(cfg.FlushInterval),
		storage.WithRegistry(storage.RegistryConfig{
			Enabled: true,
			AgentID: cfg.AgentID,
			NodeID:  cfg.NodeID,
			Host:    cfg.AdvertisedHost,
			Port:    cfg.AdvertisedPort,
			TTL:     cfg.Ownership.TTL,
		}),
	)
	if err != nil {
		// A lease refusal and a superseded-writer refusal are both "another
		// agent owns this log" and both are fatal. Serving anyway would
		// reissue offsets that are already taken and overwrite the segments
		// behind them, which is silent data loss rather than a failed start.
		// A partition that is merely already claimed by a live peer is not
		// fatal: this agent serves the rest of the log.
		switch {
		case errors.Is(err, storage.ErrLeaseHeld):
			log.Fatalf("Another agent already holds the writer lease for this bucket: %v", err)
		case errors.Is(err, storage.ErrPartitionHeld):
			log.Fatalf("Another agent holds a partition this agent needs to serve: %v", err)
		case strings.Contains(err.Error(), "newer writer"):
			log.Fatalf("Refusing to start: %v", err)
		}
		log.Fatalf("Failed to init storage engine: %v", err)
	}

	// Link Coordinator for persistence
	engine.SetCoordinator(protocol.GlobalCoordinator)

	// Initialize Metrics

	// For now, we pass dummy functions for topic/partition counts until we implement them in storage
	// or we can implement them on the engine now.
	// We will wire up the engine to provide these stats.

	// Start Metrics Server
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: metricsMux}
	go func() {
		log.Printf("Metrics listening on %s", cfg.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Do not Fatalf here: os.Exit would skip the deferred offset
			// flush and checkpoint below.
			log.Printf("Metrics server failed: %v", err)
		}
	}()

	log.Printf("Auto-create topics: %v (partitions per new topic: %d)", cfg.AutoCreateTopics, cfg.AutoCreatePartitions)
	log.Printf("Protocol versions advertised: %s", protocol.AdvertisedVersions(cfg.SASLUsername != ""))
	for _, key := range []int16{protocol.ApiKeyProduce, protocol.ApiKeyMetadata, protocol.ApiKeyHeartbeat} {
		if reason := protocol.VersionCeilingReason(key); reason != "" {
			log.Printf("  %s ceiling: %s", protocol.ApiName(key), reason)
		}
	}

	// SCRAM credentials live in object storage alongside the log's own control
	// plane, so every agent pointed at the bucket offers the same set. A store
	// that cannot be opened is not fatal: PLAIN keeps working, and SCRAM is
	// simply absent from the advertised mechanisms rather than advertised and
	// failing every exchange.
	authCfg := protocol.AuthConfig{
		Username: cfg.SASLUsername,
		Password: cfg.SASLPassword,
	}
	credStore, err := auth.NewStore(engine)
	if err != nil {
		log.Printf("SCRAM: credential store unavailable, SCRAM not offered: %v", err)
	} else {
		names, lerr := credStore.List(context.Background())
		switch {
		case lerr != nil:
			// Unreadable is not the same as empty. Assuming zero credentials
			// here would silently disable SCRAM for a working deployment whose
			// bucket is briefly unreachable, which is the worst time to change
			// how the broker authenticates.
			log.Printf("SCRAM: could not read %s, SCRAM not offered: %v", auth.Prefix, lerr)
		case len(names) == 0:
			log.Printf("SCRAM: no credentials under %s, SCRAM not offered", auth.Prefix)
		default:
			authCfg.Credentials = credStore
			authCfg.SCRAMEnabled = true
			log.Printf("SCRAM: %d credential(s) available under %s", len(names), auth.Prefix)
		}
	}

	// Authorization rules live in the bucket like credentials do, so several
	// agents enforce the same policy. The policy is read once at startup and
	// refreshed in the background; the request path never touches object
	// storage.
	//
	// A store that fails to read at startup is not fatal. It leaves
	// authorization off, which is the same as a deployment that never
	// configured it, rather than refusing every request because the bucket is
	// briefly unreachable.
	if aclStore, aerr := auth.NewACLStore(engine); aerr != nil {
		log.Printf("ACL: store unavailable, authorization not enforced: %v", aerr)
	} else if rerr := aclStore.Reload(ctx); rerr != nil {
		log.Printf("ACL: could not read rules, authorization not enforced: %v", rerr)
	} else if aclStore.Enabled() {
		authCfg.ACLs = aclStore
		aclStore.Start(ctx)
		log.Printf("ACL: %d rule(s) under %s", len(aclStore.Policy().ACLs()), auth.ACLPrefix)
	} else {
		log.Printf("ACL: no rules under %s, all requests allowed", auth.ACLPrefix)
	}

	srv := server.NewServer(cfg.ListenAddr, engine, protocol.ServerConfig{
		Auth:                 authCfg,
		AdvertisedHost:       cfg.AdvertisedHost,
		AdvertisedPort:       cfg.AdvertisedPort,
		NodeID:               cfg.NodeID,
		AutoCreateTopics:     cfg.AutoCreateTopics,
		AutoCreatePartitions: cfg.AutoCreatePartitions,
	})

	// Post-init metrics wiring if needed (e.g. if engine is tailored)
	// For now we'll do a simple lazy approach or just pass a closure if engine supports it.
	// But init() needs the functions.
	metrics.Init(
		func() float64 { return float64(engine.GetTopicCount()) },
		func() float64 { return float64(engine.GetPartitionCount()) },
	)

	// A fatal error in the accept loop must trigger the same clean shutdown
	// as SIGTERM, rather than os.Exit skipping our defers.
	serverErr := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil {
			serverErr <- err
		}
	}()

	log.Printf("Listening on %s", cfg.ListenAddr)

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-quit:
		log.Println("Shutting down...")
	case err := <-serverErr:
		log.Printf("Shutting down after server failure: %v", err)
	}

	// Order matters: stop accepting and tear down client connections first,
	// so nothing new arrives while the engine is flushing offsets and
	// writing its final checkpoint.
	if err := srv.Stop(); err != nil {
		log.Printf("Error stopping server: %v", err)
	}
	if err := metricsSrv.Close(); err != nil {
		log.Printf("Error stopping metrics server: %v", err)
	}
	if err := engine.Close(); err != nil {
		log.Printf("Error closing storage engine: %v", err)
	}
	protocol.GlobalCoordinator.Close()
	log.Println("Shutdown complete.")
}
