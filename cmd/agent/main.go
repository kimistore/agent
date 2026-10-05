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
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"kimistore/internal/config"
	"kimistore/internal/metrics"
	"kimistore/internal/protocol"
	"kimistore/internal/server"
	"kimistore/internal/storage"
	"kimistore/internal/storage/s3"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	log.Println("Starting Kimistore Agent...")

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
		storage.WithOperationTimeout(cfg.S3Timeout),
		storage.WithAgentID(cfg.AgentID),
	)
	if err != nil {
		// A lease refusal and a superseded-writer refusal are both "another
		// agent owns this log" and both are fatal. Serving anyway would
		// reissue offsets that are already taken and overwrite the segments
		// behind them, which is silent data loss rather than a failed start.
		switch {
		case errors.Is(err, storage.ErrLeaseHeld):
			log.Fatalf("Another agent already holds the writer lease for this bucket: %v", err)
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

	srv := server.NewServer(cfg.ListenAddr, engine, protocol.ServerConfig{
		Auth: protocol.AuthConfig{
			Username: cfg.SASLUsername,
			Password: cfg.SASLPassword,
		},
		AdvertisedHost:       cfg.AdvertisedHost,
		AdvertisedPort:       cfg.AdvertisedPort,
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
