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
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

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

	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		bucket = "kimistore"
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	store, err := s3.NewStore(ctx, bucket, region)
	if err != nil {
		log.Fatalf("Failed to init S3: %v", err)
	}

	walDir := "./data/wal"
	retentionCfg := storage.RetentionConfig{
		RetentionBytes: envInt64("KIMISTORE_RETENTION_BYTES", -1),
		RetentionTime:  time.Duration(envInt64("KIMISTORE_RETENTION_MS", 0)) * time.Millisecond,
		CheckInterval:  time.Duration(envInt64("KIMISTORE_RETENTION_CHECK_MS", 300_000)) * time.Millisecond,
	}
	if retentionCfg.RetentionTime > 0 || retentionCfg.RetentionBytes > 0 {
		log.Printf("Retention enabled: time=%s bytes=%d checkEvery=%s",
			retentionCfg.RetentionTime, retentionCfg.RetentionBytes, retentionCfg.CheckInterval)
	} else {
		log.Println("Retention disabled (set KIMISTORE_RETENTION_MS and/or KIMISTORE_RETENTION_BYTES to enable)")
	}

	engine, err := storage.NewStorageEngine(walDir, store, bucket, retentionCfg)
	if err != nil {
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
	metricsSrv := &http.Server{Addr: ":9091", Handler: metricsMux}
	go func() {
		log.Println("Metrics listening on :9091")
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Do not Fatalf here: os.Exit would skip the deferred offset
			// flush and checkpoint below.
			log.Printf("Metrics server failed: %v", err)
		}
	}()

	saslUser := os.Getenv("SASL_USERNAME")
	saslPassword := os.Getenv("SASL_PASSWORD")
	srv := server.NewServer(":19092", engine, saslUser, saslPassword)

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

	log.Println("Listening on :19092")

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

// envInt64 reads an integer environment variable, falling back to def.
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
