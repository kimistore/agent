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
	"syscall"

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
	ctx := context.TODO()

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
	engine, err := storage.NewStorageEngine(walDir, store, bucket, storage.RetentionConfig{})
	if err != nil {
		log.Fatalf("Failed to init storage engine: %v", err)
	}
	defer engine.Close()

	// Link Coordinator for persistence
	engine.SetCoordinator(protocol.GlobalCoordinator)

	// Initialize Metrics

	// For now, we pass dummy functions for topic/partition counts until we implement them in storage
	// or we can implement them on the engine now.
	// We will wire up the engine to provide these stats.

	// Start Metrics Server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Metrics listening on :9091")
		if err := http.ListenAndServe(":9091", nil); err != nil {
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

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	log.Println("Listening on :19092")

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down...")
	if err := srv.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}
