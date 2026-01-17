package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"kimistore/internal/server"
	"kimistore/internal/storage"
	"kimistore/internal/storage/s3"
)

func main() {
	log.Println("Starting Kimistore Agent...")

	// Setup Storage
	ctx := context.TODO()

	bucket := "kimistore"
	store, err := s3.NewStore(ctx, bucket, "garage")
	if err != nil {
		log.Fatalf("Failed to init S3: %v", err)
	}

	walDir := "./data/wal"
	engine, err := storage.NewStorageEngine(walDir, store, bucket, storage.RetentionConfig{})
	if err != nil {
		log.Fatalf("Failed to init storage engine: %v", err)
	}
	defer engine.Close()

	saslUser := os.Getenv("SASL_USERNAME")
	saslPassword := os.Getenv("SASL_PASSWORD")
	srv := server.NewServer(":19092", engine, saslUser, saslPassword)

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
