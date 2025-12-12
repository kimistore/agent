package main

import (
        "context"
        "log"
        "os"
        "os/signal"
        "syscall"

        "go-stream/internal/server"
        "go-stream/internal/storage"
        "go-stream/internal/storage/s3"
)

func main() {
        log.Println("Starting WarpStream-clone Agent...")

        // Setup Storage
        ctx := context.TODO()
        
        bucket := "test-warpstream-us-east-1-andy"
        store, err := s3.NewStore(ctx, bucket, "us-east-1")
        if err != nil {
                log.Fatalf("Failed to init S3: %v", err)
        }

        walDir := "./data/wal"
        engine, err := storage.NewStorageEngine(walDir, store, bucket)
        if err != nil {
                log.Fatalf("Failed to init storage engine: %v", err)
        }
        defer engine.Close()

        srv := server.NewServer(":19092", engine)

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
