package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"go-stream/internal/server"
)

func main() {
	log.Println("Starting WarpStream-clone Agent...")

	srv := server.NewServer(":19092")

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
