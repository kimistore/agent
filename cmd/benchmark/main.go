package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"go-stream/internal/server"
	"go-stream/internal/storage"

	"github.com/segmentio/kafka-go"
)

// MockObjectStore (Discard)
type MockObjectStore struct{}

func (m *MockObjectStore) Put(ctx context.Context, key string, r io.Reader) error {
	// Discard all writes
	_, err := io.Copy(io.Discard, r)
	return err
}
func (m *MockObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *MockObjectStore) List(ctx context.Context, prefix string) ([]storage.ObjectMetadata, error) {
	return nil, nil
}
func (m *MockObjectStore) Delete(ctx context.Context, key string) error { return nil }

func main() {
	// 1. Setup Storage
	tmpDir, err := os.MkdirTemp("", "bench-wal")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	mockS3 := &MockObjectStore{}
	// Use default retention (infinite) or small to avoid disk fill?
	// The uploader deletes local files immediately after upload (which is fake here).
	// So disk usage should be low.

	engine, err := storage.NewStorageEngine(tmpDir, mockS3, "bench-bucket", storage.RetentionConfig{})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// 2. Setup Server
	port := "19092"
	srv := server.NewServer(":"+port, engine)
	go func() {
		if err := srv.Start(); err != nil {
			log.Printf("Server stopped: %v", err)
		}
	}()
	defer srv.Stop()

	// Wait for server to start
	time.Sleep(1 * time.Second)

	// 3. Setup Producer
	topic := "bench-topic"
	// Ensure topic exists (creates partitions)
	engine.CreateTopic(topic, 1)

	brokerAddr := "localhost:" + port

	fmt.Println("Dialing leader...")
	// DialLeader will trigger a Metadata request.
	conn, err := kafka.DialLeader(context.Background(), "tcp", brokerAddr, topic, 0)
	if err != nil {
		log.Fatalf("Failed to dial leader: %v", err)
	}
	defer conn.Close()

	// Optimize TCP
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	// 4. Benchmark Loop
	msgSize := 1024 // 1KB messages
	payload := make([]byte, msgSize)
	rand.Read(payload)

	totalMsgs := 50000
	batchSize := 1000
	fmt.Printf("Starting benchmark: %d messages of %d bytes...\n", totalMsgs, msgSize)

	start := time.Now()

	msgs := make([]kafka.Message, batchSize)
	for i := 0; i < batchSize; i++ {
		msgs[i] = kafka.Message{
			Value: payload,
		}
	}

	batches := totalMsgs / batchSize
	for i := 0; i < batches; i++ {
		conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err := conn.WriteMessages(msgs...)
		if err != nil {
			log.Printf("Write failed: %v", err)
			break
		}
		if (i+1)%100 == 0 {
			fmt.Printf("Sent %d/%d messages...\r", (i+1)*batchSize, totalMsgs)
		}
	}
	fmt.Println()

	duration := time.Since(start)
	totalBytes := int64(totalMsgs * msgSize)
	mb := float64(totalBytes) / (1024 * 1024)
	sec := duration.Seconds()

	fmt.Printf("Done in %.2f seconds.\n", sec)
	fmt.Printf("Throughput: %.2f MB/sec\n", mb/sec)
	fmt.Printf("Throughput: %.2f msgs/sec\n", float64(totalMsgs)/sec)
}
