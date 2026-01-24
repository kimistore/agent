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
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"kimistore/internal/server"
	"kimistore/internal/storage"

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
func (m *MockObjectStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return nil, nil
}

func main() {
	// 1. Config
	numPartitions := 4
	numConsumers := 4
	totalMsgs := 100000 // Increased total messages
	msgSize := 1024
	batchSize := 1000

	// 2. Setup Storage
	tmpDir, err := os.MkdirTemp("", "bench-wal")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	mockS3 := &MockObjectStore{}
	engine, err := storage.NewStorageEngine(tmpDir, mockS3, "bench-bucket", storage.RetentionConfig{})
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// 3. Setup Server
	port := "19092"
	srv := server.NewServer(":"+port, engine, "", "")
	go func() {
		if err := srv.Start(); err != nil {
			log.Printf("Server stopped: %v", err)
		}
	}()
	defer srv.Stop()

	time.Sleep(1 * time.Second)

	// 4. Create Topic with 4 Partitions
	topic := "bench-multi"
	if err := engine.CreateTopic(topic, int32(numPartitions)); err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	fmt.Printf("Created topic %s with %d partitions.\n", topic, numPartitions)

	brokerAddr := "localhost:" + port
	payload := make([]byte, msgSize)
	rand.Read(payload)

	// 5. Produce Benchmark (Distributed)
	fmt.Printf("Starting Produce Benchmark (%d msgs, %d partitions)...\n", totalMsgs, numPartitions)
	startProduce := time.Now()

	// Use kafka.Writer with RoundRobin balancer to distribute messages
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddr),
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		BatchSize:    batchSize,
		BatchBytes:   10 * 1024 * 1024,
		RequiredAcks: kafka.RequireAll,
		Compression:  kafka.Gzip,
	}
	defer writer.Close()

	msgs := make([]kafka.Message, batchSize)
	for i := 0; i < batchSize; i++ {
		msgs[i] = kafka.Message{Value: payload}
	}

	batches := totalMsgs / batchSize
	for i := 0; i < batches; i++ {
		if err := writer.WriteMessages(context.Background(), msgs...); err != nil {
			log.Fatalf("Write failed: %v", err)
		}
		if (i+1)%10 == 0 {
			fmt.Printf("Sent %d/%d messages...\r", (i+1)*batchSize, totalMsgs)
		}
	}
	fmt.Println()

	durProduce := time.Since(startProduce)
	totalBytes := int64(totalMsgs * msgSize)
	mb := float64(totalBytes) / (1024 * 1024)
	sec := durProduce.Seconds()

	fmt.Printf("Produce Done in %.2f s.\n", sec)
	fmt.Printf("Throughput: %.2f MB/sec (%.0f msgs/sec)\n", mb/sec, float64(totalMsgs)/sec)

	// 6. Consume Benchmark (Parallel)
	fmt.Printf("\nStarting Consume Benchmark (%d consumers)...\n", numConsumers)

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	var totalConsumed int64
	startConsume := time.Now()

	for i := 0; i < numConsumers; i++ {
		pID := i
		go func(partitionID int) {
			defer wg.Done()

			conn, err := kafka.DialLeader(context.Background(), "tcp", brokerAddr, topic, partitionID)
			if err != nil {
				log.Printf("Consumer %d dial failed: %v", partitionID, err)
				return
			}
			defer conn.Close()

			if _, err := conn.Seek(0, kafka.SeekStart); err != nil {
				log.Printf("Consumer %d seek failed: %v", partitionID, err)
				return
			}

			// 10s timeout per batch loop? No, refreshing deadline

			for {
				if atomic.LoadInt64(&totalConsumed) >= int64(totalMsgs) {
					break
				}

				conn.SetReadDeadline(time.Now().Add(10 * time.Second))
				batchReader := conn.ReadBatch(1, 10*1024*1024)

				for {
					_, err := batchReader.ReadMessage()
					if err != nil {
						// Batch exhausted or error (EOF usually means end of batch)
						break
					}
					// Increment global counter
					val := atomic.AddInt64(&totalConsumed, 1)
					if val >= int64(totalMsgs) {
						break
					}
				}
				batchReader.Close()
			}
		}(pID)
	}

	// Wait loop for completion instead of wg.Wait() which waits for timeout
	for {
		c := atomic.LoadInt64(&totalConsumed)
		if c >= int64(totalMsgs) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait() // Wait for all consumer goroutines to finish their cleanup (e.g., defer conn.Close())

	durConsume := time.Since(startConsume)
	secConsume := durConsume.Seconds()

	fmt.Printf("Consume Done in %.2f s.\n", secConsume)
	fmt.Printf("Throughput: %.2f MB/sec (%.0f msgs/sec)\n", mb/secConsume, float64(totalMsgs)/secConsume)
}
