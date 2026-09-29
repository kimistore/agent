package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "load-test"
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:19092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	defer conn.Close()

	// 1KB payload
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = 'A'
	}

	targetSize := 1024 * 1024 * 1024 // 1GB
	totalSent := 0
	batchSize := 100 // Send 100KB per write (approx)

	startTime := time.Now()

	for totalSent < targetSize {
		msgs := make([]kafka.Message, batchSize)
		for i := 0; i < batchSize; i++ {
			msgs[i] = kafka.Message{
				Value: payload,
			}
		}

		_, err := conn.WriteMessages(msgs...)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}

		totalSent += len(payload) * batchSize

		// Progress update every 100MB
		if totalSent%(100*1024*1024) == 0 {
			elapsed := time.Since(startTime)
			rate := float64(totalSent) / 1024 / 1024 / elapsed.Seconds()
			fmt.Printf("Sent %d MB (%.2f MB/s)\n", totalSent/1024/1024, rate)
		}
	}

	fmt.Printf("Done! Sent 1GB in %v\n", time.Since(startTime))
}
