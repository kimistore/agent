package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "my-topic"
	partition := 0

	// Debug logging
	// kafka.Logger = log.New(os.Stdout, "kafka-go: ", 0)
	// kafka.ErrorLogger = log.New(os.Stderr, "kafka-go-err: ", 0)

	// DialLeader doesn't accept ReaderConfig/WriterConfig directly to set Logger.
	// But we can create a DialContext with debug? No.
	// We can use kafka.Dialer.

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		ClientID:  "test-client",
	}

	fmt.Println("Dialing leader...")
	conn, err := dialer.DialLeader(context.Background(), "tcp", "localhost:19092", topic, partition)
	if err != nil {
		log.Fatalf("failed to dial leader: %v (Type: %T)", err, err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	fmt.Println("Producing messages...")
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("one")},
		kafka.Message{Value: []byte("two")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
	conn.Close()
	fmt.Println("Produce success.")

	// 2. Consume
	fmt.Println("Consuming messages...")
	conn, err = dialer.DialLeader(context.Background(), "tcp", "localhost:19092", topic, partition)
	if err != nil {
		log.Fatalf("failed to dial leader: %v (Type: %T)", err, err)
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	batch := conn.ReadBatch(1, 1024*1024) // min/max bytes
	defer batch.Close()

	// Read messages
	for i := 0; i < 2; i++ {
		msg, err := batch.ReadMessage()
		if err == io.EOF {
			fmt.Println("Batch finished.")
			break
		}
		if err != nil {
			log.Printf("failed to read message: %v (Type: %T)", err, err)
			break
		}
		fmt.Printf("Received: %s (Offset %d)\n", string(msg.Value), msg.Offset)
	}
	fmt.Println("Consume finished.")
}
