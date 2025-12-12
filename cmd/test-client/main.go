package main

import (
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Just list brokers
	conn, err := kafka.Dial("tcp", "localhost:19092")
	if err != nil {
		log.Fatal("failed to dial:", err)
	}
	defer conn.Close()

	brokers, err := conn.Brokers()
	if err != nil {
		log.Fatal("failed to list brokers:", err)
	}

	fmt.Println("Brokers found:", len(brokers))
	for _, b := range brokers {
		fmt.Printf("- %d: %s:%d\n", b.ID, b.Host, b.Port)
	}
}
