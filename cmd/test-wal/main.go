package main

import (
	"fmt"
	"log"
	"os"

	"go-stream/internal/storage/wal"
)

func main() {
	tmpDir := "./tmp-wal-test"
	os.RemoveAll(tmpDir) // cleanup

	mgr, err := wal.NewManager(tmpDir)
	if err != nil {
		log.Fatal(err)
	}
	defer mgr.Close()

	topic := "my-topic"
	partition := int32(0)

	// Write
	msg := []byte("Hello WarpStream!")
	off, err := mgr.Append(topic, partition, msg, 1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Appended at offset: %d\n", off)

	// Read
	readMsg, err := mgr.Read(topic, partition, off)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Read message: %s\n", string(readMsg))

	if string(readMsg) != string(msg) {
		log.Fatal("Message mismatch")
	}

	// Write more
	off2, _ := mgr.Append(topic, partition, []byte("Msg 2"), 1)
	fmt.Printf("Appended at offset: %d\n", off2)

	readMsg2, _ := mgr.Read(topic, partition, off2)
	fmt.Printf("Read message 2: %s\n", string(readMsg2))
}
