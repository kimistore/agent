package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"kimistore/internal/storage/s3"
)

func main() {
	// NOTE: This test requires AWS credentials to be configured in env or ~/.aws/credentials
	// AND a valid bucket name.

	bucket := os.Getenv("TEST_BUCKET")
	if bucket == "" {
		fmt.Println("Skipping S3 test (TEST_BUCKET not set)")
		return
	}

	ctx := context.TODO()
	s, err := s3.NewStore(ctx, bucket, "us-east-1") // Assumed region
	if err != nil {
		log.Fatal(err)
	}

	key := "test-kimistore/hello.txt"
	body := "Hello Object Storage!"

	fmt.Printf("Uploading to %s/%s...\n", bucket, key)
	if err := s.Put(ctx, key, strings.NewReader(body)); err != nil {
		log.Fatal("Put failed:", err)
	}

	fmt.Println("Downloading...")
	r, err := s.Get(ctx, key)
	if err != nil {
		log.Fatal("Get failed:", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		log.Fatal("Read failed:", err)
	}

	fmt.Printf("Content: %s\n", string(data))
}
