package storage

import (
	"context"
	"io"
)

// Engine represents the primary storage interface for the streaming platform.
type Engine interface {
	// Append writes a batch of records to the log for a given topic/partition.
	// Returns the base offset of the appended batch.
	Append(topic string, partition int32, batch []byte) (int64, error)

	// Read reads data from the log starting at the given offset.
	// It returns the data and the next offset to read from.
	Read(topic string, partition int32, offset int64) ([]byte, error)

	// Close shuts down the storage engine.
	Close() error
}

// ObjectStore is the abstraction for the cold storage layer (S3, GCS, File).
type ObjectStore interface {
	Put(ctx context.Context, key string, r io.Reader) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	List(ctx context.Context, prefix string) ([]string, error)
	// Range(key string, start, end int64) (io.ReadCloser, error) // Future optimization
}
