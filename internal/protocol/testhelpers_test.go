package protocol

import (
	"context"
	"fmt"
	"io"

	"kimistore/internal/storage"
)

// nullStore is an ObjectStore that keeps nothing. Protocol tests exercise
// encoding and dispatch, not durability.
type nullStore struct{}

func (nullStore) Put(ctx context.Context, key string, r io.Reader) error {
	_, _ = io.Copy(io.Discard, r)
	return nil
}
func (nullStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not found: %s", key)
}
func (nullStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) { return nil, nil }
func (nullStore) Delete(ctx context.Context, key string) error                      { return nil }
func (nullStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not found: %s", key)
}

// ObjectMetadata is the storage type the ObjectStore interface uses.
type ObjectMetadata = storage.ObjectMetadata
