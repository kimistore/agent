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

package storage

import (
	"context"
	"io"
)

// Engine represents the primary storage interface for the streaming platform.
type Engine interface {
	// Append writes a batch of records to the log for a given topic/partition.
	// Returns the base offset of the appended batch. When sync is true the
	// data is fsynced before returning.
	Append(topic string, partition int32, batch []byte, recordCount int, sync bool) (int64, error)

	// Read reads data from the log starting at the given offset.
	// It returns the data and the next offset to read from.
	Read(topic string, partition int32, offset int64) ([]byte, error)

	// Close shuts down the storage engine.
	Close() error
}

// ObjectMetadata contains information about a stored object.
type ObjectMetadata struct {
	Key          string
	Size         int64
	LastModified int64 // Unix timestamp
}

// ObjectStore is the abstraction for the cold storage layer (S3, GCS, File).
type ObjectStore interface {
	Put(ctx context.Context, key string, r io.Reader) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	List(ctx context.Context, prefix string) ([]ObjectMetadata, error)
	Delete(ctx context.Context, key string) error
	GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error)
}
