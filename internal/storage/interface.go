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
	"errors"
	"io"
	"time"
)

// Sentinel errors shared by the storage layer.
var (
	// ErrVersionMismatch is returned by a conditional write when the object it
	// was conditioned on is no longer the current version. It is the compare
	// and swap failure signal, not a transport error: callers retry.
	ErrVersionMismatch = errors.New("object version mismatch")

	// ErrLeaseHeld is returned when another live writer holds the lease. The
	// agent must not serve in that state.
	ErrLeaseHeld = errors.New("object store lease is held by another writer")

	// ErrLeaseLost is returned by the write path when this agent's lease could
	// not be renewed, so it no longer knows it is the only writer.
	ErrLeaseLost = errors.New("object store lease lost")

	// ErrUnsupported is returned by an object store that cannot make writes
	// conditional on the current object version.
	ErrUnsupported = errors.New("operation not supported by this object store")
)

// Engine represents the primary storage interface for the streaming platform.
type Engine interface {
	// Append writes a batch of records to the log for a given topic/partition.
	// Returns the base offset of the appended batch. When sync is true the
	// data is fsynced before returning.
	Append(topic string, partition int32, batch []byte, recordCount int, sync bool) (int64, error)

	// AppendContext is Append with a caller context, so a client that goes
	// away does not leave the write running.
	AppendContext(ctx context.Context, topic string, partition int32, batch []byte, recordCount int, sync bool) (int64, error)

	// Read reads data from the log starting at the given offset.
	Read(topic string, partition int32, offset int64) ([]byte, error)

	// ReadBatch reads consecutive records from the log starting at offset,
	// stopping once maxBytes would be exceeded, and reports the offset the
	// caller should request next.
	ReadBatch(topic string, partition int32, offset int64, maxBytes int64) ([]byte, int64, error)

	// ReadBatchContext is ReadBatch with a caller context.
	ReadBatchContext(ctx context.Context, topic string, partition int32, offset int64, maxBytes int64) ([]byte, int64, error)

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
	// GetRange reads length bytes from start. A length of zero or less
	// reads to the end of the object.
	GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error)
}

// ConditionalObjectStore is an ObjectStore that can make a write conditional
// on the current version of the object it overwrites.
//
// It is separate from ObjectStore on purpose: it is what makes the writer
// lease a fence rather than a convention, and nothing else in the agent
// needs it. An engine pointed at a store that does not implement it still
// runs, with the lease degraded to advisory (see LeaseConfig.Require).
type ConditionalObjectStore interface {
	ObjectStore

	// GetVersion returns an object's bytes and the version token its next
	// conditional write must name. found is false when the key does not
	// exist, in which case version is empty.
	GetVersion(ctx context.Context, key string) (data []byte, version string, found bool, err error)

	// PutVersion writes data only if the object is still at version, or --
	// when version is empty -- only if it does not exist at all. It returns
	// ErrVersionMismatch when the precondition fails.
	PutVersion(ctx context.Context, key string, data []byte, version string) (newVersion string, err error)
}

// Lease is an exclusive, expiring claim on the log in object storage.
//
// Only the holder may write. Epoch increases on every acquisition and
// renewal, which makes it a fencing token: a writer whose epoch is behind the
// one recorded in the durable checkpoint knows it has been superseded and
// stops, rather than overwriting a log it no longer owns.
type Lease struct {
	Holder  string `json:"holder"`
	Epoch   int64  `json:"epoch"`
	Expires int64  `json:"expires_at"`
}

// Expired reports whether the lease is no longer held, judged against now.
func (l Lease) Expired(now time.Time) bool {
	return l.Expires <= now.Unix()
}
