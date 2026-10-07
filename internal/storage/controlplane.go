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
	"bytes"
	"context"
	"fmt"
	"io"
)

// This file is the narrow, exported view of object storage that subsystems
// keeping their own control-plane state need.
//
// The engine's own control plane -- checkpoints, manifests, ownership, consumer
// offsets -- is private to this package because it is all internal bookkeeping
// that must agree with the log. The SASL credential store is different in kind:
// it is durable, operator-visible state that lives under the same bucket so
// several agents share one set of credentials, and it belongs to internal/auth
// rather than here. These four wrappers are the seam.
//
// They are deliberately whole-object reads and writes. A partial read or a
// conditional write would be cheaper for a large log, but a credential is a
// few hundred bytes and is touched once per connection, so the simpler
// interface is the right one to build on.

// GetObject reads an object in full.
func (s *StorageEngine) GetObject(ctx context.Context, key string) ([]byte, error) {
	r, err := s.objGet(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}

// PutObject writes an object in full.
func (s *StorageEngine) PutObject(ctx context.Context, key string, data []byte) error {
	if err := s.objPut(ctx, key, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	return nil
}

// ListObjectKeys returns the object keys under a prefix, without the prefix.
//
// Keys rather than full metadata: callers in other packages are only ever
// asking what exists under a control-plane prefix.
func (s *StorageEngine) ListObjectKeys(ctx context.Context, prefix string) ([]string, error) {
	objs, err := s.objList(ctx, prefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys, nil
}

// DeleteObject removes an object.
func (s *StorageEngine) DeleteObject(ctx context.Context, key string) error {
	return s.objDelete(ctx, key)
}
