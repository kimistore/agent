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
