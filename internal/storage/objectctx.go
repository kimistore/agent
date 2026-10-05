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
	"time"
)

// DefaultOperationTimeout bounds a single object-store operation.
//
// Every call to object storage used to inherit context.Background(), which
// means one slow or hung request could pin whatever goroutine made it
// indefinitely. In a Fetch that is a handler goroutine plus one of the
// connection's in-flight slots; once those fill, the client stops sending
// heartbeats and commits and the group rebalances around a storage stall.
// A bound turns that into a failed request the client can retry.
const DefaultOperationTimeout = 30 * time.Second

// objCtx derives the context for one object-store operation.
//
// It inherits the caller's context when there is one, so a client that
// disconnects cancels the work it started, and otherwise falls back to a
// background context bounded by the engine's timeout. Either way the result is
// cancelled when the engine shuts down, so a closing agent cannot be held
// open by an in-flight request.
//
// The returned CancelFunc must be called. Callers that hand a body to their
// caller use attachBody, which cancels on Close instead.
func (s *StorageEngine) objCtx(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, s.opTimeout)
	if s.closedCtx != nil {
		stop := context.AfterFunc(s.closedCtx, cancel)
		return ctx, func() {
			stop()
			cancel()
		}
	}
	return ctx, cancel
}

// attachBody ties a returned object body to the context that produced it.
//
// The body outlives the call that opened it, so cancelling on return would
// break every read that follows. Cancelling on Close instead means an aborted
// or dropped read releases its request as soon as the body goes away, and the
// operation timeout still caps a body that is simply never closed.
func attachBody(rc io.ReadCloser, cancel context.CancelFunc) io.ReadCloser {
	return &cancelReadCloser{ReadCloser: rc, cancel: cancel}
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelReadCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// objPut writes one object under the engine's operation bound.
func (s *StorageEngine) objPut(ctx context.Context, key string, r io.Reader) error {
	c, cancel := s.objCtx(ctx)
	defer cancel()
	return s.objStore.Put(c, key, r)
}

// objGet opens one object. The caller must close the returned body.
func (s *StorageEngine) objGet(ctx context.Context, key string) (io.ReadCloser, error) {
	c, cancel := s.objCtx(ctx)
	rc, err := s.objStore.Get(c, key)
	if err != nil {
		cancel()
		return nil, err
	}
	return attachBody(rc, cancel), nil
}

// objGetRange opens a byte range of one object. The caller must close the body.
func (s *StorageEngine) objGetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	c, cancel := s.objCtx(ctx)
	rc, err := s.objStore.GetRange(c, key, start, length)
	if err != nil {
		cancel()
		return nil, err
	}
	return attachBody(rc, cancel), nil
}

// objList lists one prefix under the engine's operation bound.
func (s *StorageEngine) objList(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	c, cancel := s.objCtx(ctx)
	defer cancel()
	return s.objStore.List(c, prefix)
}

// objDelete removes one object under the engine's operation bound.
func (s *StorageEngine) objDelete(ctx context.Context, key string) error {
	c, cancel := s.objCtx(ctx)
	defer cancel()
	return s.objStore.Delete(c, key)
}
