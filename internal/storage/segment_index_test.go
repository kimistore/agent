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
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// SegmentFor must pick the last segment starting at or below the offset, over
// an inventory far larger than a page of object storage could return.
func TestSegmentFor_LargeInventory(t *testing.T) {
	mc := NewMetadataCache()
	const n = 50_000
	segs := make([]*SegmentMetadata, 0, n)
	for i := 0; i < n; i++ {
		start := int64(i) * 100
		segs = append(segs, &SegmentMetadata{
			StartOffset: start,
			EndOffset:   start + 100,
			S3Key:       fmt.Sprintf("orders/0/%020d-e1.log", start),
		})
	}
	mc.SetPartitionState("orders", 0, int64(n)*100, 0, segs)

	cases := []struct {
		offset int64
		want   string
	}{
		{0, "orders/0/00000000000000000000-e1.log"},
		{99, "orders/0/00000000000000000000-e1.log"},
		{100, "orders/0/00000000000000000100-e1.log"},
		{49_999*100 + 99, fmt.Sprintf("orders/0/%020d-e1.log", 49_999*100)},
		// Before the oldest segment: nothing covers it.
		{-1, ""},
	}
	for _, tc := range cases {
		if got := mc.SegmentFor("orders", 0, tc.offset); got != tc.want {
			t.Errorf("SegmentFor(%d) = %q, want %q", tc.offset, got, tc.want)
		}
	}
}

// An unknown topic or partition must report no segment rather than panic.
func TestSegmentFor_Unknown(t *testing.T) {
	mc := NewMetadataCache()
	if got := mc.SegmentFor("nope", 0, 0); got != "" {
		t.Errorf("unknown topic: got %q, want empty", got)
	}
	mc.SetPartitionState("orders", 0, 10, 0, nil)
	if got := mc.SegmentFor("orders", 7, 0); got != "" {
		t.Errorf("unknown partition: got %q, want empty", got)
	}
	if got := mc.SegmentFor("orders", 0, 5); got != "" {
		t.Errorf("empty inventory: got %q, want empty", got)
	}
}

// listFailStore fails every LIST, proving a read resolves from the manifest
// without touching object-store listing.
type listFailStore struct {
	inner ObjectStore
	lists atomic.Int64
	gets  atomic.Int64
}

func (f *listFailStore) Put(ctx context.Context, key string, r io.Reader) error {
	return f.inner.Put(ctx, key, r)
}
func (f *listFailStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	f.gets.Add(1)
	return f.inner.Get(ctx, key)
}
func (f *listFailStore) Delete(ctx context.Context, key string) error {
	return f.inner.Delete(ctx, key)
}
func (f *listFailStore) GetRange(ctx context.Context, key string, start, length int64) (io.ReadCloser, error) {
	return f.inner.GetRange(ctx, key, start, length)
}
func (f *listFailStore) GetVersion(ctx context.Context, key string) ([]byte, string, bool, error) {
	if c, ok := f.inner.(ConditionalObjectStore); ok {
		return c.GetVersion(ctx, key)
	}
	return nil, "", false, ErrUnsupported
}
func (f *listFailStore) PutVersion(ctx context.Context, key string, data []byte, version string) (string, error) {
	if c, ok := f.inner.(ConditionalObjectStore); ok {
		return c.PutVersion(ctx, key, data, version)
	}
	return "", ErrUnsupported
}
func (f *listFailStore) List(ctx context.Context, prefix string) ([]ObjectMetadata, error) {
	f.lists.Add(1)
	return nil, fmt.Errorf("LIST is not available")
}

// A read for an offset below the local WAL must resolve through the manifest's
// inventory. Before this, the read path listed the whole partition prefix, and
// on a partition with tens of thousands of segments that listing cannot
// complete inside the operation timeout -- so every historical read failed and
// a consumer could never catch up.
func TestReadBatchContext_ResolvesFromManifestWithoutListing(t *testing.T) {
	store := &listFailStore{inner: newCASStore()}
	engine := newEngine(t, store)
	defer engine.Close()

	const base = 5_000
	key := "orders/0/" + segmentNameForTest(base)
	if err := store.inner.Put(context.Background(), key, strings.NewReader("")); err != nil {
		t.Fatalf("seed segment: %v", err)
	}

	engine.metadataCache.SetPartitionState("orders", 0, base+100, 0, []*SegmentMetadata{
		{StartOffset: base, EndOffset: base + 100, S3Key: key},
	})

	// The engine lists on construction (recovery, reconciliation) and on its
	// own timers. Let that settle so the assertion below is about this read
	// and not about startup.
	waitFor(t, func() bool { return true })
	time.Sleep(500 * time.Millisecond)
	store.lists.Store(0)

	// The seeded object is empty, so the read itself may fail on framing. What
	// must not happen is a LIST: the manifest already names the segment.
	if _, _, err := engine.ReadBatchContext(context.Background(), "orders", 0, base, defaultReadBudget); err != nil {
		t.Logf("read returned %v (the seeded segment is empty)", err)
	}

	if got := store.lists.Load(); got != 0 {
		t.Fatalf("the read issued %d LISTs; it must resolve from the manifest", got)
	}
	if store.gets.Load() == 0 {
		t.Error("the read issued no GETs at all, so it never reached the segment")
	}
}

// segmentNameForTest mirrors the on-disk and in-object segment naming.
func segmentNameForTest(base int64) string {
	return fmt.Sprintf("%020d-e1.log", base)
}
