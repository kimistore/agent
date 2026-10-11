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
	"testing"

	"kimistore/internal/storage"
)

// decodeOffsetFetchTopics reads a V1 OffsetFetch response: an array of topics,
// each with an array of (partition, offset, metadata, error).
func decodeOffsetFetchTopics(t *testing.T, body []byte) map[string]map[int32]int64 {
	t.Helper()
	dec := NewDecoder(body)

	n, err := dec.Int32()
	if err != nil {
		t.Fatalf("decoding topic count: %v", err)
	}
	out := make(map[string]map[int32]int64, n)
	for i := int32(0); i < n; i++ {
		topic, err := dec.String()
		if err != nil {
			t.Fatalf("decoding topic %d: %v", i, err)
		}
		pc, err := dec.Int32()
		if err != nil {
			t.Fatalf("decoding partition count for %s: %v", topic, err)
		}
		parts := make(map[int32]int64, pc)
		for j := int32(0); j < pc; j++ {
			p, err := dec.Int32()
			if err != nil {
				t.Fatalf("decoding partition for %s: %v", topic, err)
			}
			off, err := dec.Int64()
			if err != nil {
				t.Fatalf("decoding offset for %s/%d: %v", topic, p, err)
			}
			if _, err := dec.String(); err != nil { // metadata
				t.Fatalf("decoding metadata for %s/%d: %v", topic, p, err)
			}
			if _, err := dec.Int16(); err != nil { // error code
				t.Fatalf("decoding error code for %s/%d: %v", topic, p, err)
			}
			parts[p] = off
		}
		out[topic] = parts
	}
	return out
}

// An OffsetFetch naming no topics asks for the whole group position. Mimir's
// ingester asks exactly this way, and an empty answer made it fall back to
// auto.offset.reset and replay from the beginning, ignoring a committed offset
// that was durably stored the whole time.
func TestOffsetFetch_EmptyTopicListReturnsGroupOffsets(t *testing.T) {
	store := newOffsetStore(t)

	enc := NewEncoder()
	enc.String("mimir-trial-ingesters")
	enc.Int32(0) // no topics named

	body, err := handleOffsetFetch(NewDecoder(enc.Bytes()), NewEncoder(), store, 1)
	if err != nil {
		t.Fatalf("handleOffsetFetch: %v", err)
	}

	got := decodeOffsetFetchTopics(t, body)
	parts, ok := got["mimir-distributor"]
	if !ok {
		t.Fatalf("no mimir-distributor in the response; got %v", got)
	}
	if parts[0] != 618775 {
		t.Errorf("partition 0 offset = %d, want the committed 618775", parts[0])
	}
}

// A group with nothing committed answers with no topics, which is the
// protocol's way of saying "no position", and leaves the consumer to apply its
// own reset policy. Inventing offsets here would silently skip data.
func TestOffsetFetch_EmptyTopicListUnknownGroup(t *testing.T) {
	store := newOffsetStore(t)

	enc := NewEncoder()
	enc.String("never-seen")
	enc.Int32(0)

	body, err := handleOffsetFetch(NewDecoder(enc.Bytes()), NewEncoder(), store, 1)
	if err != nil {
		t.Fatalf("handleOffsetFetch: %v", err)
	}
	if got := decodeOffsetFetchTopics(t, body); len(got) != 0 {
		t.Errorf("unknown group returned %v, want no topics", got)
	}
}

// Naming topics explicitly must reach object storage, exactly as it did before.
// This store holds nothing, so the answer stays -1: the empty-list branch must
// not have rerouted the named path through the in-memory cache, which would
// report a position the broker has not actually committed.
func TestOffsetFetch_NamedTopicsStillReadThrough(t *testing.T) {
	store := newOffsetStore(t)

	enc := NewEncoder()
	enc.String("mimir-trial-ingesters")
	enc.Int32(1)
	enc.String("mimir-distributor")
	enc.Int32(1)
	enc.Int32(0)

	body, err := handleOffsetFetch(NewDecoder(enc.Bytes()), NewEncoder(), store, 1)
	if err != nil {
		t.Fatalf("handleOffsetFetch: %v", err)
	}
	got := decodeOffsetFetchTopics(t, body)
	if off := got["mimir-distributor"][0]; off != -1 {
		t.Errorf("named-topic fetch returned %d; it must still read through to storage", off)
	}
}

// newOffsetStore is an engine that already has a committed offset, the way one
// would after a restart recovered it from the checkpoint.
func newOffsetStore(t *testing.T) *storage.StorageEngine {
	t.Helper()
	se, err := storage.NewStorageEngine(t.TempDir(), &MockObjectStore{}, "test-bucket", storage.RetentionConfig{})
	if err != nil {
		t.Fatalf("NewStorageEngine: %v", err)
	}
	t.Cleanup(func() { _ = se.Close() })
	if err := se.SaveOffset("mimir-trial-ingesters", "mimir-distributor", 0, 618775); err != nil {
		t.Fatalf("SaveOffset: %v", err)
	}
	return se
}
