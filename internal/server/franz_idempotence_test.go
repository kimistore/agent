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

package server_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"kimistore/internal/storage"
)

// TestFranzGoIdempotentProduce is the compatibility check that matters for the
// idempotent-producer path: twmb/franz-go, the client Grafana Mimir is built
// on, enables idempotence by default, sends InitProducerId, and then produces
// batches carrying a producer id, epoch and sequence. A broker that advertises
// InitProducerId but mishandles those fields fails here.
func TestFranzGoIdempotentProduce(t *testing.T) {
	addr := freePort(t)
	cfg := testConfig(addr)
	b := startBroker(t, addr, newFSObjectStore(t), t.TempDir(), cfg)
	defer b.stop()

	topic := "franz-idempotent"
	if err := b.engine.CreateTopic(topic, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	const n = 20
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("v%03d", i))}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("idempotent produce: %v", err)
	}
	if got := b.engine.HighWaterMark(topic, 0); got != n {
		t.Errorf("high watermark = %d, want %d", got, n)
	}
}

// slowPutStore delays log-object writes past a producer's request timeout, so
// the client's ack times out and it retries the same batch. That is exactly
// the duplicate window D2 opens, and idempotence is what closes it: the retry
// must be recognised and answered with the original offset, not appended.
type slowPutStore struct {
	*fsObjectStore
	delay time.Duration
}

func (s *slowPutStore) Put(ctx context.Context, key string, r io.Reader) error {
	// Read the body first: the delay is meant to model a slow store, not a
	// consumed reader.
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if strings.HasSuffix(key, ".log") {
		select {
		case <-ctx.Done():
		case <-time.After(s.delay):
		}
	}
	return s.fsObjectStore.Put(ctx, key, strings.NewReader(string(data)))
}

// TestFranzGoIdempotentRetryDoesNotDuplicate forces the D2 timeout-and-retry
// path and checks the log does not grow twice. Without dedup this produces
// duplicate records; with it, the retry is handed back its original offset.
func TestFranzGoIdempotentRetryDoesNotDuplicate(t *testing.T) {
	addr := freePort(t)
	cfg := testConfig(addr)
	store := &slowPutStore{fsObjectStore: newFSObjectStore(t), delay: 400 * time.Millisecond}
	b := startBroker(t, addr, store, t.TempDir(), cfg)
	defer b.stop()

	topic := "franz-retry"
	if err := b.engine.CreateTopic(topic, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(addr),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		// A request timeout far shorter than the store delay, and a delivery
		// timeout long enough that the client retries rather than giving up.
		// franz-go refuses a produce timeout below 100ms.
		kgo.ProduceRequestTimeout(100*time.Millisecond),
		kgo.RecordDeliveryTimeout(20*time.Second),
		kgo.RequestRetries(20),
		kgo.MaxProduceRequestsInflightPerBroker(1),
	)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	const n = 3
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("v%03d", i))}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce after retries: %v", err)
	}

	// Exactly n records. If a retried batch had been appended again, the log
	// would be longer.
	if got := b.engine.HighWaterMark(topic, 0); got != n {
		t.Errorf("high watermark = %d, want %d: a retried batch was appended more than once", got, n)
	}
	// Prove the duplicate window was actually hit; otherwise this test would
	// pass even if the timeout never happened.
	if got := b.engine.ProducerDedups(); got < 1 {
		t.Errorf("no duplicate batch was deduplicated; the retry window was not exercised (dedups=%d)", got)
	}
}

// ensure storage stays imported if the tests above change shape.
var _ storage.ObjectStore = (*slowPutStore)(nil)
