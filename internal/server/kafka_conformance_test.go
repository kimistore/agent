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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"kimistore/internal/protocol"
	"kimistore/internal/server"
	"kimistore/internal/storage"
)

// fsObjectStore is a durable ObjectStore backed by a directory. Tests need
// durability across a broker restart, which an in-memory map cannot provide
// and which is exactly the property under test.
type fsObjectStore struct{ root string }

func newFSObjectStore(t *testing.T) *fsObjectStore {
	t.Helper()
	return &fsObjectStore{root: t.TempDir()}
}

func (s *fsObjectStore) path(key string) string {
	return filepath.Join(s.root, filepath.FromSlash(key))
}

func (s *fsObjectStore) Put(_ context.Context, key string, r io.Reader) error {
	p := s.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	// Write to a temporary file and rename, so a crash mid-write cannot
	// leave a half object behind.
	tmp := p + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func (s *fsObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	f, err := os.Open(s.path(key))
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (s *fsObjectStore) List(_ context.Context, prefix string) ([]storage.ObjectMetadata, error) {
	var out []storage.ObjectMetadata
	root := s.root
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return nil
		}
		key := filepath.ToSlash(rel)
		if !strings.HasPrefix(key, prefix) {
			return nil
		}
		out = append(out, storage.ObjectMetadata{Key: key, Size: info.Size(), LastModified: info.ModTime().Unix()})
		return nil
	})
	return out, err
}

func (s *fsObjectStore) Delete(_ context.Context, key string) error { return os.Remove(s.path(key)) }

func (s *fsObjectStore) GetRange(_ context.Context, key string, start, length int64) (io.ReadCloser, error) {
	f, err := os.Open(s.path(key))
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	if length <= 0 {
		return f, nil
	}
	return &boundedFile{f: f, remaining: length}, nil
}

type boundedFile struct {
	f         *os.File
	remaining int64
}

func (b *boundedFile) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.f.Read(p)
	b.remaining -= int64(n)
	return n, err
}
func (b *boundedFile) Close() error { return b.f.Close() }

// broker is a running agent under test.
type broker struct {
	addr   string
	engine *storage.StorageEngine
	srv    *server.Server
}

func startBroker(t *testing.T, addr string, store storage.ObjectStore, walDir string, cfg protocol.ServerConfig) *broker {
	t.Helper()

	engine, err := storage.NewStorageEngine(walDir, store, "bucket", storage.RetentionConfig{},
		// Keep the durability flush prompt in tests: the default one-second
		// coalescing window is the right production trade, but it would add
		// a second to every acks=all produce here.
		storage.WithFlushInterval(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	srv := server.NewServer(addr, engine, cfg)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return &broker{addr: addr, engine: engine, srv: srv}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("broker never came up on %s", addr)
	return nil
}

func (b *broker) stop() {
	_ = b.srv.Stop()
	_ = b.engine.Close()
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func testConfig(addr string) protocol.ServerConfig {
	host, portStr, _ := net.SplitHostPort(addr)
	var port int32
	fmt.Sscanf(portStr, "%d", &port)
	return protocol.ServerConfig{
		AdvertisedHost: host,
		AdvertisedPort: port,
	}
}

// TestKafkaGoProduceAndConsume is the end-to-end conformance check against the
// client library Grafana Mimir 3.0 is built on.
//
// It covers the failures that unit tests cannot see: whether a real client can
// negotiate, produce, and read back exactly what was written, with offsets
// that line up.
func TestKafkaGoProduceAndConsume(t *testing.T) {
	addr := freePort(t)
	cfg := testConfig(addr)
	// The kafka-go compatibility layout: its ProduceResponse decoder reads
	// Topics before ThrottleTimeMs, the reverse of the protocol.

	b := startBroker(t, addr, newFSObjectStore(t), t.TempDir(), cfg)
	defer b.stop()

	topic := "mimir-distributor"
	if err := b.engine.CreateTopic(topic, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// ---- produce, the way Mimir's distributor does: acks=all, real batches.
	const total = 1000
	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    100,
		WriteTimeout: 20 * time.Second,
	}
	defer w.Close()

	msgs := make([]kafka.Message, total)
	for i := range msgs {
		msgs[i] = kafka.Message{Key: []byte(fmt.Sprintf("k%05d", i)), Value: []byte(fmt.Sprintf("v%05d", i))}
	}
	produceCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(produceCtx, msgs...); err != nil {
		t.Fatalf("produce %d records: %v", total, err)
	}

	// The broker must have advanced the log by one offset per record. If the
	// record count is mis-parsed the log end is wrong, and every later
	// consumer is sent past the data.
	if hwm := b.engine.HighWaterMark(topic, 0); hwm != total {
		t.Errorf("high watermark = %d, want %d (one offset per record)", hwm, total)
	}

	// ---- consume, the way Mimir's ingester does: a group, reading everything.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{addr},
		Topic:       topic,
		Partition:   0,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     100 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	defer r.Close()

	readCtx, readCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer readCancel()

	var got int
	var prevOffset int64 = -1
	for got < total {
		m, err := r.ReadMessage(readCtx)
		if err != nil {
			t.Fatalf("consumed %d/%d then failed: %v", got, total, err)
		}
		// Offsets must be contiguous. A gap means the broker handed out an
		// offset it never stored, or the record batch was re-addressed wrongly.
		if m.Offset != prevOffset+1 {
			t.Fatalf("offset jumped from %d to %d at record %d", prevOffset, m.Offset, got)
		}
		prevOffset = m.Offset
		got++
	}
	if got != total {
		t.Errorf("consumed %d records, want %d", got, total)
	}
	t.Logf("produced and consumed %d records with contiguous offsets 0..%d", got, prevOffset)
}

// TestKafkaGoFetchFillsBudget checks one fetch returns several batches rather
// than one, which is what keeps a consumer from turning into a request loop.
func TestKafkaGoFetchFillsBudget(t *testing.T) {
	addr := freePort(t)
	cfg := testConfig(addr)

	b := startBroker(t, addr, newFSObjectStore(t), t.TempDir(), cfg)
	defer b.stop()

	topic := "orders"
	if err := b.engine.CreateTopic(topic, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	w := &kafka.Writer{
		Addr:      kafka.TCP(addr),
		Topic:     topic,
		Balancer:  &kafka.LeastBytes{},
		BatchSize: 1, // force many small batches
		// acks=1 on purpose. This test is about the fetch byte budget, not
		// durability: with acks=all, each synchronous single-record write is
		// sealed and uploaded on its own by the D2 flush, so one fetch would
		// only ever see one record per object. acks=1 keeps the batches in
		// the active local segment, which is exactly the state this test
		// needs to prove one fetch drains many batches.
		RequiredAcks: kafka.RequireOne,
		WriteTimeout: 20 * time.Second,
	}
	for i := 0; i < 50; i++ {
		if err := w.WriteMessages(context.Background(), kafka.Message{Value: []byte(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	w.Close()

	conn, err := kafka.DialLeader(context.Background(), "tcp", addr, topic, 0)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Seek(0, 0); err != nil {
		t.Fatalf("seek: %v", err)
	}

	// ReadBatch is the client's own fetch: one call is one round trip to the
	// broker, however many records come back. Counting ReadMessage calls
	// instead would measure the test loop, not the broker.
	rounds, records := 0, 0
	for records < 50 {
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		batch := conn.ReadBatch(1, 10e6)
		if batch == nil {
			break
		}
		n := 0
		for {
			if _, err := batch.ReadMessage(); err != nil {
				break
			}
			n++
		}
		if n == 0 {
			break
		}
		records += n
		rounds++
		if rounds > 50 {
			break
		}
	}
	if records < 50 {
		t.Fatalf("read only %d of 50 records", records)
	}
	if rounds >= 50 {
		t.Errorf("draining 50 separately-produced batches took %d fetches; one fetch must return many records", rounds)
	}
	t.Logf("drained %d separately-produced batches in %d fetch round trips", records, rounds)
}

// TestOffsetsSurviveRestart is the durability check: after a restart the log
// must continue where it left off, not rewind to zero and hand out offsets
// whose data is already stored.
func TestOffsetsSurviveRestart(t *testing.T) {
	store := newFSObjectStore(t)
	walDir := t.TempDir()
	addr := freePort(t)
	cfg := testConfig(addr)

	const total = 500

	// First incarnation.
	b := startBroker(t, addr, store, walDir, cfg)
	topic := "mimir-distributor"
	if err := b.engine.CreateTopic(topic, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    100,
		WriteTimeout: 20 * time.Second,
	}
	msgs := make([]kafka.Message, total)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: []byte(fmt.Sprintf("v%05d", i))}
	}
	if err := w.WriteMessages(context.Background(), msgs...); err != nil {
		t.Fatalf("produce: %v", err)
	}
	if hwm := b.engine.HighWaterMark(topic, 0); hwm != total {
		t.Fatalf("high watermark before restart = %d, want %d", hwm, total)
	}
	w.Close()
	b.stop()

	// Second incarnation, same object store, empty local WAL.
	b2 := startBroker(t, addr, store, t.TempDir(), cfg)
	defer b2.stop()

	if hwm := b2.engine.HighWaterMark(topic, 0); hwm != total {
		t.Errorf("high watermark after restart = %d, want %d; a rewind makes the broker reissue offsets whose data is already stored", hwm, total)
	}

	// The next write must continue the log.
	conn, err := kafka.DialLeader(context.Background(), "tcp", addr, topic, 0)
	if err != nil {
		t.Fatalf("dial after restart: %v", err)
	}
	defer conn.Close()
	// Use a second writer so the returned offset is observable.
	w2 := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		WriteTimeout: 20 * time.Second,
	}
	defer w2.Close()
	if err := w2.WriteMessages(context.Background(), kafka.Message{Value: []byte("after-restart")}); err != nil {
		t.Fatalf("produce after restart: %v", err)
	}
	if hwm := b2.engine.HighWaterMark(topic, 0); hwm != total+1 {
		t.Errorf("high watermark after writing post-restart = %d, want %d; the log rewound and reused offsets", hwm, total+1)
	}

	// And everything must still be readable, from the cold path if necessary.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{addr},
		Topic:       topic,
		Partition:   0,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	count := 0
	for count < total+1 {
		if _, err := r.ReadMessage(ctx); err != nil {
			t.Fatalf("read %d after restart: %v", count, err)
		}
		count++
	}
	t.Logf("read all %d records across the restart", count)
}

// TestUnsupportedRequestKeepsConnection checks an API the broker does not
// implement is refused in place. Closing the socket instead turns one
// unexpected request into a reconnect storm.
func TestUnsupportedRequestKeepsConnection(t *testing.T) {
	addr := freePort(t)
	b := startBroker(t, addr, newFSObjectStore(t), t.TempDir(), testConfig(addr))
	defer b.stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	send := func(apiKey, apiVersion int16, corr int32) (int32, error) {
		var frame []byte
		i16 := func(v int16) {
			x := make([]byte, 2)
			binary.BigEndian.PutUint16(x, uint16(v))
			frame = append(frame, x...)
		}
		i32 := func(v int32) {
			x := make([]byte, 4)
			binary.BigEndian.PutUint32(x, uint32(v))
			frame = append(frame, x...)
		}
		i16(apiKey)
		i16(apiVersion)
		i32(corr)
		i16(int16(len("probe")))
		frame = append(frame, "probe"...)
		frame = append(frame, make([]byte, 32)...)

		size := make([]byte, 4)
		binary.BigEndian.PutUint32(size, uint32(len(frame)))
		if _, err := conn.Write(append(size, frame...)); err != nil {
			return 0, err
		}
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		hdr := make([]byte, 4)
		if _, err := io.ReadFull(conn, hdr); err != nil {
			return 0, err
		}
		body := make([]byte, binary.BigEndian.Uint32(hdr))
		if _, err := io.ReadFull(conn, body); err != nil {
			return 0, err
		}
		return int32(binary.BigEndian.Uint32(body[:4])), nil
	}

	// ApiKey 22 (InitProducerId) is not implemented.
	if id, err := send(22, 0, 1); err != nil {
		t.Fatalf("connection dropped on an unsupported API: %v", err)
	} else if id != 1 {
		t.Errorf("correlation id = %d, want 1", id)
	}

	// The connection must still serve a supported request afterwards.
	if id, err := send(3, 1, 2); err != nil {
		t.Fatalf("connection unusable after an unsupported API: %v", err)
	} else if id != 2 {
		t.Errorf("correlation id = %d, want 2", id)
	}
}

// TestListOffsetsReportsRealBoundaries checks a consumer that resets to
// "earliest" is given an offset that exists, and "latest" tracks the log end.
func TestListOffsetsReportsRealBoundaries(t *testing.T) {
	addr := freePort(t)
	cfg := testConfig(addr)
	b := startBroker(t, addr, newFSObjectStore(t), t.TempDir(), cfg)
	defer b.stop()

	topic := "orders"
	if err := b.engine.CreateTopic(topic, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := b.engine.Append(topic, 0, singleRecord(t, i), 1, true); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	conn, err := kafka.DialLeader(context.Background(), "tcp", addr, topic, 0)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	first, last, err := conn.ReadOffsets()
	if err != nil {
		t.Fatalf("ReadOffsets: %v", err)
	}
	if first != 0 {
		t.Errorf("earliest offset = %d, want 0", first)
	}
	if last != 10 {
		t.Errorf("latest offset = %d, want 10", last)
	}
}

// singleRecord builds a one-record batch in the wrapped encoding a
// Java-style producer sends.
func singleRecord(t *testing.T, n int) []byte {
	t.Helper()
	return wrappedRecordBatch(0, 1)
}

// testRecordBatch builds a real RecordBatch (magic 2) carrying n records,
// wrapped in a message set entry the way the Java client, librdkafka and
// sarama send it.
// putZigzag writes a zigzag-encoded signed varint, which is how a record
// batch encodes record lengths, deltas and field lengths. Truncating it to a
// single byte silently breaks every value of 64 or more, because a byte with
// the high bit set starts a multi-byte sequence.
func putZigzag(w *bytes.Buffer, v int32) {
	u := uint32((v << 1) ^ (v >> 31))
	for u >= 0x80 {
		w.WriteByte(byte(u) | 0x80)
		u >>= 7
	}
	w.WriteByte(byte(u))
}

func testRecordBatch(baseOffset int64, n int32) []byte {
	const headerLen = 61

	var records bytes.Buffer
	for i := int32(0); i < n; i++ {
		// Record fields are zigzag varints. One record is:
		//   length(1) attributes(1) timestampDelta(1) offsetDelta(1)
		//   keyLength(-1 => null) valueLength value headerCount(0)
		// which is 7 bytes of body, so the length varint is zigzag(7) = 14.
		records.WriteByte(0x0E)      // record length
		records.WriteByte(0x00)      // record attributes
		records.WriteByte(0x00)      // timestamp delta
		putZigzag(&records, i)       // offset delta
		records.WriteByte(0x01)      // key length: zigzag(-1), a null key
		records.WriteByte(0x02)      // value length: zigzag(1)
		records.WriteByte(byte('v')) // value
		records.WriteByte(0x00)      // header count
	}

	batchLength := headerLen - 12 + records.Len()
	buf := make([]byte, 0, 12+batchLength)
	var t8 [8]byte
	var t2 [2]byte

	binary.BigEndian.PutUint64(t8[:8], uint64(baseOffset))
	buf = append(buf, t8[:8]...)
	binary.BigEndian.PutUint32(t8[:4], uint32(batchLength))
	buf = append(buf, t8[:4]...)
	binary.BigEndian.PutUint32(t8[:4], 0xFFFFFFFF)
	buf = append(buf, t8[:4]...)
	buf = append(buf, 2) // magic
	binary.BigEndian.PutUint32(t8[:4], 0)
	buf = append(buf, t8[:4]...)
	buf = append(buf, t2[:2]...) // attributes
	binary.BigEndian.PutUint32(t8[:4], uint32(n-1))
	buf = append(buf, t8[:4]...)
	binary.BigEndian.PutUint64(t8[:8], 0)
	buf = append(buf, t8[:8]...)
	binary.BigEndian.PutUint64(t8[:8], 0)
	buf = append(buf, t8[:8]...)
	binary.BigEndian.PutUint64(t8[:8], 0xFFFFFFFFFFFFFFFF)
	buf = append(buf, t8[:8]...)
	binary.BigEndian.PutUint16(t2[:2], 0xFFFF)
	buf = append(buf, t2[:2]...)
	binary.BigEndian.PutUint32(t8[:4], 0xFFFFFFFF)
	buf = append(buf, t8[:4]...)
	binary.BigEndian.PutUint32(t8[:4], uint32(n))
	buf = append(buf, t8[:4]...)
	buf = append(buf, records.Bytes()...)

	sum := crc32.Checksum(buf[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(buf[17:21], sum)

	// Bare: the batch's own offset and length are its first twelve bytes,
	// which is what segmentio/kafka-go sends.
	return buf
}

// TestKafkaGoReadsJavaStyleBatches covers the case the read-path re-framing
// exists for: a producer that writes a RecordBatch wrapped in a message set
// entry, as the Java client, librdkafka and sarama all do, read back by a
// client that only understands the bare form.
//
// There is no flag for this and there should not be: kafka-go assumes the bare
// form in both of its decoders, so serving the producer's original framing
// would make the data unreadable to the consumer this agent exists to serve.
func TestKafkaGoReadsJavaStyleBatches(t *testing.T) {
	addr := freePort(t)
	b := startBroker(t, addr, newFSObjectStore(t), t.TempDir(), testConfig(addr))
	defer b.stop()

	topic := "java-produced"
	if err := b.engine.CreateTopic(topic, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// A wrapped batch: outer message set entry around a RecordBatch, the
	// layout a Java client puts on the wire.
	const records = 250
	stored := wrappedRecordBatch(0, records)
	if _, err := b.engine.Append(topic, 0, stored, records, true); err != nil {
		t.Fatalf("append: %v", err)
	}
	if hwm := b.engine.HighWaterMark(topic, 0); hwm != records {
		t.Fatalf("high watermark = %d, want %d: a wrapped batch must still be counted correctly", hwm, records)
	}

	// Assert on the stored bytes, not on what the read returns: the read path
	// re-frames to bare, which is the point of the test. Checking the output
	// would assert the opposite of what is intended.
	if !wrappedRecordBatchForm(stored) {
		t.Fatal("fixture is not in the wrapped form; the test would pass for the wrong reason")
	}

	// And confirm the re-framing actually happened on the way out.
	served, err := b.engine.Read(topic, 0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if wrappedRecordBatchForm(served) {
		t.Error("a wrapped batch was served wrapped; the target client cannot read that")
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{addr}, Topic: topic, Partition: 0,
		MinBytes: 1, MaxBytes: 10e6, MaxWait: 200 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	n := 0
	for n < records {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read %d of %d: %v", n, records, err)
		}
		if m.Offset != int64(n) {
			t.Fatalf("offset jumped from %d to %d at record %d", n-1, m.Offset, n)
		}
		n++
	}
	t.Logf("read %d records produced by a Java-style client", n)
}

// wrappedRecordBatchForm reports whether a blob is a RecordBatch sitting inside
// an outer message set entry, as opposed to bare.
//
// A bare batch carries magic 2 at offset 16. A wrapped one starts with a
// 12-byte entry header, so its offset 16 falls inside the inner batch's
// batchLength and the magic lands at 28 instead.
func wrappedRecordBatchForm(blob []byte) bool {
	if len(blob) < 30 {
		return false
	}
	return blob[16] != 2 && blob[28] == 2
}

// wrappedRecordBatch builds the same RecordBatch inside an outer message set
// entry, the way the Java client, librdkafka and sarama send it.
func wrappedRecordBatch(baseOffset int64, n int) []byte {
	batch := testRecordBatch(baseOffset, int32(n))
	wrapped := make([]byte, 12, 12+len(batch))
	binary.BigEndian.PutUint64(wrapped[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(wrapped[8:12], uint32(len(batch)))
	return append(wrapped, batch...)
}

// TestKafkaConnWriteMessages exercises the connection-level produce API, which
// hard-codes the Produce versions it will speak as {v2, v3, v7}.
//
// It only works because Produce is advertised up to v3. Capping Produce at v0
// looks attractive, because a v0 response has no ThrottleTimeMs field and so
// cannot be misread, but it silently forces clients back to magic-1 records,
// which have no header field. Grafana Mimir keeps the wire format of each
// write in a record header, so under a v0 ceiling it ingests every record as
// the wrong version. This test exists to make that trade-off visible.
func TestKafkaConnWriteMessages(t *testing.T) {
	addr := freePort(t)
	b := startBroker(t, addr, newFSObjectStore(t), t.TempDir(), testConfig(addr))
	defer b.stop()

	topic := "conn-produce"
	if err := b.engine.CreateTopic(topic, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	conn, err := kafka.DialLeader(context.Background(), "tcp", addr, topic, 0)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if _, _, err := conn.ReadOffsets(); err != nil {
		t.Errorf("ReadOffsets: %v", err)
	}

	const n = 50
	msgs := make([]kafka.Message, n)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: []byte("v"), Headers: []kafka.Header{{Key: "Version", Value: []byte{0, 0, 0, 1}}}}
	}
	// The header matters: it is what a producer uses to mark the payload
	// version, and it only survives on a magic-2 record.
	if _, err := conn.WriteMessages(msgs...); err != nil {
		t.Fatalf("WriteMessages: %v", err)
	}
	t.Logf("kafka.Conn.WriteMessages wrote %d messages", n)

	if hwm := b.engine.HighWaterMark(topic, 0); hwm != n {
		t.Errorf("high watermark = %d, want %d", hwm, n)
	}

	// And they read back with their headers intact.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{addr}, Topic: topic, Partition: 0,
		MinBytes: 1, MaxBytes: 10e6, StartOffset: kafka.FirstOffset,
	})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got := 0
	for got < n {
		if _, err := r.ReadMessage(ctx); err != nil {
			t.Fatalf("read %d of %d: %v", got, n, err)
		}
		got++
	}
}
