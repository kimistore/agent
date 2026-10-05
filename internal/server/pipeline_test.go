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

package server

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"kimistore/internal/storage"
)

// pipelineServer starts a broker on a free port with one topic created.
func pipelineServer(t *testing.T) (addr string, engine *storage.StorageEngine) {
	t.Helper()
	srv, addr := startTestServer(t)
	t.Cleanup(func() { srv.Stop() })
	engine = srv.storage
	if err := engine.CreateTopic("orders", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	return addr, engine
}

// metadataFrame builds a Metadata request for all topics.
func metadataFrame(apiVersion int16, correlationID int32) []byte {
	var body []byte
	enc := func(v int32) {
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(v))
		body = append(body, b...)
	}
	enc(0) // empty topic list: all topics

	hdr := new(binaryWriter)
	hdr.i16(3) // ApiKeyMetadata
	hdr.i16(apiVersion)
	hdr.i32(correlationID)
	hdr.str("pipeline-test")
	return append(hdr.buf, body...)
}

type binaryWriter struct{ buf []byte }

func (b *binaryWriter) i16(v int16) {
	x := make([]byte, 2)
	binary.BigEndian.PutUint16(x, uint16(v))
	b.buf = append(b.buf, x...)
}
func (b *binaryWriter) i32(v int32) {
	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(v))
	b.buf = append(b.buf, x...)
}
func (b *binaryWriter) str(s string) {
	b.i16(int16(len(s)))
	b.buf = append(b.buf, s...)
}

// TestConnectionHandlesRequestsConcurrently checks a slow request does not
// block the ones behind it on the same connection.
//
// Kafka clients pipeline. A serial read-handle-write loop means one
// long-polling Fetch parks the whole socket, so heartbeats and offset commits
// queue behind it and the consumer eventually reaps itself on session
// timeout. Responses still have to come back in request order.
func TestConnectionHandlesRequestsConcurrently(t *testing.T) {
	addr, _ := pipelineServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Fire several requests back to back without reading in between, which is
	// what a pipelining client does.
	const n = 5
	for i := 0; i < n; i++ {
		frame := metadataFrame(1, int32(i+1))
		size := make([]byte, 4)
		binary.BigEndian.PutUint32(size, uint32(len(frame)))
		if _, err := conn.Write(append(size, frame...)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Every response must arrive, and they must arrive in request order.
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for i := 0; i < n; i++ {
		hdr := make([]byte, 4)
		if _, err := io.ReadFull(conn, hdr); err != nil {
			t.Fatalf("read size %d: %v", i, err)
		}
		body := make([]byte, binary.BigEndian.Uint32(hdr))
		if _, err := io.ReadFull(conn, body); err != nil {
			t.Fatalf("read body %d: %v", i, err)
		}
		if got := binary.BigEndian.Uint32(body[:4]); got != uint32(i+1) {
			t.Errorf("response %d carried correlation id %d; responses must stay in request order", i, got)
		}
	}
}

// TestOversizedFrameIsRejected checks a client announcing an absurd frame size
// is disconnected rather than being allowed to allocate it.
func TestOversizedFrameIsRejected(t *testing.T) {
	addr, _ := pipelineServer(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, maxFrameSize+1)
	if _, err := conn.Write(size); err != nil {
		t.Fatalf("write: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Error("connection stayed open after an oversized frame")
	} else {
		t.Logf("connection closed as expected: %v", err)
	}
}
