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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"kimistore/internal/metrics"
	"kimistore/internal/protocol"
	"kimistore/internal/storage"
)

// maxFrameSize bounds a single inbound request. A client announcing more than
// this is either broken or hostile, and allocating whatever it asked for
// would be an easy way to lose the process.
const maxFrameSize = 100 * 1024 * 1024

// maxInFlightRequests bounds how many requests one connection may have
// outstanding. Kafka clients pipeline aggressively; without a ceiling a slow
// handler (a long-polling Fetch) would let a client queue unbounded work.
const maxInFlightRequests = 256

type Server struct {
	addr     string
	listener net.Listener
	quit     chan struct{}
	wg       sync.WaitGroup
	storage  *storage.StorageEngine
	config   protocol.ServerConfig

	// stateMu guards the server lifecycle: the listener, the set of live
	// connections, and the closing flag. Connections are tracked so Stop can
	// actively tear them down: a handler parked in a blocking read never
	// observes s.quit on its own, so waiting for it to notice would hang
	// shutdown for as long as any client stays connected.
	stateMu sync.Mutex
	conns   map[net.Conn]struct{}
	closing bool
}

func NewServer(addr string, storage *storage.StorageEngine, config protocol.ServerConfig) *Server {
	if config.AdvertisedHost == "" {
		config.AdvertisedHost = "localhost"
	}
	if config.AdvertisedPort == 0 {
		config.AdvertisedPort = 19092
	}
	return &Server{
		addr:    addr,
		quit:    make(chan struct{}),
		conns:   make(map[net.Conn]struct{}),
		storage: storage,
		config:  config,
	}
}

func (s *Server) Start() error {
	// ListenConfig rather than net.Listen: a listener that cannot be closed
	// promptly is a listener that cannot be shut down promptly, and shutdown
	// depends on closing this one.
	lc := net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", s.addr)
	if err != nil {
		return err
	}

	// Publish the listener under the lock: Stop may run concurrently and must
	// not observe a half-published or stale listener.
	s.stateMu.Lock()
	s.listener = ln
	s.stateMu.Unlock()

	log.Printf("Broker advertised as %s:%d", s.config.AdvertisedHost, s.config.AdvertisedPort)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
				log.Printf("Accept error: %v", err)
				// A transient accept error must not spin the CPU.
				time.Sleep(5 * time.Millisecond)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// shutdownGrace bounds how long Stop waits for in-flight connection
// handlers to finish after their sockets have been closed.
const shutdownGrace = 5 * time.Second

func (s *Server) Stop() error {
	close(s.quit)

	// Mark the server closing before releasing the lock, so the accept path
	// can no longer register a new handler, and capture the listener to close.
	s.stateMu.Lock()
	s.closing = true
	ln := s.listener
	s.stateMu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}

	// Tear down every live connection, unblocking handlers parked in a read.
	// Taking the lock here also closes the race with the accept path.
	s.stateMu.Lock()
	live := len(s.conns)
	for c := range s.conns {
		_ = c.Close()
	}
	s.stateMu.Unlock()

	// Wait for handlers to unwind, but never indefinitely: a wedged handler
	// must not prevent the process from exiting.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(shutdownGrace):
		log.Printf("Shutdown: %d connection handler(s) still running after %s; exiting anyway", live, shutdownGrace)
	}
	return nil
}

// result carries one handled request back to the writer.
type result struct {
	body []byte
	err  error
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer func() { _ = conn.Close() }()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in handleConnection: %v\n%s", r, debug.Stack())
		}
	}()

	// Register so Stop can tear this connection down. If the server is
	// already closing, refuse rather than joining the wait group after the
	// shutdown sweep has already run.
	s.stateMu.Lock()
	if s.closing {
		s.stateMu.Unlock()
		return
	}
	s.conns[conn] = struct{}{}
	s.stateMu.Unlock()
	defer func() {
		s.stateMu.Lock()
		delete(s.conns, conn)
		s.stateMu.Unlock()
	}()

	metrics.IncConnection()
	defer metrics.DecConnection()

	remoteAddr := conn.RemoteAddr().String()
	log.Printf("New connection from %s", remoteAddr)
	defer log.Printf("Connection closed from %s", remoteAddr)

	session := &protocol.Session{Authenticated: false}

	// The connection's context, cancelled the moment the socket goes away.
	// Everything below inherits it, so a long-poll or a cold read started for
	// a client that has disconnected stops immediately instead of holding a
	// handler goroutine and an in-flight slot until it finishes on its own.
	connCtx, cancelConn := context.WithCancel(context.Background())
	defer cancelConn()

	// Kafka clients pipeline: several requests may be in flight on one socket
	// at a time, and responses may come back out of order by correlation ID.
	// Handling each request on its own goroutine while a dedicated writer
	// emits responses in arrival order keeps that pattern working.
	//
	// The previous serial loop meant a single long-polling Fetch parked the
	// whole connection, so heartbeats and offset commits queued behind it --
	// which shows up as spurious session timeouts and rebalance storms for
	// exactly the multiplexed clients that need a broker most.
	pending := make(chan chan result, maxInFlightRequests)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for ch := range pending {
			res := <-ch
			if res.err != nil {
				log.Printf("Protocol error: %v", res.err)
				return
			}
			if res.body == nil {
				// No response expected (e.g. acks=0).
				continue
			}
			if err := writeFrame(conn, res.body); err != nil {
				log.Printf("Write error: %v", err)
				return
			}
		}
	}()

	var handlers sync.WaitGroup
	for {
		body, err := readFrame(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			break
		}

		ch := make(chan result, 1)
		pending <- ch

		handlers.Add(1)
		go func(body []byte, ch chan<- result) {
			defer handlers.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Panic handling request: %v\n%s", r, debug.Stack())
					ch <- result{err: fmt.Errorf("handler panic: %v", r)}
				}
			}()
			resp, herr := protocol.HandleRequest(connCtx, body, s.storage, session, s.config)
			ch <- result{body: resp, err: herr}
		}(body, ch)
	}

	// Let outstanding handlers finish so their responses are not dropped
	// mid-flight, then close the writer.
	handlers.Wait()
	close(pending)
	<-writerDone
}

func readFrame(conn net.Conn) ([]byte, error) {
	headerBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, headerBuf); err != nil {
		return nil, err
	}
	metrics.AddIOBytes("inbound", 4)

	size := binary.BigEndian.Uint32(headerBuf)
	if size == 0 {
		return nil, io.EOF
	}
	if size > maxFrameSize {
		return nil, fmt.Errorf("request frame of %d bytes exceeds the %d byte limit", size, maxFrameSize)
	}

	body := make([]byte, size)
	if _, err := io.ReadFull(conn, body); err != nil {
		return nil, err
	}
	metrics.AddIOBytes("inbound", int(size))
	return body, nil
}

func writeFrame(conn net.Conn, body []byte) error {
	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(body)))
	if _, err := conn.Write(size); err != nil {
		return err
	}
	metrics.AddIOBytes("outbound", 4)
	if _, err := conn.Write(body); err != nil {
		return err
	}
	metrics.AddIOBytes("outbound", len(body))
	return nil
}
