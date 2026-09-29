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
	"log"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"kimistore/internal/metrics"
	"kimistore/internal/protocol"
	"kimistore/internal/storage"
)

type Server struct {
	addr       string
	listener   net.Listener
	quit       chan struct{}
	wg         sync.WaitGroup
	storage    *storage.StorageEngine
	authConfig protocol.AuthConfig

	// stateMu guards the server lifecycle: the listener, the set of live
	// connections, and the closing flag. Connections are tracked so Stop can
	// actively tear them down: a handler parked in a blocking read never
	// observes s.quit on its own, so waiting for it to notice would hang
	// shutdown for as long as any client stays connected.
	stateMu sync.Mutex
	conns   map[net.Conn]struct{}
	closing bool
}

func NewServer(addr string, storage *storage.StorageEngine, saslUser, saslPassword string) *Server {
	return &Server{
		addr:    addr,
		quit:    make(chan struct{}),
		conns:   make(map[net.Conn]struct{}),
		storage: storage,
		authConfig: protocol.AuthConfig{
			Username: saslUser,
			Password: saslPassword,
		},
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	// Publish the listener under the lock: Stop may run concurrently and must
	// not observe a half-published or stale listener.
	s.stateMu.Lock()
	s.listener = ln
	s.stateMu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return nil
			default:
				log.Printf("Accept error: %v", err)
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
		ln.Close()
	}

	// Tear down every live connection, unblocking handlers parked in a read.
	// Taking the lock here also closes the race with the accept path.
	s.stateMu.Lock()
	live := len(s.conns)
	for c := range s.conns {
		c.Close()
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

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

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

	session := &protocol.Session{
		Authenticated: false,
	}

	for {
		// 1. Read Message Size (int32)
		headerBuf := make([]byte, 4)
		if _, err := io.ReadFull(conn, headerBuf); err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			return
		}
		metrics.AddIOBytes("inbound", 4)

		size := binary.BigEndian.Uint32(headerBuf)

		// 2. Read Message Body
		bodyBuf := make([]byte, size)
		if _, err := io.ReadFull(conn, bodyBuf); err != nil {
			log.Printf("Read body error: %v", err)
			return
		}
		metrics.AddIOBytes("inbound", int(size))

		// 3. Process Request
		resp, err := protocol.HandleRequest(bodyBuf, s.storage, session, s.authConfig)
		if err != nil {
			log.Printf("Protocol error: %v", err)
			return // Or close connection on protocol error
		}

		if resp == nil {
			// No response needed (e.g. Acks=0)
			continue
		}

		// log.Printf("Sending Response: Size=%d", len(resp))
		// log.Printf("Response Hex: %x", resp)

		// 4. Send Response
		// Response format: Size (int32) | Body
		respSize := make([]byte, 4)
		binary.BigEndian.PutUint32(respSize, uint32(len(resp)))

		if _, err := conn.Write(respSize); err != nil {
			log.Printf("Write error: %v", err)
			return
		}
		metrics.AddIOBytes("outbound", 4)
		if _, err := conn.Write(resp); err != nil {
			log.Printf("Write body error: %v", err)
			return
		}
		metrics.AddIOBytes("outbound", len(resp))
	}
}
