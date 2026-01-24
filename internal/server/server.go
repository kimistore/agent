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
	"sync"

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
}

func NewServer(addr string, storage *storage.StorageEngine, saslUser, saslPassword string) *Server {
	return &Server{
		addr:    addr,
		quit:    make(chan struct{}),
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
	s.listener = ln

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

func (s *Server) Stop() error {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	metrics.IncConnection()
	defer metrics.DecConnection()

	// log.Printf("New connection from %s", conn.RemoteAddr())

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
