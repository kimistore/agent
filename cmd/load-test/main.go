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

// Command load-test pushes a fixed volume of data at a broker to measure
// produce throughput.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := os.Getenv("TOPIC")
	if topic == "" {
		topic = "load-test"
	}
	addr := os.Getenv("BROKER")
	if addr == "" {
		addr = "localhost:19092"
	}
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = 'A'
	}
	targetMB, _ := strconv.Atoi(os.Getenv("TARGET_MB"))
	if targetMB == 0 {
		targetMB = 1024
	}
	targetSize := int64(targetMB) * 1024 * 1024

	// kafka.Writer, not kafka.Conn.WriteMessages.
	//
	// The connection-level API only speaks Produce v2 and above, and this
	// broker deliberately offers Produce v0 alone: v0 is the one version whose
	// response layout every client reads the same way. The writer negotiates
	// across the whole range, so it works either way.
	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    100,
		WriteTimeout: 30 * time.Second,
	}
	defer func() { _ = w.Close() }()

	batchSize := 100
	totalSent := int64(0)
	startTime := time.Now()
	lastReport := 0

	for totalSent < targetSize {
		msgs := make([]kafka.Message, batchSize)
		for i := range msgs {
			msgs[i] = kafka.Message{Value: payload}
		}
		if err := w.WriteMessages(context.Background(), msgs...); err != nil {
			log.Fatal("failed to write messages: ", err)
		}
		totalSent += int64(len(payload) * batchSize)

		if totalSent/(1024*1024) >= int64(lastReport+100) {
			lastReport = int(totalSent / (1024 * 1024))
			elapsed := time.Since(startTime)
			rate := float64(totalSent) / 1024 / 1024 / elapsed.Seconds()
			fmt.Printf("Sent %d MB (%.2f MB/s)\n", lastReport, rate)
		}
	}

	fmt.Printf("Done! Sent %d MB in %v\n", targetMB, time.Since(startTime))
}
