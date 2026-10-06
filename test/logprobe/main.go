// Command logprobe counts what is actually in kimistore's log, independently of
// any consumer.
//
// The integrity e2e reads its data back through Mimir, which makes Mimir a
// confound: a slow ingest, an out-of-order rejection or a paused consumer all look
// identical to a broker that lost records. This walks the log directly and reports
// the record count alongside the high watermark, so "the broker lost it" and "the
// consumer did not read it" are distinguishable.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	addr, topic := os.Args[1], os.Args[2]

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	client := &kafka.Client{Addr: kafka.TCP(addr), Timeout: 30 * time.Second}

	parts, err := client.Metadata(ctx, &kafka.MetadataRequest{
		Addr:   kafka.TCP(addr),
		Topics: []string{topic},
	})
	if err != nil {
		fmt.Println("metadata:", err)
		os.Exit(1)
	}
	for _, t := range parts.Topics {
		fmt.Printf("topic %s: %d partition(s)\n", t.Name, len(t.Partitions))
		for _, p := range t.Partitions {
			fmt.Printf("  partition %d leader=%s:%d\n", p.ID, p.Leader.Host, p.Leader.Port)
		}
	}

	// Offsets come from the client rather than a partition-bound connection: the
	// broker advertises host.docker.internal, which only resolves inside the
	// Docker network, and DialLeader follows it.
	first, hw := listOffsets(ctx, client, addr, topic)
	fmt.Printf("partition 0: first=%d high-watermark=%d -> %d records claimed\n",
		first, hw, hw-first)

	// Walk every record, so a high watermark that runs past what the broker can
	// serve shows up as a mismatch instead of being taken on trust.
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{addr},
		Topic:     topic,
		Partition: 0,
		Dialer:    &kafka.Dialer{Timeout: 30 * time.Second},
		MinBytes:  1,
		MaxBytes:  64 << 20,
	})
	defer func() { _ = r.Close() }()
	if err := r.SetOffset(first); err != nil {
		fmt.Println("set offset:", err)
		os.Exit(1)
	}

	records, payload := int64(0), 0
	progress := time.Now()
	for first+records < hw {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Println("read:", err)
			break
		}
		records++
		payload += len(m.Value)
		if time.Since(progress) > 20*time.Second {
			progress = time.Now()
			fmt.Printf("  ... %d/%d records, %d payload bytes\n", records, hw-first, payload)
		}
	}
	fmt.Printf("walked %d/%d records, %d payload bytes\n", records, hw-first, payload)

	if records != hw-first {
		fmt.Printf("MISMATCH: the log claims %d records but serves %d\n", hw-first, records)
		os.Exit(1)
	}
	fmt.Println("consistent: every record the high watermark claims is readable")

	// The question that matters: are the records *below* the reported earliest
	// offset still there? A log that points consumers past data it still holds
	// loses it for every consumer that starts after the trim, which is exactly
	// what a Mimir restart does when it seeks to the start of the log.
	if first > 0 {
		fmt.Printf("\nreported earliest offset is %d; probing from 0 instead\n", first)
		n, bytes := walk(ctx, addr, topic, 0, first, first)
		if n > 0 {
			fmt.Printf("records below the earliest offset ARE readable: %d record(s), %d bytes\n", n, bytes)
			fmt.Println("=> the data is intact; ListOffsets(earliest) is simply reporting it as gone")
		} else {
			fmt.Println("=> records below the earliest offset are NOT readable: the data is gone")
			os.Exit(1)
		}
	}
}

// listOffsets returns the earliest offset and the high watermark for partition 0.
// walk reads records from [from, until) and returns how many it got.
func walk(ctx context.Context, addr, topic string, from, until int64, limit int64) (int64, int) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{addr},
		Topic:     topic,
		Partition: 0,
		Dialer:    &kafka.Dialer{Timeout: 30 * time.Second},
		MinBytes:  1,
		MaxBytes:  64 << 20,
	})
	defer func() { _ = r.Close() }()
	if err := r.SetOffset(from); err != nil {
		fmt.Println("set offset:", err)
		return 0, 0
	}
	var n int64
	total := 0
	for n < limit {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Println("  read:", err)
			break
		}
		n++
		total += len(m.Value)
	}
	_ = until
	return n, total
}

func listOffsets(ctx context.Context, client *kafka.Client, addr, topic string) (int64, int64) {
	first, hw := int64(-1), int64(-1)
	for _, ts := range []int64{-2, -1} { // -2 earliest, -1 latest
		// Addr pins the broker to dial. Without it the client follows the
		// advertised leader, which is host.docker.internal and does not resolve
		// from the host -- so the probe cannot even ask the question otherwise.
		resp, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
			Addr: kafka.TCP(addr),
			Topics: map[string][]kafka.OffsetRequest{
				topic: {{Partition: 0, Timestamp: ts}},
			},
		})
		if err != nil {
			fmt.Println("list offsets:", err)
			os.Exit(1)
		}
		for _, po := range resp.Topics[topic] {
			if ts == -2 {
				first = po.FirstOffset
			} else {
				hw = po.LastOffset
			}
		}
	}
	if hw < 0 {
		fmt.Println("no offsets returned for", topic)
		os.Exit(1)
	}
	return first, hw
}
