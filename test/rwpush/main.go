// Command rw2 pushes Prometheus remote-write samples using the official
// Prometheus protobuf types, so the payload on the wire is exactly what a real
// Prometheus agent would send.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:9098/api/v1/push", "remote-write endpoint")
	tenant := flag.String("tenant", "demo", "tenant id")
	series := flag.Int("series", 5, "number of series")
	points := flag.Int("points", 20, "samples per series")
	now := flag.Int64("now", 0, "override timestamp (unix ms)")
	flag.Parse()

	nowMs := *now
	if nowMs == 0 {
		nowMs = time.Now().UnixMilli()
	}

	req := &prompb.WriteRequest{}
	for s := 0; s < *series; s++ {
		ts := &prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: fmt.Sprintf("kimi_test_metric_%d", s)},
				{Name: "instance", Value: fmt.Sprintf("host-%d:9100", s)},
				{Name: "job", Value: "kimi-e2e"},
			},
		}
		for p := 0; p < *points; p++ {
			ts.Samples = append(ts.Samples, prompb.Sample{
				Value:     float64(s*100 + p),
				Timestamp: nowMs - int64((*points-p-1)*60_000),
			})
		}
		req.Timeseries = append(req.Timeseries, *ts)
	}

	raw, err := req.Marshal()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// The explicit proto parameter matters: Mimir 3.x sniffs the content type
	// to choose a deserialiser, and a bare "application/x-protobuf" leaves it
	// guessing.
	contentType := "application/x-protobuf; proto=prometheus.WriteRequest"

	body := snappy.Encode(nil, raw)
	hr, err := http.NewRequest("POST", *endpoint, bytes.NewReader(body))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	hr.Header.Set("Content-Type", contentType)
	hr.Header.Set("Content-Encoding", "snappy")
	hr.Header.Set("X-Scope-OrgID", *tenant)

	resp, err := http.DefaultClient.Do(hr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "push failed:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Printf("push: HTTP %d (%d series x %d points, %d bytes) ct=%s\n",
		resp.StatusCode, *series, *points, len(raw), contentType)
	if resp.StatusCode/100 != 2 {
		fmt.Println("  ", string(out))
		os.Exit(1)
	}
}
