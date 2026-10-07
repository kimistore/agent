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
	// The integrity test pushes in batches, and needs to know exactly which
	// series and samples to expect on the way back out. Values are a pure
	// function of (series, point) so the expectation is computed rather than
	// captured, which means a wrong value cannot agree with itself.
	seriesStart := flag.Int("series-start", 0, "first series index")
	batch := flag.Int("batch", 0, "series per request (0 = all in one request)")
	interval := flag.Int64("interval-ms", 60_000, "milliseconds between points")
	flag.Parse()

	nowMs := *now
	if nowMs == 0 {
		nowMs = time.Now().UnixMilli()
	}

	perRequest := *series
	if *batch > 0 && *batch < perRequest {
		perRequest = *batch
	}

	totalSeries := *seriesStart + *series
	totalBytes := 0
	requests := 0
	for first := *seriesStart; first < totalSeries; first += perRequest {
		count := perRequest
		if first+count > totalSeries {
			count = totalSeries - first
		}
		n, err := pushBatch(first, count, *points, nowMs, *interval, *endpoint, *tenant)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		totalBytes += n
		requests++
	}

	fmt.Printf("push: %d request(s), %d series x %d points, %d bytes\n",
		requests, *series, *points, totalBytes)
}

// pushBatch sends one remote-write request covering series [first, first+count).
func pushBatch(first, count, points int, nowMs, interval int64, endpoint, tenant string) (int, error) {
	req := &prompb.WriteRequest{}
	for i := 0; i < count; i++ {
		s := first + i
		ts := &prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: fmt.Sprintf("kimi_test_metric_%d", s)},
				{Name: "instance", Value: fmt.Sprintf("host-%d:9100", s)},
				{Name: "job", Value: "kimi-e2e"},
			},
		}
		for p := 0; p < points; p++ {
			ts.Samples = append(ts.Samples, prompb.Sample{
				Value:     float64(s*100 + p),
				Timestamp: nowMs - int64(points-p-1)*interval,
			})
		}
		req.Timeseries = append(req.Timeseries, *ts)
	}

	raw, err := req.Marshal()
	if err != nil {
		return 0, err
	}

	// The explicit proto parameter matters: Mimir 3.x sniffs the content type
	// to choose a deserialiser, and a bare "application/x-protobuf" leaves it
	// guessing.
	contentType := "application/x-protobuf; proto=prometheus.WriteRequest"

	body := snappy.Encode(nil, raw)
	hr, err := http.NewRequest("POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	hr.Header.Set("Content-Type", contentType)
	hr.Header.Set("Content-Encoding", "snappy")
	hr.Header.Set("X-Scope-OrgID", tenant)

	resp, err := http.DefaultClient.Do(hr)
	if err != nil {
		return 0, fmt.Errorf("push failed: %w", err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(out))
	}
	return len(raw), nil
}
