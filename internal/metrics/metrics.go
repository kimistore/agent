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

package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Brokers Metrics
	RequestCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_request_count_total",
		Help: "Total number of requests",
	}, []string{"api", "version", "error"})

	RequestTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_request_time_milliseconds_total",
		Help:    "Total time spent processing requests",
		Buckets: []float64{1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000},
	}, []string{"api", "version"})

	RequestSizeBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_request_size_bytes_total",
		Help: "Total size of requests in bytes",
	}, []string{"api", "version"})

	// General Metrics
	ConnectionCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_server_connection_count",
		Help: "Number of active connections",
	})

	NetworkIOBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_network_io_bytes_total",
		Help: "Total number of bytes sent or received",
	}, []string{"direction"})

	// Uploader Metrics
	UploaderTaskCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kimistore_uploader_task_total",
		Help: "Total number of upload tasks processed",
	}, []string{"status", "source"}) // status: success, error; source: fast-path, reconciliation

	UploaderMissedEvents = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_uploader_missed_events_total",
		Help: "Total number of missed events due to channel overflow",
	})

	UploaderInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kimistore_uploader_in_flight_count",
		Help: "Number of uploads currently in progress",
	})

	UploaderBytesUploaded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_uploader_bytes_total",
		Help: "Total number of bytes uploaded to S3",
	})

	// Consumer group offset persistence
	OffsetsFlushed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_offsets_flushed_total",
		Help: "Total number of consumer group offsets successfully persisted to object storage",
	})

	OffsetFlushFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_offset_flush_failures_total",
		Help: "Total number of failed object storage writes for consumer group offsets (these are retried, not dropped)",
	})

	// Retention
	RetentionSegmentsDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_retention_segments_deleted_total",
		Help: "Total number of log segments reclaimed by retention",
	})

	// Coordinator / session reaper
	CoordinatorEvictions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_coordinator_evicted_members_total",
		Help: "Total number of consumer group members evicted for exceeding their session timeout",
	})

	// Long-poll signalling: how many Fetch requests parked waiting for data.
	FetchLongPolls = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_fetch_longpoll_total",
		Help: "Total number of Fetch requests that blocked waiting for new data instead of returning empty",
	})

	FetchLongPollWakeups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kimistore_fetch_longpoll_wakeup_total",
		Help: "Long-polled Fetch requests by outcome: woke with data, or timed out",
	}, []string{"outcome"})

	// WAL recovery
	WALRecoveryTruncations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_wal_recovery_truncations_total",
		Help: "Total number of times WAL recovery discarded a torn or corrupt trailing record after an unclean shutdown",
	})

	// These will be registered manually via callbacks
	TopicCount     prometheus.GaugeFunc
	PartitionCount prometheus.GaugeFunc
)

// Init initializes functional metrics that rely on callbacks to other systems
func Init(topicCountFn func() float64, partitionCountFn func() float64) {
	TopicCount = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "kafka_topic_count",
		Help: "Total number of topics",
	}, topicCountFn)

	PartitionCount = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "kafka_partition_total_count",
		Help: "Total number of partitions",
	}, partitionCountFn)
}

// Helpers

func ObserveRequest(apiKey int16, version int16, errorCode int16, start time.Time, size int) {
	apiStr := strconv.Itoa(int(apiKey))
	verStr := strconv.Itoa(int(version))
	errStr := strconv.Itoa(int(errorCode))

	durationMs := float64(time.Since(start).Milliseconds())

	RequestCount.WithLabelValues(apiStr, verStr, errStr).Inc()
	RequestTime.WithLabelValues(apiStr, verStr).Observe(durationMs)
	RequestSizeBytes.WithLabelValues(apiStr, verStr).Add(float64(size))
}

func IncConnection() {
	ConnectionCount.Inc()
}

func DecConnection() {
	ConnectionCount.Dec()
}

func AddIOBytes(direction string, bytes int) {
	NetworkIOBytes.WithLabelValues(direction).Add(float64(bytes))
}
