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
