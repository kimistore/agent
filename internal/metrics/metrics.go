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

	"kimistore/internal/version"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// All metrics are namespaced under kimistore_. Reusing Kafka's own metric
// names (kafka_request_count_total and friends) makes a side-by-side
// deployment, or a shared Prometheus, silently merge the two systems'
// numbers, which is worse than having no metrics at all.
var (
	// Broker metrics
	RequestCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kimistore_request_count_total",
		Help: "Total number of requests",
	}, []string{"api", "version", "error"})

	RequestTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kimistore_request_time_milliseconds_total",
		Help:    "Total time spent processing requests",
		Buckets: []float64{1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000},
	}, []string{"api", "version"})

	RequestSizeBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kimistore_request_size_bytes_total",
		Help: "Total size of requests in bytes",
	}, []string{"api", "version"})

	// General Metrics
	ConnectionCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kimistore_server_connection_count",
		Help: "Number of active connections",
	})

	NetworkIOBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kimistore_network_io_bytes_total",
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

	// UnsupportedAPIVersions counts requests refused because this broker does
	// not implement that API, or that version of it.
	//
	// It is the signal to watch for a client that is not honouring version
	// negotiation: a client which reads ApiVersions and adapts should never
	// generate one of these. A non-zero count for a specific API means some
	// client is hard-coding a version list, and the count says which.
	UnsupportedAPIVersions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kimistore_unsupported_api_versions_total",
		Help: "Requests refused with UNSUPPORTED_VERSION, by API and version",
	}, []string{"api", "version"})

	// WAL recovery
	WALRecoveryTruncations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_wal_recovery_truncations_total",
		Help: "Total number of times WAL recovery discarded a torn or corrupt trailing record after an unclean shutdown",
	})

	// Writer lease. These answer the question an operator actually has during
	// a bad morning: is this process the only one writing the log, and is it
	// still able to renew that claim?
	LeaseOwned = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kimistore_lease_owned",
		Help: "1 when this agent holds the object-store writer lease, 0 otherwise",
	})

	// Per-partition ownership. These replace the lease gauges once ownership is
	// on: an operator asks how much of the log this agent is responsible for,
	// and whether it can still prove it.
	PartitionsOwned = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kimistore_partitions_owned",
		Help: "Partitions this agent currently holds an ownership claim on",
	})

	OwnershipClaimFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_ownership_claim_failures_total",
		Help: "Failed attempts to acquire or renew a partition ownership claim",
	})

	OwnershipRenewalFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_ownership_renewal_failures_total",
		Help: "Failed attempts to renew a partition ownership claim",
	})

	PartitionWriteRefusals = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_ownership_refused_writes_total",
		Help: "Produce requests refused because this agent does not own the partition",
	})

	WriterEpoch = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kimistore_writer_epoch",
		Help: "Ownership epoch this agent writes a partition under",
	}, []string{"partition"})

	// Routing. These answer the question phase 3 exists to make answerable:
	// which agents does this one believe are alive, and who owns what.
	AgentsLive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kimistore_agents_live",
		Help: "Agents in this agent's view of the cluster, including itself",
	})

	RoutingBrokers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kimistore_routing_brokers",
		Help: "Brokers this agent reports in Metadata",
	})

	RoutingAgeSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kimistore_routing_age_seconds",
		Help: "Age of the routing view Metadata is answered from; a rising value means the refresh is failing",
	})

	RoutingPublishConflicts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_routing_publish_conflicts_total",
		Help: "Liveness renewals refused because another process publishes the same agent id",
	})

	RoutingInconsistentTables = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_routing_inconsistent_tables_total",
		Help: "Routing tables ignored because they disagreed with the agent's liveness record",
	})

	RoutingDuplicateNodeIDs = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_routing_duplicate_node_ids_total",
		Help: "Brokers dropped from the routing view because another broker claimed the same node id",
	})

	RoutingInventoryFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_routing_inventory_failures_total",
		Help: "Refreshes where the cluster topic/partition inventory could not be listed",
	})

	// Coordinator fencing. CoordinatorRequestsRefused is the number to watch: it
	// rising means clients are being sent to an agent that is not the
	// coordinator, which is either a normal reshuffle or a cluster whose views
	// disagree.
	AgentNotLive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kimistore_agent_live",
		Help: "1 when this agent can prove it is alive and may coordinate groups, 0 after a failed renewal",
	})

	CoordinatorOwner = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_coordinator_groups_total",
		Help: "Group requests accepted as coordinator",
	})

	CoordinatorRequestsRefused = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_coordinator_refused_total",
		Help: "Group requests refused with NOT_COORDINATOR because another agent holds the group",
	})

	LeaseEpoch = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kimistore_lease_epoch",
		Help: "Fencing epoch of the writer lease this agent holds",
	})

	LeaseRenewalFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_lease_renewal_failures_total",
		Help: "Failed attempts to renew the object-store writer lease",
	})

	LeasedOutRefusals = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_lease_refused_writes_total",
		Help: "Produce requests refused because the writer lease was lost or held elsewhere",
	})

	// Object-store operations that hit their deadline. A rising count here is
	// the visible symptom of the stall that unbounded calls used to hide.
	ObjStoreTimeouts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_object_store_timeouts_total",
		Help: "Object-store operations abandoned after exceeding the operation timeout",
	})

	// Durability of acks=all writes (posture D2). DurableWaitSeconds is how
	// long a producer waited for its segment to land in object storage, which
	// is the ack latency D2 adds on top of the local fsync. DurableTimeouts
	// counts the waits that gave up, and DurableFlushes counts the forced
	// segment seals they caused.
	DurableWaitSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kimistore_durable_wait_seconds",
		Help:    "Time an acks=all produce waited for its segment to reach object storage",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	})

	DurableTimeouts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_durable_timeouts_total",
		Help: "acks=all produces that abandoned the wait for object storage before their deadline",
	})

	DurableFlushes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_durable_flushes_total",
		Help: "Active segments sealed to satisfy a pending acks=all durability wait",
	})

	// Handover. Phase 4's whole claim is that a failover does not have to cost a
	// tail of in-flight writes, and that is only worth anything if it is measured:
	// HandoverSeconds is how long a partition actually took to move, and
	// HandoverKept counts the handovers that refused to release rather than hand
	// over a partition whose tail was not safe.
	HandoverCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_handover_completed_total",
		Help: "Partitions handed to another agent with their tail durable and a manifest written",
	})

	HandoverKept = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_handover_kept_total",
		Help: "Handovers that kept their claim because the partition's tail was not safe to release",
	})

	HandoverSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kimistore_handover_seconds",
		Help:    "Wall time to hand a partition over, from seal to released claim",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	})

	// Recovery. ObjectsReconciled counts partitions whose recovered log end had to
	// be corrected upwards from object storage, which is the signature of a crash
	// that left a manifest behind the segments it describes. It should be rare; if
	// it is not, the tail of the system is failing more often than the dashboards
	// suggest.
	ObjectsReconciled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_objects_reconciled_total",
		Help: "Partitions whose recovered log end was raised to match object storage after a crash",
	})

	// Idempotent producers. ProducerDuplicates is the number of retried
	// batches recognised and answered with their original offset instead of
	// being appended a second time.
	ProducerIDsAllocated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_producer_ids_allocated_total",
		Help: "Producer ids handed out by InitProducerId",
	})

	ProducerDuplicates = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kimistore_producer_duplicate_batches_total",
		Help: "Idempotent produce batches recognised as retries and not appended again",
	})

	// These will be registered manually via callbacks
	TopicCount     prometheus.GaugeFunc
	PartitionCount prometheus.GaugeFunc

	// BuildInfo carries the build stamp as labels with a constant value of 1.
	BuildInfo *prometheus.GaugeVec
)

// Init initializes functional metrics that rely on callbacks to other systems
func Init(topicCountFn func() float64, partitionCountFn func() float64) {
	TopicCount = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "kimistore_topic_count",
		Help: "Total number of topics",
	}, topicCountFn)

	PartitionCount = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "kimistore_partition_total_count",
		Help: "Total number of partitions",
	}, partitionCountFn)

	// Build info, following the Prometheus convention: a constant value of 1
	// carrying the version and commit as labels. A gauge rather than an
	// info-style metric because this agent does not register a custom
	// collector, and the label form is what dashboards and alert queries
	// already expect.
	BuildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kimistore_build_info",
		Help: "Build information; the value is always 1",
	}, []string{"version", "commit", "date"})

	BuildInfo.With(prometheus.Labels{
		"version": version.Version,
		"commit":  version.Commit,
		"date":    version.Date,
	}).Set(1)
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
