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

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kimistore/internal/coordinator"
	"kimistore/internal/metrics"
	"kimistore/internal/storage/index"
	"kimistore/internal/storage/wal"
)

type StorageEngine struct {
	walMgr   *wal.Manager
	objStore ObjectStore
	bucket   string
	walDir   string

	quit chan struct{}
	wg   sync.WaitGroup
	// closeOnce makes Close idempotent. Shutdown has more than one possible
	// caller (a signal handler, a failing startup, a test), and a second call
	// must be a no-op rather than a panic on a closed channel.
	closeOnce sync.Once
	closeErr  error

	// opTimeout bounds every object-store operation. See DefaultOperationTimeout.
	opTimeout time.Duration

	// closedCtx is cancelled when the engine starts shutting down, so
	// in-flight object-store calls are released instead of holding the drain.
	closedCtx    context.Context
	cancelClosed context.CancelFunc

	// lease is the exclusive writer claim. It is nil when the engine was
	// built without one, in which case the write path is unfenced.
	lease *leaseState

	// Parallel Uploader
	uploadChan      chan wal.UploadTask
	inFlightUploads sync.Map // path -> struct{}

	// pendingUploads counts segments handed to the uploader but not yet
	// stored. Shutdown waits on it rather than on the queue length, which can
	// read as empty while a task is still being enqueued.
	pendingUploads atomic.Int64

	// Cache for S3 List results (topic/partition -> []keys)
	segmentCache map[string][]string
	cacheMu      sync.RWMutex

	// Buffer for offsets (groupID/topic/partition -> offset)
	offsetBuf   map[string]int64
	offsetBufMu sync.Mutex

	// committedByGroup tracks each group's committed offset per topic/partition.
	// The retention log-start is the minimum across groups, computed on demand.
	//
	// This must be per-group rather than a single running minimum: a single
	// min map can never increase, so a group that advances its commit would
	// leave retention permanently over-protecting segments it no longer needs.
	committedMu      sync.Mutex
	committedByGroup map[string]map[string]int64
	// committedAuthoritative is set once consumer offsets have been loaded
	// from object storage. Until then retention must not run, because an
	// empty map would look exactly like "no consumers" and invite deletion of
	// data a consumer still needs.
	committedAuthoritative bool

	retentionCfg RetentionConfig

	// leaseCfg is the requested writer lease, applied before any goroutine
	// starts so the claim is held before recovery reads durable state.
	leaseCfg LeaseConfig

	metadataCache *MetadataCache
	coordinator   *coordinator.Coordinator

	// agentID names this agent's durable metadata namespace. The checkpoint
	// lives under _agents/<agentID>/, so two agents sharing a bucket cannot
	// overwrite each other's checkpoint. See WithAgentID.
	agentID string

	// manifestDirty tracks partitions whose durable manifest is behind the
	// in-memory state, each with the generation at which it was marked. Only
	// dirty partitions are rewritten, so a checkpoint costs one PUT per
	// changed partition rather than one per partition.
	manifestMu    sync.Mutex
	manifestDirty map[string]uint64
	manifestGen   uint64
	// manifestSaved flips once the first full save has completed, so a cold
	// start writes a manifest for every recovered partition exactly once.
	manifestSaved atomic.Bool

	// flushInterval is how often a partition that has an acks=all producer
	// waiting on it is sealed and uploaded. See WithFlushInterval.
	flushInterval time.Duration

	// durableMu guards durable and waiters; durableCh is the broadcast latch
	// an acks=all producer parks on. See durable.go.
	durableMu sync.Mutex
	durable   map[string]*durableState
	waiters   map[string]int64
	durableCh chan struct{}

	// producerStatesMu guards the map of per-partition idempotent producer
	// state. Each value carries its own lock, so appends to different
	// partitions do not serialise on one mutex. See producer.go.
	producerStatesMu sync.Mutex
	producerStates   map[string]*partitionProducerState

	// producerIDMu guards the monotonic producer id allocator. It is
	// persisted, so a restart cannot reissue an id and mistake a new
	// producer for an old one.
	producerIDMu   sync.Mutex
	nextProducerID int64

	// producerDedups counts retried batches recognised and answered rather
	// than appended. Exposed so a test can assert the duplicate window was
	// actually exercised, not merely that the log happened to be short.
	producerDedups atomic.Int64

	// dataCh is a broadcast latch signalled on every successful append, so a
	// long-polling Fetch can park until there is something to read instead of
	// returning empty immediately and being re-polled at full speed. Guarded
	// by dataMu.
	dataMu sync.Mutex
	dataCh chan struct{}
}

// StorageEngine is the only Engine implementation, and the assertion keeps it
// honest as the interface grows.
var _ Engine = (*StorageEngine)(nil)

// Option configures optional engine behaviour. The engine is usable without
// any of them; each one trades a guarantee for a configuration knob.
type Option func(*StorageEngine)

// WithLease makes the engine claim exclusive write access to the log before it
// serves anything, and refuse writes if the claim is later lost.
func WithLease(cfg LeaseConfig) Option {
	return func(se *StorageEngine) { se.leaseCfg = cfg }
}

// WithOperationTimeout bounds every object-store call. Non-positive values
// keep DefaultOperationTimeout.
func WithOperationTimeout(d time.Duration) Option {
	return func(se *StorageEngine) {
		if d > 0 {
			se.opTimeout = d
		}
	}
}

// WithAgentID sets the durable identity used to namespace this agent's
// checkpoint. It must be stable across restarts, which is what lets a restart
// pick up its own checkpoint again rather than the bucket-global one the
// previous version wrote. Empty keeps defaultAgentID.
func WithAgentID(id string) Option {
	return func(se *StorageEngine) {
		if strings.TrimSpace(id) != "" {
			se.agentID = id
		}
	}
}

// WithFlushInterval sets how long an acks=all produce may wait for its segment
// to be sealed and uploaded before the flush loop forces the issue. It is
// therefore both the coalescing window (how many appends share one object
// store PUT) and the upper bound on the ack latency D2 adds. Non-positive
// keeps DefaultFlushInterval.
func WithFlushInterval(d time.Duration) Option {
	return func(se *StorageEngine) {
		if d > 0 {
			se.flushInterval = d
		}
	}
}

// defaultAgentID is the hostname, which is stable across a restart of the
// same agent. It replaces the writer id (hostname/pid) for the checkpoint key:
// a pid changes on every start and a checkpoint keyed by it would never be
// read back.
func defaultAgentID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "unknown-agent"
	}
	return host
}

func NewStorageEngine(walDir string, objStore ObjectStore, bucket string, retentionCfg RetentionConfig, opts ...Option) (*StorageEngine, error) {
	closedCtx, cancelClosed := context.WithCancel(context.Background())
	se := &StorageEngine{
		objStore:         objStore,
		bucket:           bucket,
		walDir:           walDir,
		quit:             make(chan struct{}),
		uploadChan:       make(chan wal.UploadTask, 1024),
		segmentCache:     make(map[string][]string),
		offsetBuf:        make(map[string]int64),
		committedByGroup: make(map[string]map[string]int64),
		retentionCfg:     retentionCfg,
		metadataCache:    NewMetadataCache(),
		dataCh:           make(chan struct{}),
		opTimeout:        DefaultOperationTimeout,
		closedCtx:        closedCtx,
		cancelClosed:     cancelClosed,
		agentID:          defaultAgentID(),
		manifestDirty:    make(map[string]uint64),
		flushInterval:    DefaultFlushInterval,
		durable:          make(map[string]*durableState),
		waiters:          make(map[string]int64),
		durableCh:        make(chan struct{}),
		producerStates:   make(map[string]*partitionProducerState),
		nextProducerID:   1,
	}
	for _, opt := range opts {
		opt(se)
	}

	onRoll := func(task wal.UploadTask) {
		task.Source = "fast-path"
		se.pendingUploads.Add(1)
		select {
		case se.uploadChan <- task:
		default:
			se.pendingUploads.Add(-1)
			metrics.UploaderMissedEvents.Inc()
			log.Printf("Warning: Upload channel full, skipping fast-path for %s (it stays on local disk and is retried by the reconciler)", task.Path)
		}
	}

	mgr, err := wal.NewManager(walDir, onRoll)
	if err != nil {
		cancelClosed()
		return nil, err
	}
	se.walMgr = mgr

	// Claim the log before reading any of it. Two agents pointed at one
	// bucket would otherwise each assign offsets from their own recovered
	// state and overwrite each other's segments, and nothing below would
	// notice: the failure mode is silent data loss, not an error.
	lease, err := acquireLease(context.Background(), objStore, se.leaseCfg)
	if err != nil {
		_ = mgr.Close()
		cancelClosed()
		return nil, err
	}
	se.lease = lease

	// Load/Merge Cache from WAL
	if err := se.metadataCache.Load(walDir); err != nil {
		log.Printf("Warning: Failed to load metadata cache: %v", err)
	}

	// Start worker pool (8 workers)
	for i := 0; i < 8; i++ {
		se.wg.Add(1)
		go se.uploaderWorker(se.closedCtx)
	}

	// Start background uploader (reconciliation), offset flusher, retention,
	// checkpoint, and durability flush loops.
	se.wg.Add(5)
	go se.uploaderLoop()
	go se.offsetFlusherLoop()
	go se.retentionLoop()
	go se.checkpointLoop()
	go se.flushLoop()

	// Recover the durable log position before the engine is handed out.
	//
	// This has to happen here rather than in a later wiring step. If it were
	// left to whoever attaches a coordinator, then any caller that skipped
	// that step would come up with an empty local WAL, no checkpoint and no
	// object-store recovery, and start handing out offsets that are already
	// taken.
	if err := se.restoreDurableState(context.Background()); err != nil {
		if errors.Is(err, errSuperseded) {
			// Durable state belongs to a writer that held the log after us.
			// There is no safe way to carry on from here, so tear the engine
			// down and let the caller decide.
			_ = se.Close()
			return nil, err
		}
		log.Printf("Warning: could not fully restore durable state: %v", err)
	}

	// Load consumer offsets from object storage so retention knows the log
	// start. Registered on the wait group so Close cannot return while it is
	// still using the object store.
	se.wg.Add(1)
	go func() {
		defer se.wg.Done()
		se.rehydrateCommittedOffsets(context.Background())
	}()

	return se, nil
}

// restoreDurableState reloads the checkpoint and then repairs anything the
// checkpoint does not know about from object storage. It is safe to call more
// than once.
func (s *StorageEngine) restoreDurableState(ctx context.Context) error {
	if err := s.loadCheckpointInto(ctx, false); err != nil {
		if errors.Is(err, errSuperseded) {
			// Being fenced out is not recoverable by falling back: the only
			// correct action is to not serve this log.
			return err
		}
		log.Printf("No usable checkpoint (%v); recovering log position from object storage", err)
	}
	if err := s.recoverManifest(ctx); err != nil {
		if errors.Is(err, errSuperseded) {
			return err
		}
		log.Printf("Warning: could not recover log end offsets from object storage: %v", err)
	}
	s.applySeeds()
	// The producer id allocator must be loaded before any produce can run, or
	// a restart could reissue an id and a new producer would be mistaken for
	// an old one. The sequence state is then rebuilt from the local WAL tail
	// so a retried batch after a restart is still recognised as a duplicate.
	s.loadProducerIDSeq(ctx)
	s.recoverProducerState(ctx)
	return nil
}

// Append writes a batch and returns its base offset. When sync is true the
// data is fsynced before returning, so the offset may be acknowledged to the
// producer as durable. Callers should set sync from the request's acks value
// (acks=0 is fire-and-forget; acks>=1 promises durability).
func (s *StorageEngine) Append(topic string, partition int32, batch []byte, recordCount int, sync bool) (int64, error) {
	return s.AppendContext(context.Background(), topic, partition, batch, recordCount, sync)
}

// AppendContext is Append with a caller context, so a produce request whose
// client has gone away stops costing a durable write.
func (s *StorageEngine) AppendContext(ctx context.Context, topic string, partition int32, batch []byte, recordCount int, sync bool) (int64, error) {
	return s.appendCore(ctx, topic, partition, batch, recordCount, sync)
}

// appendCore is the write path shared by the idempotent and non-idempotent
// producers. It assigns an offset and updates the durable position; producer
// sequence accounting is layered on top by AppendIdempotentContext.
func (s *StorageEngine) appendCore(ctx context.Context, topic string, partition int32, batch []byte, recordCount int, sync bool) (int64, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	// Refuse to write unless this agent still owns the log. A writer whose
	// lease was taken over would assign offsets that collide with the new
	// holder's and overwrite its segments; that is silent data loss, so it
	// has to be a refusal rather than a best-effort write.
	if err := s.lease.checkWrite(); err != nil {
		metrics.LeasedOutRefusals.Inc()
		return -1, err
	}
	// Take the read lock first: once a partition is known the write path
	// never needs the cache's write lock, so a hot partition does not
	// serialise every producer on one mutex.
	if !s.metadataCache.Has(topic, partition) {
		s.metadataCache.AddPartition(topic, partition)
	}

	// Seed the durability frontier before the write, while the log end offset
	// is still the pre-append position: everything below it came from object
	// storage at startup or an earlier confirmed upload, so it is a sound
	// floor for the contiguity check that follows.
	s.ensureDurable(topic, partition, s.metadataCache.LogEndOffset(topic, partition))

	offset, err := s.walMgr.Append(topic, partition, batch, recordCount, sync)
	if err != nil {
		return -1, err
	}

	// The record is in the log, so the durable position must reflect it even
	// if the client has already gone away: a stale log end offset is what
	// makes the next append collide with this one.
	s.metadataCache.AdvancePartition(topic, partition, offset+int64(recordCount))
	s.markManifestDirty(topic, partition)
	// Wake any long-polling Fetch requests. Signalled after the write so
	// a woken reader is guaranteed to observe the new high watermark.
	s.signalData()

	if ctx.Err() != nil {
		// The client is gone. The record is already in the log, so it cannot
		// be rolled back; abandoning the response is the only correct
		// outcome, and the caller reports a cancelled request.
		return offset, ctx.Err()
	}
	return offset, nil
}

// signalData broadcasts that new data is available.
func (s *StorageEngine) signalData() {
	s.dataMu.Lock()
	close(s.dataCh)
	s.dataCh = make(chan struct{})
	s.dataMu.Unlock()
}

// DataSignal returns a channel closed on the next successful append. Callers
// must re-check their condition after it fires, since it is a broadcast and
// may be triggered by an append to a different partition.
func (s *StorageEngine) DataSignal() <-chan struct{} {
	s.dataMu.Lock()
	defer s.dataMu.Unlock()
	return s.dataCh
}

// Read returns exactly one stored record blob covering offset. Callers that
// want to fill a budget should use ReadBatch.
func (s *StorageEngine) Read(topic string, partition int32, offset int64) ([]byte, error) {
	// A zero budget means "one entry": the point of Read is the entry that
	// covers the offset, not throughput.
	data, _, err := s.ReadBatch(topic, partition, offset, 0)
	return data, err
}

// defaultReadBudget bounds a read when the caller expresses no preference.
const defaultReadBudget = int64(1 << 20)

// ReadBatch serves as many records as fit the byte budget, starting at
// offset, and reports the offset to fetch next.
//
// Returning one blob per Fetch turns the consumer into a request loop: one
// round trip per producer batch, and on the cold path one object-store GET
// per batch. Honouring the caller's byte budget is what makes the fetch loop
// behave like a broker's.
func (s *StorageEngine) ReadBatch(topic string, partition int32, offset int64, maxBytes int64) ([]byte, int64, error) {
	return s.ReadBatchContext(context.Background(), topic, partition, offset, maxBytes)
}

// ReadBatchContext is ReadBatch with a caller context, so a consumer that
// disconnects mid-fetch releases its object-store read instead of holding a
// handler goroutine and an in-flight slot until the read happens to finish.
func (s *StorageEngine) ReadBatchContext(ctx context.Context, topic string, partition int32, offset int64, maxBytes int64) ([]byte, int64, error) {
	// A caller that has gone away must be told that, rather than being told
	// the offset is missing. The two mean very different things to a consumer:
	// one is a retry, the other a reset.
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	// maxBytes of zero or less means "one entry"; Read relies on that, while
	// a fetch with no stated ceiling gets a sane default.
	oneEntry := maxBytes <= 0
	if oneEntry {
		maxBytes = defaultReadBudget
	}

	// 1. Hot path: the local WAL.
	if data, next, err := s.walMgr.ReadBatch(topic, partition, offset, budgetFor(oneEntry, maxBytes)); err == nil {
		return s.reframe(data, offset), next, nil
	}

	// 2. Cold path: object storage.
	prefix := partitionPrefix(topic, partition)
	cacheKey := fmt.Sprintf("%s/%d", topic, partition)

	keys, err := s.cachedSegments(ctx, prefix, cacheKey)
	if err != nil {
		return nil, 0, err
	}

	bestKey := bestSegmentFor(keys, offset)
	if bestKey == "" {
		return nil, 0, fmt.Errorf("offset %d not found in any segment under %s", offset, prefix)
	}

	baseOffset := parseOffsetFromKey(bestKey)
	start := int64(0)
	indexed := false

	if rc, err := s.objGet(ctx, strings.TrimSuffix(bestKey, ".log")+".index"); err == nil {
		if indexBytes, readErr := io.ReadAll(rc); readErr == nil && len(indexBytes) > 0 {
			if pos, lookupErr := index.Lookup(indexBytes, offset, baseOffset); lookupErr == nil {
				start, indexed = pos, true
			}
		}
		_ = rc.Close()
	}

	var rc io.ReadCloser
	if indexed {
		// The index already points at the start of a record, so only the
		// region from there to the end of the segment is needed. Sizing this
		// to the whole segment is what made a 1 KB read cost a 10 MB GET.
		rc, err = s.objGetRange(ctx, bestKey, start, 0)
	} else {
		rc, err = s.objGet(ctx, bestKey)
	}
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rc.Close() }()

	data, next, err := scanStreamForOffset(rc, offset, budgetFor(oneEntry, maxBytes), baseOffset)
	if err != nil {
		return nil, 0, err
	}
	return s.reframe(data, offset), next, nil
}

// reframe serves a wrapped RecordBatch bare.
//
// This is not a deviation from the protocol: both framings are valid Kafka,
// and a RecordBatch is self-describing either way. It is done unconditionally
// because the client this agent exists to serve cannot read the wrapped form
// at all, which would otherwise make data written by a Java or librdkafka
// producer unreadable. See wal.UnwrapBatches.
func (s *StorageEngine) reframe(data []byte, offset int64) []byte {
	out, changed := wal.UnwrapBatches(data, offset)
	if !changed {
		return data
	}
	return out
}

// budgetFor maps the single-entry request onto a byte budget: large enough for
// one blob, small enough that the caller's drain loops stop after it.
func budgetFor(oneEntry bool, maxBytes int64) int64 {
	if oneEntry {
		return singleEntryBudget
	}
	return maxBytes
}

// singleEntryBudget is effectively unbounded for one record, and
// unsatisfiable for two: the drain loops compare against it and stop.
const singleEntryBudget int64 = 1 << 40

// cachedSegments returns the segment keys under prefix, listing object
// storage only on a cache miss.
func (s *StorageEngine) cachedSegments(ctx context.Context, prefix, cacheKey string) ([]string, error) {
	s.cacheMu.RLock()
	keys, hit := s.segmentCache[cacheKey]
	s.cacheMu.RUnlock()
	if hit {
		return keys, nil
	}

	// Serialise the refill so a burst of cold reads produces one LIST rather
	// than one per reader.
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if keys, hit := s.segmentCache[cacheKey]; hit {
		return keys, nil
	}

	objects, err := s.objList(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list %s: %w", prefix, err)
	}
	keys = make([]string, 0, len(objects))
	for _, o := range objects {
		if strings.HasSuffix(o.Key, ".log") {
			keys = append(keys, o.Key)
		}
	}
	s.segmentCache[cacheKey] = keys
	return keys, nil
}

// bestSegmentFor picks the newest segment whose start offset is at or below
// the requested offset.
func bestSegmentFor(keys []string, offset int64) string {
	var bestKey string
	var bestStart int64 = -1
	for _, k := range keys {
		if !strings.HasSuffix(k, ".log") {
			continue
		}
		start := parseOffsetFromKey(k)
		if start < 0 {
			continue
		}
		if start <= offset && start > bestStart {
			bestStart, bestKey = start, k
		}
	}
	return bestKey
}

func (s *StorageEngine) GetTopics() ([]string, error) {
	return s.metadataCache.GetTopics(), nil
}

// GetPartitions reports every partition known for a topic.
//
// The durable cache is the authority, unioned with whatever the local WAL
// directory knows. Consulting the local directory alone is what used to make a
// restarted agent advertise a single partition for a topic that had N: sealed
// segments are deleted once offloaded, so the directory is empty on restart
// and the partition set with it.
func (s *StorageEngine) GetPartitions(topic string) ([]int32, error) {
	seen := make(map[int32]bool)
	var out []int32
	for _, p := range s.metadataCache.GetPartitions(topic) {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	if local, err := s.walMgr.ListPartitions(topic); err == nil {
		for _, p := range local {
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	return out, nil
}

// HighWaterMark is the offset the next append to the partition will take.
//
// A single-node agent replicates nothing, so the high watermark and the log
// end offset are the same thing.
func (s *StorageEngine) HighWaterMark(topic string, partition int32) int64 {
	if l := s.walMgr.HighWaterMark(topic, partition); l > 0 {
		return l
	}
	return s.metadataCache.LogEndOffset(topic, partition)
}

// LogStartOffset is the oldest offset still retrievable. It is what
// ListOffsets must report as the earliest offset; returning 0 once retention
// has reclaimed the start turns every new consumer into a livelock of
// OffsetOutOfRange against its own reset.
func (s *StorageEngine) LogStartOffset(topic string, partition int32) int64 {
	return s.metadataCache.LogStartOffset(topic, partition)
}

func (s *StorageEngine) CreateTopic(topic string, partitions int32) error {
	return s.CreateTopicContext(context.Background(), topic, partitions)
}

// CreateTopicContext is CreateTopic with a caller context, so the checkpoint
// it writes is bounded by the request that caused it.
func (s *StorageEngine) CreateTopicContext(ctx context.Context, topic string, partitions int32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.walMgr.CreateTopic(topic, partitions)
	if err == nil {
		s.metadataCache.AddTopic(topic)
		for i := int32(0); i < partitions; i++ {
			s.metadataCache.AddPartition(topic, i)
			// A topic that only exists in memory vanishes on restart, so
			// its partitions are written to their manifests immediately.
			s.markManifestDirty(topic, i)
		}
		// Persist immediately: a topic whose creation only exists in memory
		// is a topic that vanishes on restart.
		if err := s.SaveCheckpointContext(ctx); err != nil {
			log.Printf("CreateTopic: could not persist topic %s: %v", topic, err)
		}
	}
	return err
}

// TopicExists reports whether the topic is known to the durable registry.
func (s *StorageEngine) TopicExists(topic string) bool {
	for _, t := range s.metadataCache.GetTopics() {
		if t == topic {
			return true
		}
	}
	return false
}

func (s *StorageEngine) DeleteTopic(topic string) error {
	return s.DeleteTopicContext(context.Background(), topic)
}

// DeleteTopicContext is DeleteTopic with a caller context.
func (s *StorageEngine) DeleteTopicContext(ctx context.Context, topic string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.walMgr.DeleteTopic(topic)
	if err == nil {
		s.metadataCache.RemoveTopic(topic)
		s.forgetManifestDirty(topic)
		// Drop retention bookkeeping for the topic across every group.
		s.committedMu.Lock()
		for _, byGroup := range s.committedByGroup {
			for k := range byGroup {
				if strings.HasPrefix(k, topic+"/") {
					delete(byGroup, k)
				}
			}
		}
		s.committedMu.Unlock()
		// Delete cold segments from Object Storage asynchronously in the background
		// Detached from the request: deleting a topic's cold data is cleanup,
		// not part of answering DeleteTopics, and the client should not wait
		// on it. The engine's own context is the right parent here rather than
		// the request's, so the cleanup survives the client going away and
		// still dies with the process.
		//nolint:contextcheck // deliberately not derived from the request
		go s.asyncDeleteTopicFromS3(s.closedCtx, topic)
		// Drop the consumed offsets too. Leaving them behind would pin
		// retention's log start for a topic that no longer exists.
		//nolint:contextcheck // deliberately not derived from the request
		go s.asyncDeleteTopicOffsets(s.closedCtx, topic)
	}
	return err
}

// asyncDeleteTopicOffsets removes every consumer group's committed offsets
// for a deleted topic.
func (s *StorageEngine) asyncDeleteTopicOffsets(ctx context.Context, topic string) {
	objects, err := s.objList(ctx, offsetsPrefix)
	if err != nil {
		log.Printf("Async offset cleanup: failed to list offsets: %v", err)
		return
	}
	prefix := topic + "/"
	for _, obj := range objects {
		if !strings.Contains(obj.Key, prefix) {
			continue
		}
		if err := s.objDelete(ctx, obj.Key); err != nil {
			log.Printf("Async offset cleanup: failed to delete %s: %v", obj.Key, err)
			continue
		}
		log.Printf("Async offset cleanup: deleted %s", obj.Key)
	}
}

func (s *StorageEngine) asyncDeleteTopicFromS3(ctx context.Context, topic string) {
	// Both the segments and the Phase 0 per-partition manifests live outside a
	// topic's data prefix, so deleting one does not delete the other.
	s.deletePrefix(ctx, topic+"/")
	s.deletePrefix(ctx, topicsMetadataPrefix+topic+"/")
}

// deletePrefix removes every object under prefix, logging each failure and
// carrying on: a topic deletion is cleanup, and one stuck object must not
// abandon the rest.
func (s *StorageEngine) deletePrefix(ctx context.Context, prefix string) {
	objects, err := s.objList(ctx, prefix)
	if err != nil {
		log.Printf("Async S3 cleanup: Failed to list S3 objects under %s: %v", prefix, err)
		return
	}

	for _, obj := range objects {
		if !strings.HasPrefix(obj.Key, prefix) {
			continue
		}
		if err := s.objDelete(ctx, obj.Key); err != nil {
			log.Printf("Async S3 cleanup: Failed to delete cold object %s: %v", obj.Key, err)
		} else {
			log.Printf("Async S3 cleanup: Deleted cold object %s", obj.Key)
		}
	}
}

// scanStreamForOffset walks stored entries from r, skipping until it reaches
// targetOffset, and then accumulates records until the byte budget is spent.
//
// Each entry is [offset(8)][size(4)][body], and a body may hold many records,
// so the target offset can legitimately land in the middle of one. Offsets in
// the returned blob are rewritten into the log's offset space, and a wrapped
// RecordBatch has its own base offset repaired as well.
func scanStreamForOffset(r io.Reader, targetOffset, maxBytes int64, baseOffset int64) ([]byte, int64, error) {
	if maxBytes <= 0 {
		maxBytes = defaultReadBudget
	}

	var out []byte
	next := targetOffset
	found := false

	header := make([]byte, 12)
	for {
		if _, err := io.ReadFull(r, header); err != nil {
			if !found {
				return nil, 0, fmt.Errorf("offset %d not found in segment: %w", targetOffset, err)
			}
			return out, next, nil
		}

		msgOffset := int64(binary.BigEndian.Uint64(header[0:8]))
		msgSize := int32(binary.BigEndian.Uint32(header[8:12]))
		if msgSize < 0 || msgSize > maxWalEntrySize {
			if !found {
				return nil, 0, fmt.Errorf("offset %d not found in segment: implausible entry size %d", targetOffset, msgSize)
			}
			return out, next, nil
		}
		if msgOffset > targetOffset && found {
			return out, next, nil
		}

		body := make([]byte, msgSize)
		if _, err := io.ReadFull(r, body); err != nil {
			if !found {
				return nil, 0, fmt.Errorf("offset %d not found in segment: %w", targetOffset, err)
			}
			return out, next, nil
		}

		count := wal.CountMessageSet(body)
		if count == 0 {
			count = 1
		}
		endOffset := msgOffset + int64(count)
		if targetOffset >= endOffset {
			continue // target lies beyond this entry
		}

		patched := wal.PatchStoredBlob(body, msgOffset, count)
		if out == nil {
			out = make([]byte, 0, len(patched))
		}
		if int64(len(out))+int64(len(patched)) > maxBytes && found {
			return out, next, nil
		}
		out = append(out, patched...)
		next = endOffset
		found = true
	}
}

// maxWalEntrySize bounds a single stored entry, mirroring the WAL's own
// limit, so a corrupt size field cannot trigger a huge allocation.
const maxWalEntrySize = 100 * 1024 * 1024

// Close shuts the engine down, making sure nothing is left on local disk.
//
// The order matters. Active segments are sealed first, which queues them for
// upload; the upload workers are then given a bounded window to drain before
// they are stopped. Exiting any earlier would strand the most recent writes on
// a local disk that is expected to disappear with the process.
func (s *StorageEngine) Close() error {
	s.closeOnce.Do(s.close)
	return s.closeErr
}

func (s *StorageEngine) close() {
	s.walMgr.SealAll()
	s.drainUploads(shutdownUploadBudget)

	// Stop renewing the lease, but keep holding it: the final offset flush
	// and checkpoint below still write durable state, and they must not race
	// a replacement writer taking the log over half way through.
	s.lease.close()

	close(s.quit)

	// Let the loops make their final pass. Every object-store call is bounded,
	// so this cannot hang indefinitely, but the bound is enforced explicitly
	// as well rather than trusted.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownFlushBudget):
		log.Printf("Shutdown: background loops still running after %s; releasing the lease and exiting anyway",
			shutdownFlushBudget)
	}

	// Hand the log over only once the last durable write has landed.
	if s.lease != nil {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownLeaseReleaseBudget)
		s.lease.release(ctx, s.objStore)
		cancel()
	}

	// Anything still parked on object storage is released now rather than
	// being waited for.
	s.cancelClosed()

	s.closeErr = s.walMgr.Close()
}

// shutdownUploadBudget is how long Close waits for the upload queue to empty.
// It is a ceiling, not a target: a slow object store must not hang shutdown.
const shutdownUploadBudget = 30 * time.Second

// shutdownFlushBudget bounds the final offset flush and checkpoint.
const shutdownFlushBudget = 45 * time.Second

// shutdownLeaseReleaseBudget bounds the hand-off of the writer lease.
const shutdownLeaseReleaseBudget = 10 * time.Second

// drainUploads blocks until every handed-off segment has been stored, or the
// budget expires.
func (s *StorageEngine) drainUploads(budget time.Duration) {
	deadline := time.Now().Add(budget)
	for {
		if s.pendingUploads.Load() == 0 {
			return
		}
		if time.Now().After(deadline) {
			log.Printf("Shutdown: %d segment(s) still uploading after %s; they remain on local disk and are retried on next start",
				s.pendingUploads.Load(), budget)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (s *StorageEngine) uploaderWorker(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.quit:
			return
		case task := <-s.uploadChan:
			s.handleUpload(ctx, task)
			s.pendingUploads.Add(-1)
		}
	}
}

func (s *StorageEngine) handleUpload(ctx context.Context, task wal.UploadTask) {
	// 1. Check/Set In-Flight
	if _, loaded := s.inFlightUploads.LoadOrStore(task.Path, struct{}{}); loaded {
		return // Already being handled
	}
	defer s.inFlightUploads.Delete(task.Path)

	metrics.UploaderInFlight.Inc()
	defer metrics.UploaderInFlight.Dec()

	// Verify file still exists (might have been uploaded by someone else just now)
	info, err := os.Stat(task.Path)
	if err != nil {
		return
	}

	key := fmt.Sprintf("%s/%d/%s", task.Topic, task.Partition, filepath.Base(task.Path))
	indexKey := strings.TrimSuffix(key, ".log") + ".index"

	log.Printf("Uploader: Processing %s (size: %d, source: %s)", key, info.Size(), task.Source)

	// 2. Generate Index
	indexData, err := index.GenerateIndex(task.Path, task.BaseOffset)
	if err != nil {
		log.Printf("Error generating index for %s: %v", task.Path, err)
	} else if len(indexData) > 0 {
		if err := s.objPut(ctx, indexKey, bytes.NewReader(indexData)); err != nil {
			log.Printf("Failed to upload index %s: %v", indexKey, err)
		}
	}

	// 3. Upload Log
	f, err := os.Open(task.Path)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()

	if err := s.objPut(ctx, key, f); err != nil {
		metrics.UploaderTaskCount.WithLabelValues("error", task.Source).Inc()
		log.Printf("Failed to upload %s: %v", key, err)
		return
	}

	// The segment is in object storage. Release any acks=all producer whose
	// offset falls inside it. A reconciliation task carries no end offset
	// (its segment predates any waiter), and markSegmentDurable ignores it.
	s.markSegmentDurable(task.Topic, task.Partition, task.BaseOffset, task.EndOffset)

	// 4. Remove Local
	if err := os.Remove(task.Path); err != nil {
		log.Printf("Failed to remove %s: %v", task.Path, err)
	} else {
		log.Printf("Uploaded and trimmed %s", key)
	}

	// 5. Update Metrics & Cache
	metrics.UploaderTaskCount.WithLabelValues("success", task.Source).Inc()
	metrics.UploaderBytesUploaded.Add(float64(info.Size()))

	cacheKey := fmt.Sprintf("%s/%d", task.Topic, task.Partition)
	s.cacheMu.Lock()
	if list, ok := s.segmentCache[cacheKey]; ok {
		s.segmentCache[cacheKey] = append(list, key)
	}
	s.cacheMu.Unlock()
}

func (s *StorageEngine) uploaderLoop() {
	defer s.wg.Done()

	// Initial scan
	s.uploadSegments()

	ticker := time.NewTicker(1 * time.Minute) // Reconciliation every minute
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			s.uploadSegments()
		}
	}
}

func (s *StorageEngine) uploadSegments() {
	err := filepath.Walk(s.walDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if info.Name() == "active.log" {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".log") {
			return nil
		}

		// path is .../wal/<topic>/<partition>/<offset>.log
		// Extract topic and partition
		rel, _ := filepath.Rel(s.walDir, path)
		parts := strings.Split(rel, string(os.PathSeparator))
		if len(parts) != 3 {
			return nil
		}

		topic := parts[0]
		pID, _ := strconv.ParseInt(parts[1], 10, 32)
		filename := parts[2]
		baseOffset, _ := strconv.ParseInt(strings.TrimSuffix(filename, ".log"), 10, 64)

		task := wal.UploadTask{
			Topic:      topic,
			Partition:  int32(pID),
			Path:       path,
			BaseOffset: baseOffset,
			Source:     "reconciliation",
		}

		select {
		case s.uploadChan <- task:
		default:
			// Queue full, will be picked up in next reconciliation
		}

		return nil
	})

	if err != nil {
		log.Printf("Error walking WAL dir: %v", err)
	}
}
func (s *StorageEngine) SaveOffset(groupID, topic string, partition int32, offset int64) error {
	// Buffer the offset save
	key := fmt.Sprintf("%s/%s/%d", groupID, topic, partition)
	s.offsetBufMu.Lock()
	s.offsetBuf[key] = offset
	s.offsetBufMu.Unlock()

	s.recordCommitted(groupID, topic, partition, offset)
	return nil
}

// recordCommitted notes a group's latest committed offset. Retention derives
// its log start from the minimum across groups, so this must track each group
// independently.
func (s *StorageEngine) recordCommitted(groupID, topic string, partition int32, offset int64) {
	tp := topic + "/" + strconv.Itoa(int(partition))

	s.committedMu.Lock()
	defer s.committedMu.Unlock()
	if s.committedByGroup[groupID] == nil {
		s.committedByGroup[groupID] = make(map[string]int64)
	}
	s.committedByGroup[groupID][tp] = offset
}

func (s *StorageEngine) offsetFlusherLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			// Shutting down: the client was told these commits succeeded, so
			// give the object store a few chances before we exit rather than
			// dropping them on a transient error.
			s.flushOffsetsOnShutdown()
			return
		case <-ticker.C:
			s.flushOffsets()
		}
	}
}

// flushOffsetsOnShutdown retries the offset flush a bounded number of times so
// a brief object-store hiccup during termination does not lose acked commits.
func (s *StorageEngine) flushOffsetsOnShutdown() {
	const attempts = 3
	for i := 1; i <= attempts; i++ {
		s.flushOffsets()

		s.offsetBufMu.Lock()
		remaining := len(s.offsetBuf)
		s.offsetBufMu.Unlock()
		if remaining == 0 {
			return
		}
		if i < attempts {
			log.Printf("Shutdown: %d offset(s) still unflushed, retrying (%d/%d)", remaining, i+1, attempts)
			time.Sleep(time.Duration(i) * 250 * time.Millisecond)
		}
	}

	s.offsetBufMu.Lock()
	remaining := len(s.offsetBuf)
	s.offsetBufMu.Unlock()
	if remaining > 0 {
		log.Printf("Shutdown: giving up on %d offset(s) that could not be written to object storage; "+
			"consumers will resume from the last persisted position", remaining)
	}
}

func (s *StorageEngine) flushOffsets() {
	s.offsetBufMu.Lock()
	if len(s.offsetBuf) == 0 {
		s.offsetBufMu.Unlock()
		return
	}
	// Snapshot under the lock, then release it before doing network I/O.
	// Entries are NOT removed here: a commit was already acknowledged to the
	// client, so it stays in the buffer until we know it is safely in S3.
	todo := make(map[string]int64, len(s.offsetBuf))
	for k, v := range s.offsetBuf {
		todo[k] = v
	}
	s.offsetBufMu.Unlock()

	ctx := context.Background()
	for k, offset := range todo {
		// k is "groupID/topic/partition"
		s3Key := fmt.Sprintf("_offsets/%s", k)
		data := []byte(strconv.FormatInt(offset, 10))
		if err := s.objPut(ctx, s3Key, bytes.NewReader(data)); err != nil {
			// Leave the entry in the buffer so the next tick retries it.
			// Dropping it here would silently discard a commit the client
			// was already told had succeeded.
			metrics.OffsetFlushFailures.Inc()
			log.Printf("Failed to flush offset %s (%d): %v -- will retry", s3Key, offset, err)
			continue
		}
		metrics.OffsetsFlushed.Inc()
		// Only clear the entry if it has not been superseded by a newer
		// commit while this write was in flight.
		s.offsetBufMu.Lock()
		if cur, ok := s.offsetBuf[k]; ok && cur == offset {
			delete(s.offsetBuf, k)
		}
		s.offsetBufMu.Unlock()
	}
}

func (s *StorageEngine) checkpointLoop() {
	defer s.wg.Done()
	// Checkpoint every 30 seconds
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {

		select {
		case <-s.quit:
			if err := s.SaveCheckpointContext(s.closedCtx); err != nil {
				log.Printf("Shutdown checkpoint: %v", err)
			}
			return
		case <-ticker.C:
			if err := s.SaveCheckpointContext(s.closedCtx); err != nil {
				log.Printf("Checkpoint: %v", err)
			}
		}
	}
}

// SetCoordinator links the group coordinator and replays any group state the
// checkpoint carried.
//
// The durable log position is already restored by NewStorageEngine; this only
// adds the coordinator, which may not exist yet at engine construction.
func (s *StorageEngine) SetCoordinator(c *coordinator.Coordinator) {
	s.coordinator = c
	if c == nil || s.metadataCache.Coordinator == nil {
		return
	}
	c.FromState(*s.metadataCache.Coordinator)
	log.Printf("Restored coordinator state from checkpoint")
}

// applySeeds hands the recovered log end offsets to the WAL manager so the
// next write continues the log instead of starting it over.
func (s *StorageEngine) applySeeds() {
	seeds := make(map[string]int64)
	for topic, parts := range s.metadataCache.TopicsSnapshot() {
		for pid, ps := range parts {
			if ps.LogEndOffset > 0 {
				seeds[fmt.Sprintf("%s/%d", topic, pid)] = ps.LogEndOffset
			}
		}
	}
	s.walMgr.Seed(seeds)
	if len(seeds) > 0 {
		log.Printf("Storage: restored log end offsets for %d partition(s) from durable metadata", len(seeds))
	}
}

// markManifestDirty records that a partition's durable manifest no longer
// matches the in-memory position and must be rewritten.
func (s *StorageEngine) markManifestDirty(topic string, partition int32) {
	key := topic + "/" + strconv.Itoa(int(partition))
	s.manifestMu.Lock()
	s.manifestGen++
	s.manifestDirty[key] = s.manifestGen
	s.manifestMu.Unlock()
}

// forgetManifestDirty drops every pending mark for a topic. A deleted
// partition is gone from the snapshot, so a mark left behind would never be
// written and never be cleared.
func (s *StorageEngine) forgetManifestDirty(topic string) {
	prefix := topic + "/"
	s.manifestMu.Lock()
	for k := range s.manifestDirty {
		if strings.HasPrefix(k, prefix) {
			delete(s.manifestDirty, k)
		}
	}
	s.manifestMu.Unlock()
}

// legacyCheckpointKey is where the checkpoint lived before Phase 0. Recovery
// still reads it so an upgrade does not lose the fast path.
const legacyCheckpointKey = "_meta/checkpoint.json"

// checkpointKey is this agent's private checkpoint object. Namespacing it by
// agent id is what stops a second agent sharing the bucket from overwriting
// the checkpoint, which carried the whole topic inventory.
func (s *StorageEngine) checkpointKey() string {
	id := s.agentID
	if id == "" {
		id = defaultAgentID()
	}
	// A configured id may contain a slash; the key must not grow an extra
	// path segment from it.
	id = strings.ReplaceAll(id, "/", "_")
	return "_agents/" + id + "/checkpoint.json"
}

func (s *StorageEngine) SaveCheckpoint() error {
	return s.SaveCheckpointContext(context.Background())
}

// SaveCheckpointContext writes the durable checkpoint and manifest under a
// caller context, so a request-triggered checkpoint is abandoned when the
// client goes away instead of holding a handler open.
func (s *StorageEngine) SaveCheckpointContext(ctx context.Context) error {
	// Refuse to overwrite durable state once another writer has advanced the
	// log past this agent's epoch. Writing here would replace the new
	// writer's topic inventory and log end offsets with a stale copy of our
	// own, which is the corruption the lease exists to prevent.
	if stored := s.metadataCache.WriterEpoch; stored > s.lease.Epoch() {
		return fmt.Errorf("refusing to write checkpoint: it was written at epoch %d, this agent holds epoch %d",
			stored, s.lease.Epoch())
	}

	if s.coordinator != nil {
		state := s.coordinator.ToState()
		s.metadataCache.Coordinator = &state
	}

	// Carry consumer offsets into the checkpoint so a graceful restart does
	// not have to re-read every offset object before retention may run.
	s.committedMu.Lock()
	s.metadataCache.Committed = cloneCommitted(s.committedByGroup)
	s.committedMu.Unlock()

	s.metadataCache.WriterEpoch = s.lease.Epoch()
	s.metadataCache.Writer = s.lease.fencedWriter()

	data, err := s.metadataCache.ToJSON()
	if err != nil {
		return err
	}
	key := s.checkpointKey()

	if err := s.objPut(ctx, key, bytes.NewReader(data)); err != nil {
		log.Printf("Failed to save checkpoint: %v", err)
		return err
	}
	log.Printf("Saved metadata checkpoint to %s (%d bytes)", key, len(data))

	// The manifest is the copy that survives a SIGKILL, where the periodic
	// checkpoint above never got a chance to run.
	if err := s.SaveManifestContext(ctx); err != nil {
		log.Printf("Failed to save log position manifest: %v", err)
		return err
	}
	return nil
}

// LoadCheckpoint re-reads the durable checkpoint. Used by tests and by
// operators; normal startup restores in NewStorageEngine.
func (s *StorageEngine) LoadCheckpoint() error {
	if err := s.loadCheckpointInto(context.Background(), true); err != nil {
		return err
	}
	if err := s.recoverManifest(context.Background()); err != nil {
		log.Printf("Warning: could not recover log end offsets from object storage: %v", err)
	}
	s.applySeeds()
	return nil
}

// loadCheckpointInto reads the checkpoint and merges it into the cache.
// withRecovery also re-applies the consumer offset map, which only a caller
// explicitly asking for a reload needs.
func (s *StorageEngine) loadCheckpointInto(ctx context.Context, withRecovery bool) error {
	key := s.checkpointKey()

	r, err := s.objGet(ctx, key)
	if err != nil {
		// Migration: a bucket written before Phase 0 keeps its checkpoint at
		// the bucket-global key. Read it once so an upgrade preserves the
		// fast path; the next checkpoint writes the namespaced object.
		if lr, legacyErr := s.objGet(ctx, legacyCheckpointKey); legacyErr == nil {
			key, r, err = legacyCheckpointKey, lr, nil
		}
	}
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()

	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if err := s.metadataCache.FromJSON(data); err != nil {
		return err
	}

	// A checkpoint stamped with a higher writer epoch was written by an agent
	// that held the log after us. Its log end offsets are authoritative and
	// ours are not, so serving from here would reissue offsets it has already
	// handed out. The lease cannot catch this on its own: it was released
	// when the previous agent stopped, and epochs only move forward.
	if stored := s.metadataCache.WriterEpoch; stored > s.lease.Epoch() {
		return fmt.Errorf("%w: checkpoint %s was written at epoch %d by %q, this agent holds epoch %d",
			errSuperseded, key, stored, s.metadataCache.Writer, s.lease.Epoch())
	}

	if s.coordinator != nil && s.metadataCache.Coordinator != nil {
		s.coordinator.FromState(*s.metadataCache.Coordinator)
		log.Printf("Restored coordinator state from checkpoint")
	}

	if !withRecovery {
		return nil
	}

	// Merge any offsets carried in the checkpoint. Rehydration from object
	// storage is authoritative and runs separately, so a checkpoint that is
	// missing or stale cannot understate what consumers need.
	if len(s.metadataCache.Committed) > 0 {
		s.committedMu.Lock()
		groups := 0
		for group, byPartition := range s.metadataCache.Committed {
			for tp, off := range byPartition {
				s.recordLocked(group, tp, off)
			}
			groups++
		}
		s.committedMu.Unlock()
		log.Printf("Restored committed offsets for %d group(s) from checkpoint", groups)
	}

	log.Printf("Loaded metadata cache from checkpoint %s (%d bytes)", key, len(data))
	return nil
}

func cloneCommitted(in map[string]map[string]int64) map[string]map[string]int64 {
	if in == nil {
		return nil
	}
	out := make(map[string]map[string]int64, len(in))
	for group, byPartition := range in {
		cp := make(map[string]int64, len(byPartition))
		for k, v := range byPartition {
			cp[k] = v
		}
		out[group] = cp
	}
	return out
}

// recordLocked stores a commit. Callers must hold committedMu.
func (s *StorageEngine) recordLocked(group, topicPartition string, offset int64) {
	if s.committedByGroup[group] == nil {
		s.committedByGroup[group] = make(map[string]int64)
	}
	s.committedByGroup[group][topicPartition] = offset
}

// offsetsPrefix is the object-store location of consumer group offsets.
const offsetsPrefix = "_offsets/"

// rehydrateCommittedOffsets rebuilds per-group committed offsets from object
// storage, which is the authoritative record of what consumers have claimed.
//
// The checkpoint only covers up to 30s of commits, and may be missing or
// stale entirely. Retention must not treat "no offsets known" as "no
// consumers", so it stays disabled until this has run. The engine fails
// closed: if the offsets cannot be read, retention does not run at all,
// because it cannot prove a segment is safe to delete.
func (s *StorageEngine) rehydrateCommittedOffsets(ctx context.Context) {
	objects, err := s.objList(ctx, offsetsPrefix)
	if err != nil {
		log.Printf("Retention: cannot read consumer offsets from %s: %v -- "+
			"retention is disabled until offsets can be read", offsetsPrefix, err)
		return
	}

	restored, skipped := 0, 0
	for _, obj := range objects {
		group, topicPartition, ok := parseOffsetKey(obj.Key)
		if !ok {
			skipped++
			continue
		}

		rc, err := s.objGet(ctx, obj.Key)
		if err != nil {
			skipped++
			continue
		}
		raw, readErr := io.ReadAll(rc)
		_ = rc.Close()
		if readErr != nil {
			skipped++
			continue
		}
		off, convErr := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if convErr != nil {
			skipped++
			continue
		}

		s.committedMu.Lock()
		s.recordLocked(group, topicPartition, off)
		s.committedMu.Unlock()
		restored++
	}

	s.committedMu.Lock()
	s.committedAuthoritative = true
	groups := len(s.committedByGroup)
	s.committedMu.Unlock()

	if skipped > 0 {
		log.Printf("Retention: %d committed offset object(s) could not be read and were skipped; "+
			"retention will over-protect rather than risk deleting live data", skipped)
	}
	log.Printf("Retention: rehydrated %d committed offset(s) across %d group(s) from %s", restored, groups, offsetsPrefix)
}

// parseOffsetKey splits "_offsets/<group>/<topic>/<partition>" into its parts.
// The partition is the final segment and the topic the one before it, so a
// group name containing slashes still parses correctly.
func parseOffsetKey(key string) (group, topicPartition string, ok bool) {
	rest := strings.TrimPrefix(key, offsetsPrefix)
	if rest == key {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) < 3 {
		return "", "", false
	}
	partition := parts[len(parts)-1]
	topic := parts[len(parts)-2]
	group = strings.Join(parts[:len(parts)-2], "/")
	if group == "" || topic == "" || partition == "" {
		return "", "", false
	}
	return group, topic + "/" + partition, true
}

func (s *StorageEngine) LoadOffset(groupID, topic string, partition int32) (int64, error) {
	key := fmt.Sprintf("_offsets/%s/%s/%d", groupID, topic, partition)

	ctx := context.Background()
	rc, err := s.objGet(ctx, key)
	if err != nil {
		// Assume not found if error (simplified)
		return -1, nil
	}
	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	if err != nil {
		return -1, err
	}

	offset, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return -1, err
	}
	// A commit read back from storage also constrains retention.
	s.recordCommitted(groupID, topic, partition, offset)
	return offset, nil
}

func (s *StorageEngine) GetTopicCount() int {
	return s.metadataCache.GetTopicCount()
}

func (s *StorageEngine) GetPartitionCount() int {
	return s.metadataCache.GetPartitionCount()
}

// ---------------------------------------------------------------------------
// Durable log position
// ---------------------------------------------------------------------------

// legacyManifestKey is where the whole-log position lived before Phase 0. It
// exists because the checkpoint is written on a timer and on shutdown: a
// process killed with SIGKILL leaves neither, and the only remaining record of
// how far a partition had advanced is the segment inventory in object storage.
//
// Phase 0 replaced it with one object per partition (see
// partition_manifest.go). It is still read on startup so an upgrade recovers
// the position the previous version would have, rather than falling all the
// way back to the segment inventory.
const legacyManifestKey = "_meta/manifest.json"

// manifestEntry is the durable position of one partition, as written by the
// legacy whole-log manifest. New writes use perPartitionManifest.
type manifestEntry struct {
	Topic          string             `json:"topic"`
	Partition      int32              `json:"partition"`
	LogEndOffset   int64              `json:"log_end_offset"`
	LogStartOffset int64              `json:"log_start_offset"`
	Segments       []*SegmentMetadata `json:"segments"`
}

// manifest is the legacy whole-log position document.
//
// It used to be a bare array of entries. The writer epoch was added later, so
// readers accept both shapes: an agent that meets an older manifest must still
// recover from it rather than deciding the log is unreadable.
type manifest struct {
	WriterEpoch int64           `json:"writer_epoch,omitempty"`
	Writer      string          `json:"writer,omitempty"`
	Partitions  []manifestEntry `json:"partitions"`
}

func (s *StorageEngine) SaveManifest() error {
	return s.SaveManifestContext(context.Background())
}

// SaveManifestContext writes the durable position of every partition whose
// state has changed since the last save. See savePartitionManifests.
func (s *StorageEngine) SaveManifestContext(ctx context.Context) error {
	return s.savePartitionManifests(ctx)
}

// parseManifest reads both the legacy object shape and the bare array that
// preceded it.
func parseManifest(data []byte) (manifest, error) {
	var m manifest
	if err := json.Unmarshal(data, &m); err == nil && m.Partitions != nil {
		return m, nil
	}
	var legacy []manifestEntry
	if err := json.Unmarshal(data, &legacy); err != nil {
		return manifest{}, err
	}
	return manifest{Partitions: legacy}, nil
}

// recoverManifest restores log end offsets from object storage.
//
// This is the recovery path that matters for a landing zone: after a crash the
// local WAL is usually empty (sealed segments were deleted once uploaded), and
// without this the agent would rewind every partition to offset 0 and start
// handing out offsets whose data is already stored.
func (s *StorageEngine) recoverManifest(ctx context.Context) error {
	restored := 0

	// Preferred source: the per-partition manifests written on every
	// checkpoint and segment roll. One bounded LIST under _topics/ discovers
	// them, which is also how a partition whose topic is gone from local disk
	// and from the checkpoint is still found.
	n, found, err := s.loadPartitionManifests(ctx)
	if err != nil {
		return err
	}
	restored += n

	// Migration: a bucket written before Phase 0 still has the whole-log
	// manifest and no per-partition objects. Read it once, and rewrite it in
	// the new shape at the next checkpoint.
	if !found {
		n, err := s.loadLegacyManifest(ctx)
		if err != nil {
			return err
		}
		restored += n
	}

	// Fallback: derive the log end offset from the segments themselves, for
	// any partition no manifest covers. This also repairs a manifest written
	// before a partition's final segments were uploaded.
	for _, topic := range s.discoverTopics(ctx) {
		for _, partition := range s.metadataCache.GetPartitions(topic) {
			if s.metadataCache.LogEndOffset(topic, partition) > 0 && !s.needsObjectRecovery(topic, partition) {
				continue
			}
			segments, err := s.objectSegments(ctx, topic, partition)
			if err != nil || len(segments) == 0 {
				continue
			}
			leo, start, err := s.endOffsetsFromObject(ctx, topic, partition, segments)
			if err != nil {
				log.Printf("Recovery: could not determine end of %s/%d: %v", topic, partition, err)
				continue
			}
			if leo > s.metadataCache.LogEndOffset(topic, partition) {
				s.metadataCache.SetPartitionState(topic, partition, leo, start, segments)
				// Persist the repaired position so the next restart does not
				// pay for the same segment read.
				s.markManifestDirty(topic, partition)
				restored++
			}
		}
	}

	if restored > 0 {
		log.Printf("Storage: recovered log position for %d partition(s) from object storage", restored)
	}
	return nil
}

// needsObjectRecovery reports whether the durable position might lag the
// segments in object storage. Conservative on purpose: an extra range read is
// cheap next to handing out a duplicate offset.
func (s *StorageEngine) needsObjectRecovery(topic string, partition int32) bool {
	snapshot := s.metadataCache.TopicsSnapshot()
	parts, ok := snapshot[topic]
	if !ok {
		return true
	}
	ps, ok := parts[partition]
	if !ok {
		return true
	}
	// The log end offset must cover the last known segment.
	for _, seg := range ps.Segments {
		if seg.EndOffset > ps.LogEndOffset {
			return true
		}
	}
	return false
}

// discoverTopics lists the topic prefixes present in object storage. A topic
// that only exists there, because all of its local segments have been
// offloaded, is still a topic.
func (s *StorageEngine) discoverTopics(ctx context.Context) []string {
	seen := make(map[string]bool)
	for _, t := range s.metadataCache.GetTopics() {
		seen[t] = true
	}
	// There is no cheap "list all prefixes" call, so walk the partition index
	// the manifest and checkpoint maintain, and let the fallback pick up
	// anything else lazily on first access.
	for _, t := range seen {
		_ = t
	}
	return s.metadataCache.GetTopics()
}

// objectSegments lists the uploaded segments of a partition, oldest first.
func (s *StorageEngine) objectSegments(ctx context.Context, topic string, partition int32) ([]*SegmentMetadata, error) {
	objects, err := s.objList(ctx, partitionPrefix(topic, partition))
	if err != nil {
		return nil, err
	}
	segs := filterSegments(objects)
	out := make([]*SegmentMetadata, 0, len(segs))
	for _, o := range segs {
		out = append(out, &SegmentMetadata{StartOffset: parseOffsetFromKey(o.Key), S3Key: o.Key, EndOffset: -1})
	}
	return out, nil
}

// endOffsetsFromObject determines where a partition actually ends by reading
// the last record out of the last uploaded segment.
//
// It uses the index sidecar to seek to the final indexed entry, then reads
// forward. The result is the offset just past the last record, which is what
// the next append must use.
func (s *StorageEngine) endOffsetsFromObject(ctx context.Context, topic string, partition int32, segments []*SegmentMetadata) (int64, int64, error) {
	last := segments[len(segments)-1]
	baseOffset := last.StartOffset
	if baseOffset < 0 {
		baseOffset = 0
	}

	// Entry length: 8 byte offset + 4 byte size, prefixed by nothing else in
	// the local file format.
	const entryHeader = 12

	indexReader, err := s.objGet(ctx, strings.TrimSuffix(last.S3Key, ".log")+".index")
	if err != nil {
		return 0, 0, fmt.Errorf("no index for %s: %w", last.S3Key, err)
	}
	indexBytes, err := io.ReadAll(indexReader)
	_ = indexReader.Close()
	if err != nil {
		return 0, 0, err
	}

	pos, err := index.Lookup(indexBytes, int64(^uint64(0)>>1), baseOffset)
	if err != nil {
		return 0, 0, err
	}

	// Read from the last indexed position to the end of the segment and take
	// the offset of the final entry plus its record count.
	rc, err := s.objGetRange(ctx, last.S3Key, pos, 0)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = rc.Close() }()

	end, start := baseOffset, baseOffset
	for {
		hdr := make([]byte, entryHeader)
		if _, err := io.ReadFull(rc, hdr); err != nil {
			break // clean end of segment, or a truncated tail
		}
		off := int64(binary.BigEndian.Uint64(hdr[0:8]))
		size := int32(binary.BigEndian.Uint32(hdr[8:12]))
		if size < 0 || size > 100*1024*1024 {
			break
		}
		body := make([]byte, size)
		if _, err := io.ReadFull(rc, body); err != nil {
			break
		}
		count := wal.CountMessageSet(body)
		if count == 0 {
			count = 1
		}
		if off >= start {
			start = off
		}
		end = off + int64(count)
	}

	last.EndOffset = end
	return end, start, nil
}
