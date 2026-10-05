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

package protocol

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"kimistore/internal/coordinator"
	"kimistore/internal/metrics"
	"kimistore/internal/storage"
)

// ... constants ...
const (
	ApiKeyProduce          = 0
	ApiKeyFetch            = 1
	ApiKeyListOffsets      = 2
	ApiKeyMetadata         = 3
	ApiKeyOffsetCommit     = 8
	ApiKeyOffsetFetch      = 9
	ApiKeyFindCoordinator  = 10
	ApiKeyJoinGroup        = 11
	ApiKeyHeartbeat        = 12
	ApiKeyLeaveGroup       = 13
	ApiKeySyncGroup        = 14
	ApiKeyDescribeGroups   = 15
	ApiKeyListGroups       = 16
	ApiKeySaslHandshake    = 17
	ApiKeyApiVersions      = 18
	ApiKeyCreateTopics     = 19
	ApiKeyDeleteTopics     = 20
	ApiKeySaslAuthenticate = 36
)

const (
	ErrNone                       = 0
	ErrUnknown                    = -1
	ErrUnknownTopicOrPartition    = 3
	ErrNotLeaderForPartition      = 6
	ErrRequestTimedOut            = 7
	ErrGroupAuthorizationFailed   = 30
	ErrClusterAuthorizationFailed = 31
	ErrUnsupportedVersion         = 35
	ErrTopicAlreadyExists         = 36
	ErrSaslAuthenticationFailed   = 58
	ErrUnsupportedSaslMechanism   = 33
	ErrIllegalSaslState           = 34
	ErrOffsetOutOfRange           = 1
	ErrUnknownTopicOrPartitionV0  = 3
)

type Session struct {
	Authenticated bool
	User          string
}

// ServerConfig is the per-broker configuration the protocol layer needs.
// The advertised address in particular has to be configurable: it is what
// clients dial, and a hardcoded loopback address makes the broker unreachable
// from anywhere else.
type ServerConfig struct {
	// Auth, when Username is non-empty, requires SASL/PLAIN.
	Auth AuthConfig

	// AdvertisedHost and AdvertisedPort are reported in Metadata and
	// FindCoordinator.
	AdvertisedHost string
	AdvertisedPort int32

	// AutoCreateTopics mirrors Kafka's auto.create.topics.enable: a Metadata
	// request naming an unknown topic creates it.
	AutoCreateTopics     bool
	AutoCreatePartitions int32
}

type AuthConfig struct {
	Username string
	Password string
}

// AdvertisedVersions renders the version table for the startup log, one line
// per API, so the ceilings are visible in the agent log and not only in the
// documentation.
func AdvertisedVersions(saslConfigured bool) string {
	keys := make([]int, 0, len(supportedAPIVersions))
	for k := range supportedAPIVersions {
		if !saslConfigured && (k == ApiKeySaslHandshake || k == ApiKeySaslAuthenticate) {
			continue
		}
		keys = append(keys, int(k))
	}
	sort.Ints(keys)

	var b strings.Builder
	for _, k := range keys {
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=0-%d", apiName(int16(k)), supportedAPIVersions[int16(k)])
	}
	return b.String()
}

// VersionCeilingReason explains, for the APIs whose ceiling is lower than a
// client might expect, why it is there. Clients that hard-code a version list
// fail with a message naming only the API key, so the reason belongs somewhere
// the operator will actually see it.
func VersionCeilingReason(apiKey int16) string {
	switch apiKey {
	case ApiKeyProduce:
		return "v3, the newest non-flexible version. Below v3 clients fall back to magic-1 " +
			"records, which have no header field, so every Kafka record header a producer sets " +
			"is dropped. Grafana Mimir keeps each write's wire format in a record header and " +
			"ingests every record as the wrong version without it."
	case ApiKeyMetadata:
		return "v6, the newest version that is not flexible."
	case ApiKeyFetch:
		return "v5, which adds log_start_offset so a client can find the log start without a " +
			"separate ListOffsets round trip."
	}
	return ""
}

// DefaultServerConfig is used by tests and by any caller that does not supply
// one.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		AdvertisedHost:       "localhost",
		AdvertisedPort:       19092,
		AutoCreateTopics:     true,
		AutoCreatePartitions: 1,
	}
}

// supportedAPIVersions is the single source of truth for what this broker
// implements. ApiVersions advertises from it and the dispatcher refuses from
// it, so the two can no longer disagree -- a broker that advertises a version
// it then mis-parses is worse than one that never offers it.
// supportedAPIVersions is the single source of truth for what this broker
// implements. ApiVersions advertises from it and the dispatcher refuses from
// it, so the two cannot disagree -- a broker that advertises a version it then
// mis-parses is worse than one that never offers it.
//
// Produce is offered up to v3 and no further, for a reason that has nothing to
// do with response layout. Produce v3 is where magic 2 became legal, and magic
// 2 is the record batch format. Offer a client only v0-v2 and it falls back
// to magic 1, which has no header field, so every Kafka record header the
// producer set is silently discarded. Grafana Mimir carries the wire format of
// each write in a record header; capping Produce at v0 makes it ingest every
// record as the wrong version and fail to parse it. v3 is the newest Produce
// version before the flexible (tagged-field) encoding, which is not
// implemented here.
//
// Heartbeat and LeaveGroup stop at v0 because the target client decodes those
// two with ErrorCode before ThrottleTimeMs, the reverse of the schema.
var supportedAPIVersions = map[int16]int16{
	ApiKeyProduce:          3,
	ApiKeyFetch:            5,
	ApiKeyListOffsets:      2,
	ApiKeyMetadata:         6,
	ApiKeyApiVersions:      0,
	ApiKeyOffsetCommit:     0,
	ApiKeyOffsetFetch:      1,
	ApiKeyFindCoordinator:  0,
	ApiKeyJoinGroup:        1,
	ApiKeySyncGroup:        0,
	ApiKeyHeartbeat:        0,
	ApiKeyLeaveGroup:       0,
	ApiKeyCreateTopics:     0,
	ApiKeyDeleteTopics:     0,
	ApiKeyListGroups:       0,
	ApiKeyDescribeGroups:   0,
	ApiKeySaslHandshake:    1,
	ApiKeySaslAuthenticate: 0,
}

var apiNames = map[int16]string{
	ApiKeyProduce: "Produce", ApiKeyFetch: "Fetch", ApiKeyListOffsets: "ListOffsets",
	ApiKeyMetadata: "Metadata", ApiKeyApiVersions: "ApiVersions", ApiKeyOffsetCommit: "OffsetCommit",
	ApiKeyOffsetFetch: "OffsetFetch", ApiKeyFindCoordinator: "FindCoordinator", ApiKeyJoinGroup: "JoinGroup",
	ApiKeySyncGroup: "SyncGroup", ApiKeyHeartbeat: "Heartbeat", ApiKeyLeaveGroup: "LeaveGroup",
	ApiKeyCreateTopics: "CreateTopics", ApiKeyDeleteTopics: "DeleteTopics", ApiKeyListGroups: "ListGroups",
	ApiKeyDescribeGroups: "DescribeGroups", ApiKeySaslHandshake: "SaslHandshake", ApiKeySaslAuthenticate: "SaslAuthenticate",
}

// ApiName is the human-readable name of an API key.
func ApiName(k int16) string { return apiName(k) }

func apiName(k int16) string {
	if n, ok := apiNames[k]; ok {
		return n
	}
	return fmt.Sprintf("api-%d", k)
}

// arrayFirstResponses are the APIs whose v0 response begins with an array
// rather than an error code. Getting this wrong turns an UNSUPPORTED_VERSION
// into a decode failure on the client, so the shape has to be picked per API.
// clusterID is the identifier this broker reports. It has no meaning of its
// own; it only has to be stable.
const clusterID = "kimistore-cluster"

var arrayFirstResponses = map[int16]bool{
	ApiKeyProduce:      true,
	ApiKeyMetadata:     true,
	ApiKeyCreateTopics: true,
	ApiKeyDeleteTopics: true,
}

// throttleFirstVersions lists, per API, the first response version that gained
// a leading ThrottleTimeMs. A refusal has to be shaped like a real response of
// the version that was asked for, or the client cannot parse it.
var throttleFirstVersions = map[int16]int16{
	ApiKeyProduce:      1,
	ApiKeyMetadata:     3,
	ApiKeyCreateTopics: 2,
	ApiKeyDeleteTopics: 1,
}

// unsupportedVersionResponse builds an UNSUPPORTED_VERSION reply shaped like a
// real response of the requested version, so the client can parse the refusal
// instead of discarding the connection.
//
// The correlation ID has already been written by HandleRequest, so this appends
// only the body.
func unsupportedVersionResponse(enc *Encoder, apiKey int16, version int16) []byte {
	if first, hasThrottle := throttleFirstVersions[apiKey]; hasThrottle && version >= first {
		enc.Int32(0) // ThrottleTimeMs
	}
	if arrayFirstResponses[apiKey] {
		// The v0 layout of these responses is an array with no error code, so
		// an empty array is the only way to say "nothing here".
		enc.Int32(0)
		return enc.Bytes()
	}
	enc.Int16(ErrUnsupportedVersion)
	// Most responses carry an array or partition list after the code. An empty
	// one keeps the framing valid without claiming any data.
	enc.Int32(0)
	return enc.Bytes()
}

// orBackground substitutes a root context for a nil one.
func orBackground(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

// HandleRequest decodes, dispatches and encodes one request.
//
// ctx belongs to the connection, not the individual request: it is cancelled
// when the client goes away, so a long-poll or an object-store read stops
// costing resources the moment nobody is left to receive the answer.
func HandleRequest(ctx context.Context, data []byte, store *storage.StorageEngine, session *Session, cfg ServerConfig) ([]byte, error) {
	// A nil context is a caller bug, but recovering from it turns a panic
	// into a working request. contextcheck reads this as a discarded
	// context; there is no parent to inherit when the input is nil.
	ctx = orBackground(ctx)
	dec := NewDecoder(data)

	// Parse Header
	apiKey, err := dec.Int16()
	if err != nil {
		return nil, err
	}
	apiVersion, err := dec.Int16()
	if err != nil {
		return nil, err
	}
	correlationID, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	clientID, err := dec.String()
	if err != nil {
		// Start of V1 header has ClientID. earlier V0 had it too?
		// Kafka 0.9+ has ClientID in header.
		// For very old versions it might differ, but let's assume standard header.
		return nil, err
	}

	// Silence high-volume requests like Fetch (1)
	if apiKey != ApiKeyFetch {
		log.Printf("Request: ApiKey=%d Version=%d CorrelationID=%d ClientID=%s",
			apiKey, apiVersion, correlationID, clientID)
	}

	enc := NewEncoder()
	// Response Header: CorrelationID
	enc.Int32(correlationID)

	// Check Authentication
	// If auth is configured, we only allow ApiVersions, SaslHandshake, SaslAuthenticate
	// OR if session is authenticated.
	authRequired := cfg.Auth.Username != ""
	isAuthRelated := apiKey == ApiKeySaslHandshake || apiKey == ApiKeySaslAuthenticate || apiKey == ApiKeyApiVersions

	if authRequired && !session.Authenticated && !isAuthRelated {
		log.Printf("Unauthenticated access attempt: ApiKey=%d ClientID=%s", apiKey, clientID)
		// Return appropriate error.
		// For many APIs, ClusterAuthorizationFailed or GroupAuthorizationFailed is appropriate.
		// NOTE: Some clients might strictly expect standard ErrorCode in response body.
		// We try to handle it.
		// But first, switch on ApiKey to dispatch correctly, but implement auth check inside or here?
		// If we do it here, we must know the response format.
		// Most responses start with ErrorCode (int16) exceptions:
		// - Produce: Array
		// - Metadata: Array
		// - etc.
		// Simplest for now: Let specific handlers check?
		// Or generic failure with best guess?
		// Let's create a generic "AuthFailed" responder?
		// No, it's complex because every response schema is different.
		// Strategy: Pass session/auth to handlers or check at top of each case?
		// Better: We check here and return a specific "Auth Error" for known schemas.
		// For MVP, if we return error from HandleRequest, Server just closes connection?
		// That is arguably safer for unauth access.
		// BUT standard Kafka clients might retry infinitely if connection closes without error.
		// Let's implement a 'handleAuthFailure' helper or just close connection for now.
		return nil, fmt.Errorf("authentication required")
	}

	// Metrics: Start Timer
	startTime := time.Now()
	var errorCode int16 = ErrNone

	var resp []byte
	var errProc error

	// An API this broker does not implement, or a version of one it does not
	// implement, gets UNSUPPORTED_VERSION -- not a dropped connection. The
	// old behaviour closed the socket, which turned a single unexpected
	// request into a reconnect loop with the client retrying the same request.
	if maxV, known := supportedAPIVersions[apiKey]; !known {
		log.Printf("Unsupported API Key: %d (version %d) from %s", apiKey, apiVersion, clientID)
		metrics.UnsupportedAPIVersions.WithLabelValues(apiName(apiKey), strconv.Itoa(int(apiVersion))).Inc()
		resp = unsupportedVersionResponse(enc, apiKey, apiVersion)
		errorCode = ErrUnsupportedVersion
		metrics.ObserveRequest(apiKey, apiVersion, errorCode, startTime, len(resp))
		return resp, nil
	} else if apiVersion > maxV {
		// A client that read ApiVersions would not get here, so this is worth
		// naming out loud: some client libraries hard-code the versions they
		// will use rather than negotiating, and when one of them meets a
		// ceiling it fails with a version-negotiation error naming the API
		// key rather than anything the broker can see.
		log.Printf("Refused %s v%d from %s: this broker implements up to v%d", apiName(apiKey), apiVersion, clientID, maxV)
		metrics.UnsupportedAPIVersions.WithLabelValues(apiName(apiKey), strconv.Itoa(int(apiVersion))).Inc()
		resp = unsupportedVersionResponse(enc, apiKey, apiVersion)
		errorCode = ErrUnsupportedVersion
		metrics.ObserveRequest(apiKey, apiVersion, errorCode, startTime, len(resp))
		return resp, nil
	}

	switch apiKey {
	case ApiKeySaslHandshake:
		resp, errProc = handleSaslHandshake(dec, enc, apiVersion)
	case ApiKeySaslAuthenticate:
		resp, errProc = handleSaslAuthenticate(dec, enc, apiVersion, session, cfg)
	case ApiKeyProduce:
		resp, errProc = handleProduce(ctx, dec, enc, store, apiVersion, cfg)
	case ApiKeyFetch:
		resp, errProc = handleFetch(ctx, dec, enc, store, apiVersion)
	case ApiKeyListOffsets:
		resp, errProc = handleListOffsets(dec, enc, store, apiVersion)
	case ApiKeyApiVersions:
		resp, errProc = handleApiVersions(dec, enc, apiVersion, cfg)
	case ApiKeyMetadata:
		resp, errProc = handleMetadata(ctx, dec, enc, store, apiVersion, cfg)
	case ApiKeyFindCoordinator:
		resp, errProc = handleFindCoordinator(dec, enc, apiVersion, cfg)
	case ApiKeyJoinGroup:
		resp, errProc = handleJoinGroup(dec, enc, apiVersion)
	case ApiKeySyncGroup:
		resp, errProc = handleSyncGroup(dec, enc, apiVersion)
	case ApiKeyHeartbeat:
		resp, errProc = handleHeartbeat(dec, enc, apiVersion)
	case ApiKeyLeaveGroup:
		resp, errProc = handleLeaveGroup(dec, enc, apiVersion)
	case ApiKeyOffsetCommit:
		resp, errProc = handleOffsetCommit(dec, enc, store, apiVersion)
	case ApiKeyOffsetFetch:
		resp, errProc = handleOffsetFetch(dec, enc, store, apiVersion)
	case ApiKeyCreateTopics:
		resp, errProc = handleCreateTopics(ctx, dec, enc, store, apiVersion)
	case ApiKeyDeleteTopics:
		resp, errProc = handleDeleteTopics(ctx, dec, enc, store, apiVersion)
	case ApiKeyListGroups:
		resp, errProc = handleListGroups(dec, enc, store, apiVersion)
	case ApiKeyDescribeGroups:
		resp, errProc = handleDescribeGroups(dec, enc, store, apiVersion)
	default:
		errProc = fmt.Errorf("unsupported api key: %d", apiKey)
	}

	if errProc != nil {
		errorCode = ErrUnknown
	}

	// Metrics: Observe
	metrics.ObserveRequest(apiKey, apiVersion, errorCode, startTime, len(resp))

	return resp, errProc
}

// ----------------------------------------------------------------------
// Group Coordinator Handlers (Stubs for MVP)
// ----------------------------------------------------------------------

var GlobalCoordinator = coordinator.NewCoordinator()

func handleFindCoordinator(dec *Decoder, enc *Encoder, version int16, cfg ServerConfig) ([]byte, error) {
	// FindCoordinator Request V0:
	// GroupID (string)

	groupID, err := dec.String()
	if err != nil {
		return nil, err
	}
	log.Printf("FindCoordinator: GroupID=%s", groupID)

	// V1 adds KeyType (int8) and V2 adds an error message.
	if version >= 1 {
		if _, err := dec.Int8(); err != nil {
			return nil, err
		}
	}

	// FindCoordinator Response:
	//   V0: ErrorCode | NodeID | Host | Port
	//   V1: ThrottleTimeMs | ErrorCode | ErrorMessage | NodeID | Host | Port
	if version >= 1 {
		enc.Int32(0) // ThrottleTimeMs
	}
	enc.Int16(ErrNone)
	if version >= 1 {
		enc.String("") // ErrorMessage
	}
	enc.Int32(0) // NodeID: this agent is the only coordinator
	enc.String(cfg.AdvertisedHost)
	enc.Int32(cfg.AdvertisedPort)

	return enc.Bytes(), nil
}

func handleJoinGroup(dec *Decoder, enc *Encoder, version int16) ([]byte, error) {
	// JoinGroup Request V0:
	groupID, _ := dec.String()
	sessionTimeout, _ := dec.Int32()

	var rebalanceTimeout int32
	if version >= 1 {
		rebalanceTimeout, _ = dec.Int32()
	} else {
		rebalanceTimeout = sessionTimeout
	}

	memberID, _ := dec.String()
	protocolType, _ := dec.String()

	// Parse Protocols Array
	count, _ := dec.Int32()
	var protocols []coordinator.GroupProtocol
	for i := 0; i < int(count); i++ {
		name, _ := dec.String()
		meta, _ := dec.Bytes()
		protocols = append(protocols, coordinator.GroupProtocol{Name: name, Metadata: meta})
	}

	log.Printf("JoinGroup: Group=%s Member=%s ProtocolType=%s RebalanceTimeout=%d", groupID, memberID, protocolType, rebalanceTimeout)

	// Call Coordinator
	newMemberID, generationID, leaderID, members, err := GlobalCoordinator.JoinGroup(groupID, memberID, protocolType, protocols, sessionTimeout, rebalanceTimeout)

	errorCode := int16(ErrNone)
	if err != nil {
		errorCode = ErrGroupAuthorizationFailed
	}
	// JoinGroup Response V0
	enc.Int16(errorCode)
	enc.Int32(generationID)
	selected := GlobalCoordinator.SelectedProtocol(groupID)
	if selected == "" && len(protocols) > 0 {
		selected = protocols[0].Name
	}

	// Kafka returns the group's *selected* protocol, and every member's
	// metadata for that same protocol. Returning each member's own first
	// protocol instead would hand the leader the wrong metadata whenever
	// members disagree on ordering, and it would pick a balancer the group
	// did not settle on.
	enc.String(selected)
	enc.String(leaderID)
	enc.String(newMemberID)

	enc.Int32(int32(len(members)))
	for _, m := range members {
		enc.String(m.MemberID)
		var meta []byte
		for _, p := range m.Protocols {
			if p.Name == selected {
				meta = p.Metadata
				break
			}
		}
		enc.PutBytes(meta)
	}

	return enc.Bytes(), nil
}

func handleSyncGroup(dec *Decoder, enc *Encoder, version int16) ([]byte, error) {
	// SyncGroup Request V0
	groupID, _ := dec.String()
	generationID, _ := dec.Int32()
	memberID, _ := dec.String()

	count, _ := dec.Int32()
	var assignments []coordinator.GroupAssignment
	for i := 0; i < int(count); i++ {
		mID, _ := dec.String()
		assignBytes, _ := dec.Bytes()
		assignments = append(assignments, coordinator.GroupAssignment{MemberID: mID, Assignment: assignBytes})
	}

	log.Printf("SyncGroup: Group=%s Member=%s Gen=%d", groupID, memberID, generationID)

	myAssignment, err := GlobalCoordinator.SyncGroup(groupID, memberID, generationID, assignments)

	errorCode := int16(ErrNone)
	if err != nil {
		log.Printf("SyncGroup Error: %v", err)
		// Map coordinator outcomes onto the Kafka codes clients expect. The
		// distinction matters: RebalanceInProgress tells the client to rejoin
		// immediately, whereas UnknownMemberId makes it discard its member ID.
		switch err {
		case coordinator.ErrRebalanceInProgress:
			errorCode = 27 // RebalanceInProgress
		case coordinator.ErrMemberNotFound:
			errorCode = 25 // UnknownMemberId
		case coordinator.ErrSyncTimeout:
			// 80 = UnstableOffsetCommitPhase in newer protocols; for the V0/V1
			// range a RebalanceInProgress is the closest honest answer and
			// makes the client back off and rejoin.
			errorCode = 27
		default:
			errorCode = 25
		}
	}

	// SyncGroup Response V0
	enc.Int16(errorCode)
	enc.PutBytes(myAssignment)

	return enc.Bytes(), nil
}

func handleHeartbeat(dec *Decoder, enc *Encoder, version int16) ([]byte, error) {
	groupID, _ := dec.String()
	generationID, _ := dec.Int32()
	memberID, _ := dec.String()

	err := GlobalCoordinator.Heartbeat(groupID, memberID, generationID)
	errorCode := int16(ErrNone)
	if err != nil {
		// A member the reaper has already evicted should be told to rejoin
		// rather than simply "in progress", so its client regenerates state
		// and picks up a fresh assignment.
		if errors.Is(err, coordinator.ErrMemberNotFound) {
			errorCode = 25 // UnknownMemberId
		} else {
			errorCode = 27 // RebalanceInProgress
		}
	}

	enc.Int16(errorCode)
	return enc.Bytes(), nil
}

func handleLeaveGroup(dec *Decoder, enc *Encoder, version int16) ([]byte, error) {
	groupID, _ := dec.String()
	memberID, _ := dec.String()

	// Leaving is best-effort: a client may leave after the reaper has already
	// evicted it, and that is not an error worth failing the request over.
	if err := GlobalCoordinator.LeaveGroup(groupID, memberID); err != nil {
		log.Printf("LeaveGroup: %s/%s: %v", groupID, memberID, err)
	}

	enc.Int16(ErrNone)
	return enc.Bytes(), nil
}

func handleOffsetCommit(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	groupID, _ := dec.String()
	topicCount, _ := dec.Int32()

	// We need to buffer the response structure to write it after processing
	// Response: TopicArray [TopicName, PartitionArray [PartitionID, ErrorCode]]

	// To simplify: We process and write immediately.
	// NOTE: If request parsing fails, we might produce partial response.

	// Wait, we need to read ALL request first?
	// Usually Handler decodes then acts then encodes.
	// For MVP we can stream read/write if strict order.

	// BUT `enc` is append-only.
	// V0 Request: GroupID, TopicArray...
	// V0 Response: TopicArray...

	enc.Int32(topicCount)

	for i := int32(0); i < topicCount; i++ {
		topic, _ := dec.String()
		enc.String(topic)

		partCount, _ := dec.Int32()
		enc.Int32(partCount)

		for j := int32(0); j < partCount; j++ {
			partition, _ := dec.Int32()
			offset, _ := dec.Int64()
			_, _ = dec.String() // Metadata

			// SAVE OFFSET
			err := GlobalCoordinator.CommitOffset(store, groupID, topic, partition, offset)

			enc.Int32(partition)
			if err != nil {
				log.Printf("Error committing offset group=%s topic=%s part=%d off=%d: %v", groupID, topic, partition, offset, err)
				enc.Int16(ErrUnknown)
			} else {
				enc.Int16(ErrNone)
			}
		}
	}

	return enc.Bytes(), nil
}

// handleOffsetFetch returns committed offsets for a group.
//
// v0 is a bare group id and answers with a flat partition list; v1 adds a
// topic list to the request and answers with a topic-nested structure. The
// request version is not optional here: parsing a v0 request as if it were v1
// reads a topic count out of whatever bytes follow, and answers with a
// structure the client cannot decode.
func handleOffsetFetch(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	groupID, err := dec.String()
	if err != nil {
		return nil, err
	}

	if version == 0 {
		// Response V0: offsets[] of (partition, offset, metadata, error)
		enc.Int32(0)
		return enc.Bytes(), nil
	}

	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	log.Printf("OffsetFetch: Group=%s Count=%d", groupID, count)

	// Response V1: topics[] of (name, partitions[] of
	//              (partition, offset, metadata, error))
	enc.Int32(count)

	for i := 0; i < int(count); i++ {
		topic, err := dec.String()
		if err != nil {
			return nil, err
		}
		enc.String(topic)

		partitionCount, err := dec.Int32()
		if err != nil {
			return nil, err
		}
		enc.Int32(partitionCount)

		for j := 0; j < int(partitionCount); j++ {
			partition, err := dec.Int32()
			if err != nil {
				return nil, err
			}

			offset, err := GlobalCoordinator.FetchOffset(store, groupID, topic, partition)
			if err != nil || offset < 0 {
				// -1 is the protocol's "no committed offset", which is what
				// tells the consumer to apply auto.offset.reset.
				offset = -1
			}

			enc.Int32(partition)
			enc.Int64(offset)
			enc.String("") // Metadata
			enc.Int16(ErrNone)
		}
	}

	return enc.Bytes(), nil
}

// handleApiVersions answers the negotiation request.
//
// Only v0 is answered. A flexible-version client (v3+) expects a throttle
// field and compact strings, so a v0-shaped body is not decodable; replying
// UNSUPPORTED_VERSION in the v0 shape is the conventional way to make a client
// retry at v0, and that is what librdkafka does.
func handleApiVersions(dec *Decoder, enc *Encoder, version int16, cfg ServerConfig) ([]byte, error) {
	if version > 0 {
		enc.Int16(ErrUnsupportedVersion)
		enc.Int32(0) // empty array
		return enc.Bytes(), nil
	}

	// Response V0: ErrorCode (int16) | ApiKeys (array of key/min/max)
	enc.Int16(ErrNone)

	entries := make([][3]int16, 0, len(supportedAPIVersions))
	for key, maxV := range supportedAPIVersions {
		// Advertising SASL when no mechanism is configured invites clients to
		// attempt a handshake against a broker with nothing to offer.
		if !cfg.Auth.Required() && (key == ApiKeySaslHandshake || key == ApiKeySaslAuthenticate) {
			continue
		}
		entries = append(entries, [3]int16{key, 0, maxV})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i][0] < entries[j][0] })

	enc.Int32(int32(len(entries)))
	for _, e := range entries {
		enc.Int16(e[0])
		enc.Int16(e[1])
		enc.Int16(e[2])
	}

	return enc.Bytes(), nil
}

func handleMetadata(ctx context.Context, dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16, cfg ServerConfig) ([]byte, error) {
	// Metadata Request V0-V6:
	//   V0-V5: Topics (array of string). An empty array means "all topics".
	//   V6:    adds allow_auto_topic_creation (boolean, one byte).
	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	var requestedTopics []string
	for i := 0; i < int(count); i++ {
		t, err := dec.String()
		if err != nil {
			return nil, err
		}
		requestedTopics = append(requestedTopics, t)
	}
	if version >= 6 {
		if _, err := dec.Int8(); err != nil { // allow_auto_topic_creation
			return nil, err
		}
	}

	// Metadata Response, in the order the schema declares its fields:
	//   V0:    Brokers | Topics
	//   V1:    Brokers | ControllerId | Topics
	//   V2:    Brokers | ClusterId | ControllerId | Topics
	//   V3+:   ThrottleTimeMs | Brokers | ClusterId | ControllerId | Topics
	//
	// Kafka serialises fields in declaration order, so this ordering is the
	// contract.

	if version >= 3 {
		enc.Int32(0) // ThrottleTimeMs
	}

	// 1. Brokers
	enc.Int32(1) // Broker Count

	// The advertised address, not the bind address: this is what the client
	// will dial. It has to be reachable from the client, which a loopback
	// address is not.
	enc.Int32(0) // NodeID
	enc.String(cfg.AdvertisedHost)
	enc.Int32(cfg.AdvertisedPort)

	if version >= 1 {
		enc.String("") // Rack
	}

	// cluster_id arrived in Metadata v2 and is nullable from v2 onwards.
	// Emitting it at v1, as an earlier version of this code did, puts four
	// bytes where a v1 client expects the controller id and desynchronises
	// every following field.
	if version >= 2 {
		enc.String(clusterID)
	}

	if version >= 1 {
		enc.Int32(0) // ControllerID
	}

	// 2. Topic Metadata
	allTopics, _ := store.GetTopics()
	topicsToReturn := requestedTopics
	if count <= 0 {
		topicsToReturn = allTopics
	} else if cfg.AutoCreateTopics {
		// Kafka creates a topic when a client asks about one that does not
		// exist, and clients depend on it: a producer needs partitions with a
		// known leader before it can write anything, and without this it
		// fails with "unknown partition leader" rather than producing.
		for _, tName := range requestedTopics {
			if store.TopicExists(tName) {
				continue
			}
			partitions := cfg.AutoCreatePartitions
			if partitions < 1 {
				partitions = 1
			}
			if err := store.CreateTopicContext(ctx, tName, partitions); err != nil {
				log.Printf("Metadata: could not auto-create topic %q: %v", tName, err)
			} else {
				log.Printf("Metadata: auto-created topic %q with %d partition(s)", tName, partitions)
			}
		}
	}
	if len(topicsToReturn) == 0 {
		// Nothing known yet. An empty topic list is a valid answer; inventing
		// placeholder topics would hand clients partitions that do not exist.
		topicsToReturn = nil
	}

	enc.Int32(int32(len(topicsToReturn)))

	for _, tName := range topicsToReturn {
		enc.Int16(0) // TopicErrorCode
		enc.String(tName)
		if version >= 1 {
			enc.Int8(0) // IsInternal
		}

		partitions, err := store.GetPartitions(tName)
		if err != nil {
			partitions = nil
		}
		if len(partitions) == 0 {
			// An unknown topic has no partitions. Reporting a synthetic
			// partition 0 would make clients hash onto a partition the broker
			// would then create on first write, outside any assignment.
			enc.Int32(0)
			continue
		}

		enc.Int32(int32(len(partitions)))
		for _, pid := range partitions {
			enc.Int16(0)   // PartitionErrorCode
			enc.Int32(pid) // PartitionID
			enc.Int32(0)   // Leader (Node 0)
			if version >= 7 {
				enc.Int32(0) // LeaderEpoch
			}
			enc.Int32(1) // Replica Count
			enc.Int32(0) // Node 0
			enc.Int32(1) // Isr Count
			enc.Int32(0) // Node 0
			if version >= 5 {
				enc.Int32(0) // OfflineReplicas
			}
		}
	}

	return enc.Bytes(), nil
}

func handleSaslHandshake(dec *Decoder, enc *Encoder, version int16) ([]byte, error) {
	// SaslHandshake Request V0:
	// Mechanism (string)

	// SaslHandshake Request V1:
	// Mechanism (string)

	mech, err := dec.String()
	if err != nil {
		return nil, err
	}
	log.Printf("SaslHandshake: Mechanism=%s Version=%d", mech, version)

	// We only support PLAIN
	if mech == "PLAIN" {
		enc.Int16(ErrNone)
		enc.Int32(1)        // Enabled Mechanisms Array Length
		enc.String("PLAIN") // Mechanism
	} else {
		enc.Int16(ErrUnsupportedSaslMechanism)
		enc.Int32(1)
		enc.String("PLAIN")
	}

	return enc.Bytes(), nil
}

func handleSaslAuthenticate(dec *Decoder, enc *Encoder, version int16, session *Session, cfg ServerConfig) ([]byte, error) {
	// SaslAuthenticate Request V0:
	// AuthBytes (bytes)

	authBytes, err := dec.Bytes()
	if err != nil {
		return nil, err
	}

	// SASL PLAIN format: [AuthorizationID] NULL [AuthenticationID] NULL [Password]
	// We typically ignore AuthorizationID.
	// We expect: \x00 username \x00 password
	// Or: authorized_user \x00 username \x00 password

	parts := make([][]byte, 0)
	last := 0
	for i := 0; i < len(authBytes); i++ {
		if authBytes[i] == 0 {
			parts = append(parts, authBytes[last:i])
			last = i + 1
		}
	}
	parts = append(parts, authBytes[last:])

	var username, password string
	if len(parts) == 3 {
		// normal case
		// parts[0] is authz id (usually empty)
		username = string(parts[1])
		password = string(parts[2])
	} else {
		log.Printf("SaslAuthenticate: Invalid PLAIN payload format. Parts=%d", len(parts))
		enc.Int16(ErrSaslAuthenticationFailed)
		enc.String("Invalid SASL PLAIN payload")
		// SaslAuth Response V0:
		// ErrorCode (int16)
		// ErrorMessage (string)
		// AuthBytes (bytes)
		enc.PutBytes(nil)
		return enc.Bytes(), nil
	}

	if cfg.Auth.Required() && subtle.ConstantTimeCompare([]byte(username), []byte(cfg.Auth.Username)) == 1 &&
		subtle.ConstantTimeCompare([]byte(password), []byte(cfg.Auth.Password)) == 1 {
		session.Authenticated = true
		session.User = username
		enc.Int16(ErrNone)
		enc.String("")    // No error message
		enc.PutBytes(nil) // No auth bytes
	} else {
		// Do not log the attempted username on a failed handshake: this is
		// attacker-controlled input on an unauthenticated path.
		log.Printf("SaslAuthenticate: authentication failed")
		enc.Int16(ErrSaslAuthenticationFailed)
		enc.String("Authentication failed")
		enc.PutBytes(nil)
	}

	return enc.Bytes(), nil
}

// Required reports whether SASL authentication is configured.
func (a AuthConfig) Required() bool { return a.Username != "" }
