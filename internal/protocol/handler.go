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

	"kimistore/internal/auth"
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
	ApiKeyInitProducerID   = 22
	ApiKeySaslAuthenticate = 36
)

const (
	ErrNone                       = 0
	ErrUnknown                    = -1
	ErrUnknownTopicOrPartition    = 3
	ErrLeaderNotAvailable         = 5
	ErrNotLeaderForPartition      = 6
	ErrNotCoordinator             = 16
	ErrCoordinatorNotAvailable    = 15
	ErrIllegalGeneration          = 22
	ErrUnknownMemberID            = 25
	ErrRequestTimedOut            = 7
	ErrOutOfOrderSequence         = 45
	ErrDuplicateSequence          = 46
	ErrInvalidProducerEpoch       = 47
	ErrUnknownProducerID          = 59
	ErrFencedInstanceID           = 78
	ErrTopicAuthorizationFailed   = 29
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

	// saslMechanism is the mechanism the client chose in SaslHandshake.
	//
	// SaslAuthenticate carries no mechanism field, so the server has to carry
	// it from the handshake. Guessing PLAIN, as this did before, would work only
	// because there was only one mechanism to guess; with SCRAM the client's
	// final message would be parsed as the wrong mechanism's and every
	// authentication would fail.
	saslMechanism string
	// scram is the in-flight SCRAM conversation, nil until a SCRAM
	// client-first message arrives. It is per-connection because SCRAM spans
	// several SaslAuthenticate requests.
	scram *auth.Exchange
}

// SASLMechanism returns the mechanism negotiated by SaslHandshake.
func (s *Session) SASLMechanism() string { return s.saslMechanism }

// SCRAM returns the in-flight SCRAM exchange, or nil if none has started.
func (s *Session) SCRAM() *auth.Exchange { return s.scram }

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

	// NodeID is this broker's id, as reported in Metadata and returned by
	// FindCoordinator. It must match the id this agent publishes in its routing
	// record: a client that is told a partition's leader is node 7 then looks
	// node 7 up in the broker list it was just given, so the two have to agree.
	//
	// Zero is a legal id. Storage derives a stable one from the agent id when
	// the operator does not set it, and the registry publishes the same value.
	NodeID int32

	// AutoCreateTopics mirrors Kafka's auto.create.topics.enable: a Metadata
	// request naming an unknown topic creates it.
	AutoCreateTopics     bool
	AutoCreatePartitions int32
}

// AuthConfig is the authentication configuration.
//
// The two halves are deliberately separate. Username/Password is the original
// single shared credential behind SASL_USERNAME and SASL_PASSWORD, and it is
// kept exactly as it was so an existing deployment changes nothing by upgrading.
// Credentials is the per-user SCRAM store; when it is nil, SCRAM is simply not
// offered.
type AuthConfig struct {
	Username string
	Password string

	// Credentials is the SCRAM credential store, or nil when no SCRAM
	// credentials have been created. A nil store is what keeps SCRAM out of the
	// advertised mechanism list rather than advertising it and failing every
	// exchange.
	Credentials *auth.Store

	// SCRAMEnabled says whether SCRAM should be offered at all.
	//
	// It is separate from Credentials being non-nil on purpose. A store can
	// open successfully and hold nothing, and a broker whose SCRAM store is
	// merely reachable is not an authenticated broker. Treating an empty store
	// as "SCRAM configured" makes Required() true, which gates every request on
	// the session being authenticated, and that silently breaks a deployment
	// which has no SASL credentials at all -- it would start rejecting
	// everything. The caller sets this only after confirming credentials exist.
	SCRAMEnabled bool

	// ACLs holds the authorization rules. Nil, or a store with no rules, means
	// no authorization at all and every request is allowed.
	ACLs *auth.ACLStore
}

// Mechanisms returns the SASL mechanisms this broker will serve, in the order
// they should be advertised.
//
// SCRAM is listed before PLAIN when both are available, and PLAIN only if it
// actually has a credential. Advertising a mechanism the broker cannot complete
// is worse than not advertising it: a client that selects it fails after a
// round trip instead of negotiating something else.
//
// SCRAM additionally requires SCRAMEnabled, so an empty credential store cannot
// turn a broker that has no authentication at all into one that demands it.
// See SCRAMEnabled.
func (a AuthConfig) Mechanisms() []string {
	var out []string
	if a.SCRAMEnabled && a.Credentials != nil {
		out = append(out, auth.MechanismSCRAMSHA256, auth.MechanismSCRAMSHA512)
	}
	if a.Username != "" {
		out = append(out, auth.MechanismPlain)
	}
	return out
}

// Supports reports whether a mechanism is enabled.
func (a AuthConfig) Supports(mechanism string) bool {
	for _, m := range a.Mechanisms() {
		if m == mechanism {
			return true
		}
	}
	return false
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
		return "v7, which adds LeaderEpoch to each partition -- the ownership epoch of the agent " +
			"that leads it, which is how a client detects that the leader it cached has been " +
			"replaced. v7 is the newest non-flexible version; v8 adds topic authorization, which " +
			"is not implemented."
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
	ApiKeyProduce:         3,
	ApiKeyFetch:           5,
	ApiKeyListOffsets:     2,
	ApiKeyMetadata:        7,
	ApiKeyApiVersions:     0,
	ApiKeyOffsetCommit:    0,
	ApiKeyOffsetFetch:     1,
	ApiKeyFindCoordinator: 0,
	ApiKeyJoinGroup:       1,
	ApiKeySyncGroup:       0,
	ApiKeyHeartbeat:       0,
	ApiKeyLeaveGroup:      0,
	ApiKeyCreateTopics:    0,
	ApiKeyDeleteTopics:    0,
	ApiKeyListGroups:      0,
	ApiKeyDescribeGroups:  0,
	ApiKeySaslHandshake:   1,
	// v1 appends session_lifetime_ms to the response. It is advertised and
	// answered because clients that can use it will otherwise fall back to v0
	// and lose the ability to tell a non-expiring session from an expiry.
	ApiKeySaslAuthenticate: 1,
	ApiKeyInitProducerID:   1,
}

var apiNames = map[int16]string{
	ApiKeyProduce: "Produce", ApiKeyFetch: "Fetch", ApiKeyListOffsets: "ListOffsets",
	ApiKeyMetadata: "Metadata", ApiKeyApiVersions: "ApiVersions", ApiKeyOffsetCommit: "OffsetCommit",
	ApiKeyOffsetFetch: "OffsetFetch", ApiKeyFindCoordinator: "FindCoordinator", ApiKeyJoinGroup: "JoinGroup",
	ApiKeySyncGroup: "SyncGroup", ApiKeyHeartbeat: "Heartbeat", ApiKeyLeaveGroup: "LeaveGroup",
	ApiKeyCreateTopics: "CreateTopics", ApiKeyDeleteTopics: "DeleteTopics", ApiKeyListGroups: "ListGroups",
	ApiKeyDescribeGroups: "DescribeGroups", ApiKeySaslHandshake: "SaslHandshake", ApiKeySaslAuthenticate: "SaslAuthenticate",
	ApiKeyInitProducerID: "InitProducerId",
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
	// Required(), not a bare Username check: a broker configured only with SCRAM
	// credentials has no SASL_USERNAME, and testing the username field alone
	// would leave every request ungated. An unauthenticated client could then
	// produce and consume on a broker whose credentials exist.
	authRequired := cfg.Auth.Required()
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
		resp, errProc = handleSaslHandshake(dec, enc, apiVersion, session, cfg)
	case ApiKeySaslAuthenticate:
		resp, errProc = handleSaslAuthenticate(ctx, dec, enc, apiVersion, session, cfg)
	case ApiKeyProduce:
		resp, errProc = handleProduce(ctx, dec, enc, store, apiVersion, session, cfg)
	case ApiKeyFetch:
		resp, errProc = handleFetch(ctx, dec, enc, store, apiVersion, session, cfg)
	case ApiKeyListOffsets:
		resp, errProc = handleListOffsets(ctx, dec, enc, store, apiVersion)
	case ApiKeyApiVersions:
		resp, errProc = handleApiVersions(dec, enc, apiVersion, cfg)
	case ApiKeyMetadata:
		resp, errProc = handleMetadata(ctx, dec, enc, store, apiVersion, session, cfg)
	case ApiKeyFindCoordinator:
		resp, errProc = handleFindCoordinator(ctx, dec, enc, store, apiVersion, cfg)
	case ApiKeyJoinGroup:
		resp, errProc = handleJoinGroup(dec, enc, store, apiVersion)
	case ApiKeySyncGroup:
		resp, errProc = handleSyncGroup(dec, enc, store, apiVersion)
	case ApiKeyHeartbeat:
		resp, errProc = handleHeartbeat(dec, enc, store, apiVersion)
	case ApiKeyLeaveGroup:
		resp, errProc = handleLeaveGroup(dec, enc, store, apiVersion)
	case ApiKeyOffsetCommit:
		resp, errProc = handleOffsetCommit(dec, enc, store, apiVersion)
	case ApiKeyOffsetFetch:
		resp, errProc = handleOffsetFetch(dec, enc, store, apiVersion)
	case ApiKeyCreateTopics:
		resp, errProc = handleCreateTopics(ctx, dec, enc, store, apiVersion, session, cfg)
	case ApiKeyDeleteTopics:
		resp, errProc = handleDeleteTopics(ctx, dec, enc, store, apiVersion, session, cfg)
	case ApiKeyListGroups:
		resp, errProc = handleListGroups(dec, enc, store, apiVersion)
	case ApiKeyDescribeGroups:
		resp, errProc = handleDescribeGroups(dec, enc, store, apiVersion)
	case ApiKeyInitProducerID:
		resp, errProc = handleInitProducerId(ctx, dec, enc, store, apiVersion)
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
// Group Coordinator Handlers
// ----------------------------------------------------------------------

var GlobalCoordinator = coordinator.NewCoordinator()

// coordinatorFor answers which agent coordinates a group.
//
// Rendezvous hashing over the live set, so the answer is the same on every agent
// that shares a view, and an agent leaving moves only the groups it held. The
// client treats this as a hint and does not have to trust it: whichever agent it
// ends up talking to re-checks, and answers NOT_COORDINATOR if it is not the one.
func coordinatorFor(store *storage.StorageEngine, groupID string, cfg ServerConfig) (coordinator.AgentRef, bool) {
	if store != nil {
		if ref, ok := store.CoordinatorFor(groupID); ok {
			return ref, true
		}
	}
	// No routing view: this agent is the whole cluster.
	return coordinator.AgentRef{
		Agent:  "self",
		NodeID: cfg.NodeID,
		Host:   cfg.AdvertisedHost,
		Port:   cfg.AdvertisedPort,
	}, true
}

// coordinatorFence decides whether this agent may act as a group's coordinator.
//
// Two checks, and both are refusals rather than degradations:
//
//   - If this agent is not the rendezvous winner for the group over its own live
//     set, another agent is. Acting anyway is what produces two coordinators for
//     one group: two independent assignment states, consumers handed different
//     partitions, and no way for either side to notice.
//   - If this agent cannot prove it is alive, it may already have been replaced.
//     There is no grace window here, unlike partition ownership, because a
//     coordinator has no epoch token to fence it with.
//
// NOT_COORDINATOR is the right answer for both: it tells the client to re-resolve
// the coordinator, which is exactly the recovery.
func coordinatorFence(store *storage.StorageEngine, groupID string) error {
	if store == nil {
		return nil
	}
	if !store.AgentLive() {
		metrics.CoordinatorRequestsRefused.Inc()
		return coordinator.ErrCoordinatorUnfenced
	}
	if !store.IsCoordinator(groupID) {
		metrics.CoordinatorRequestsRefused.Inc()
		return coordinator.ErrNotCoordinator
	}
	return nil
}

func handleFindCoordinator(ctx context.Context, dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16, cfg ServerConfig) ([]byte, error) {
	// FindCoordinator Request V0:
	// GroupID (string)

	groupID, err := dec.String()
	if err != nil {
		return nil, err
	}

	// V1 adds KeyType (int8) and V2 adds an error message.
	if version >= 1 {
		if _, err := dec.Int8(); err != nil {
			return nil, err
		}
	}

	ref, _ := coordinatorFor(store, groupID, cfg)
	log.Printf("FindCoordinator: group=%s coordinator=%s (node %d)", groupID, ref.Agent, ref.NodeID)

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
	// The node id has to be one Metadata advertised for this agent: a client
	// told "the coordinator is node 7" looks node 7 up in the broker list it was
	// given, and an id that is not in that list cannot be connected to.
	enc.Int32(ref.NodeID)
	enc.String(ref.Host)
	enc.Int32(ref.Port)

	return enc.Bytes(), nil
}

func handleJoinGroup(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
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

	// Refuse before touching any group state if this agent is not the one that
	// coordinates this group, or cannot prove it is alive.
	if err := coordinatorFence(store, groupID); err != nil {
		metrics.CoordinatorOwner.Inc()
		// JoinGroup v0 has no error message field.
		enc.Int16(ErrNotCoordinator)
		enc.Int32(-1) // generation id: no group exists here
		enc.String("")
		enc.String("")
		enc.String("")
		enc.Int32(0)
		return enc.Bytes(), nil
	}
	metrics.CoordinatorOwner.Inc()

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

func handleSyncGroup(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
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

	// Same fence as JoinGroup. A SyncGroup from a member the group no longer
	// holds must not be answered: its assignment came from a coordinator that
	// may not be this one.
	if err := coordinatorFence(store, groupID); err != nil {
		enc.Int16(ErrNotCoordinator)
		enc.PutBytes(nil)
		return enc.Bytes(), nil
	}

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

func handleHeartbeat(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	groupID, _ := dec.String()
	generationID, _ := dec.Int32()
	memberID, _ := dec.String()

	err := coordinatorFence(store, groupID)
	errorCode := int16(ErrNone)
	if err == nil {
		err = GlobalCoordinator.Heartbeat(groupID, memberID, generationID)
	}
	if err != nil {
		if errors.Is(err, coordinator.ErrNotCoordinator) || errors.Is(err, coordinator.ErrCoordinatorUnfenced) {
			errorCode = ErrNotCoordinator
			enc.Int16(errorCode)
			return enc.Bytes(), nil
		}
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

func handleLeaveGroup(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	groupID, _ := dec.String()
	memberID, _ := dec.String()

	// Leaving is not fenced: a member must be able to tell *someone* it is going
	// away, and the coordinator it thought it was talking to is the one most
	// likely to have changed. The reaper converges the group regardless, so
	// dropping a leave that arrived at the wrong agent costs nothing beyond the
	// member's own session timeout.

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

func handleMetadata(ctx context.Context, dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16, session *Session, cfg ServerConfig) ([]byte, error) {
	// Metadata Request V0-V8:
	//   V0-V5: Topics (array of string). An empty array means "all topics".
	//   V6:    adds allow_auto_topic_creation (boolean, one byte).
	// The response is identical in shape from v6 to v7; v7 only adds
	// LeaderEpoch to each partition, and v8 adds topic authorization which is
	// not implemented, so the ceiling is v7.
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

	// The cluster view. Brokers are every live agent and each partition's leader
	// is the agent that claims it, so a client ends up talking to whoever owns
	// what it wants to read and write. This is the whole point of phase 3: the
	// write side was already fenced per partition, but without a leader per
	// partition a client had no way to find the agent holding one.
	routing := store.Routing()

	// 1. Brokers
	brokers := routing.Brokers
	// With no brokers in the view there is nobody to route to, so this agent
	// describes itself from its own configuration. That happens when the
	// registry is not enabled, or before its first refresh has completed.
	//
	// In that mode every partition in the view belongs to this agent, so the
	// leader is reported as this agent's configured node id rather than whatever
	// the registry recorded. They are the same value in a configured agent;
	// using one source here means a leader can never be named with an id that is
	// missing from the broker list, which is what a client needs to connect.
	selfOnly := len(brokers) == 0
	if selfOnly {
		brokers = []storage.Broker{{
			NodeID: cfg.NodeID, Host: cfg.AdvertisedHost, Port: cfg.AdvertisedPort, Agent: "self",
		}}
	}

	enc.Int32(int32(len(brokers)))
	for _, b := range brokers {
		// The advertised address, not the bind address: this is what the client
		// will dial. It has to be reachable from the client, which a loopback
		// address is not.
		enc.Int32(b.NodeID)
		enc.String(b.Host)
		enc.Int32(b.Port)
		if version >= 1 {
			enc.String("") // Rack
		}
	}

	// cluster_id arrived in Metadata v2 and is nullable from v2 onwards.
	// Emitting it at v1, as an earlier version of this code did, puts four
	// bytes where a v1 client expects the controller id and desynchronises
	// every following field.
	if version >= 2 {
		enc.String(clusterID)
	}

	if version >= 1 {
		// The controller is this agent. There is no separate controller role
		// here, but the id has to be one that appears in the broker list above,
		// or a client will try to look up a broker nobody advertised.
		enc.Int32(cfg.NodeID)
	}

	// 2. Topic Metadata
	//
	// The topic and partition set is the cluster's, not this agent's: a topic
	// whose partitions are split across agents has to be described in full by
	// every agent, or a client that hashed a key onto a partition it cannot see
	// has nowhere to send it.
	// Describe is NOT enforced yet. A refused Metadata request has to be a
	// complete, parseable MetadataResponse: brokers array, cluster_id and
	// controller_id all precede the topic array, so a refusal that only wrote
	// topics leaves the client reading the topic count as a broker count. It
	// then retries metadata forever and the caller sees a timeout rather than
	// an authorisation error, which is worse than not enforcing it. Until that
	// response is written correctly, Describe is unrestricted here and the data
	// plane (Produce/Fetch) is what actually gates access.

	allTopics := routing.Topics()
	topicsToReturn := requestedTopics
	if count <= 0 {
		topicsToReturn = allTopics
	} else if cfg.AutoCreateTopics {
		// Kafka creates a topic when a client asks about one that does not
		// exist, and clients depend on it: a producer needs partitions with a
		// known leader before it can write anything, and without this it
		// fails with "unknown partition leader" rather than producing.
		for _, tName := range requestedTopics {
			if routing.TopicExists(tName) || store.TopicExists(tName) {
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

		partitions := routing.PartitionsOf(tName)
		if len(partitions) == 0 {
			// Fall back to this agent's own view for a topic it just created and
			// has not published yet.
			local, err := store.GetPartitions(tName)
			if err == nil && len(local) > 0 {
				partitions = local
			}
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
			route, owned := routing.Owner(tName, pid)

			// Kafka's MetadataResponseTopicPartition starts with ErrorCode and
			// only then carries the partition number. Emitting them the other way
			// round does not fail a decode of the *first* partition -- the values
			// are simply read from the wrong offsets -- so it survives every
			// single-partition test and only shows up when a topic has more than
			// one partition and a client checks that the partition numbers are
			// consecutive. Grafana Mimir does check, and reports the second
			// partition as 65536.
			if !owned {
				// The partition exists but no live agent claims it: mid-failover,
				// or its owner is wedged past the routing TTL. LEADER_NOT_AVAILABLE
				// and Leader = -1 is the encoding Kafka uses for exactly this,
				// and it is what makes a client refresh its metadata and retry
				// instead of failing the produce permanently. Reporting a leader
				// that cannot serve the partition would send the client to a
				// broker that answers NOT_LEADER_OR_FOLLOWER for ever.
				enc.Int16(ErrLeaderNotAvailable)
				enc.Int32(pid) // Partition
				enc.Int32(-1)  // Leader
				if version >= 7 {
					enc.Int32(0) // LeaderEpoch
				}
				enc.Int32(0) // Replica Count: an unowned partition has no replicas
				enc.Int32(0) // Isr Count
				if version >= 5 {
					enc.Int32(0) // OfflineReplicas
				}
				continue
			}

			leader := route.NodeID
			if selfOnly {
				leader = cfg.NodeID
			}

			enc.Int16(0)      // ErrorCode
			enc.Int32(pid)    // Partition
			enc.Int32(leader) // Leader
			if version >= 7 {
				// The ownership epoch, which is how a client detects that the
				// leader it has cached has been replaced and that its in-flight
				// idempotent requests may need resending.
				enc.Int32(int32(route.Epoch))
			}
			// One replica: this agent replicates nothing, so the leader is the
			// only copy that exists as far as the protocol is concerned.
			enc.Int32(1) // Replica Count
			enc.Int32(leader)
			enc.Int32(1) // Isr Count
			enc.Int32(leader)
			if version >= 5 {
				enc.Int32(0) // OfflineReplicas
			}
		}
	}

	return enc.Bytes(), nil
}

func handleSaslHandshake(dec *Decoder, enc *Encoder, version int16, session *Session, cfg ServerConfig) ([]byte, error) {
	// SaslHandshake Request V0:
	// Mechanism (string)

	// SaslHandshake Request V1:
	// Mechanism (string)

	mech, err := dec.String()
	if err != nil {
		return nil, err
	}

	// Every enabled mechanism is listed, not just the one asked for. A client
	// reads this array to decide whether to continue with its preferred
	// mechanism or fall back, so returning only the requested one would hide
	// the alternatives and force a reconnect to discover them.
	enabled := cfg.Auth.Mechanisms()
	log.Printf("SaslHandshake: Mechanism=%s Version=%d Enabled=%v", mech, version, enabled)

	if !cfg.Auth.Supports(mech) {
		// Refuse the mechanism but still report what is on offer, so the client
		// can see why and retry with something that works.
		enc.Int16(ErrUnsupportedSaslMechanism)
		enc.Int32(int32(len(enabled)))
		for _, m := range enabled {
			enc.String(m)
		}
		return enc.Bytes(), nil
	}

	// Remember the choice. SaslAuthenticate does not carry the mechanism, so
	// this is the only place the server learns which flow the following
	// requests belong to.
	session.saslMechanism = mech
	if mech != auth.MechanismPlain {
		ex, err := auth.StartExchange(mech)
		if err != nil {
			enc.Int16(ErrUnsupportedSaslMechanism)
			enc.Int32(int32(len(enabled)))
			for _, m := range enabled {
				enc.String(m)
			}
			return enc.Bytes(), nil
		}
		session.scram = ex
	}

	enc.Int16(ErrNone)
	enc.Int32(int32(len(enabled)))
	for _, m := range enabled {
		enc.String(m)
	}

	return enc.Bytes(), nil
}

func handleSaslAuthenticate(ctx context.Context, dec *Decoder, enc *Encoder, version int16, session *Session, cfg ServerConfig) ([]byte, error) {
	// SaslAuthenticate Request V0:
	// AuthBytes (bytes)

	authBytes, err := dec.Bytes()
	if err != nil {
		return nil, err
	}

	// Dispatch on the mechanism chosen in SaslHandshake. Falling through to
	// PLAIN here would be a silent downgrade: a client that negotiated SCRAM
	// would have its final message parsed as a PLAIN payload, and would be told
	// its authentication failed rather than that the broker got it wrong.
	switch session.saslMechanism {
	case "", auth.MechanismPlain:
		return handleSaslPlain(enc, version, session, cfg, authBytes)
	default:
		return handleSaslSCRAM(ctx, enc, version, session, cfg, authBytes)
	}
}

// handleSaslPlain serves the single shared SASL_USERNAME/SASL_PASSWORD
// credential.
func handleSaslPlain(enc *Encoder, version int16, session *Session, cfg ServerConfig, authBytes []byte) ([]byte, error) {
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
		writeSaslAuthenticateResponse(enc, version, ErrSaslAuthenticationFailed, "Invalid SASL PLAIN payload", nil)
		return enc.Bytes(), nil
	}

	if cfg.Auth.Required() && subtle.ConstantTimeCompare([]byte(username), []byte(cfg.Auth.Username)) == 1 &&
		subtle.ConstantTimeCompare([]byte(password), []byte(cfg.Auth.Password)) == 1 {
		session.Authenticated = true
		session.User = username
		writeSaslAuthenticateResponse(enc, version, ErrNone, "", nil)
	} else {
		// Do not log the attempted username on a failed handshake: this is
		// attacker-controlled input on an unauthenticated path.
		log.Printf("SaslAuthenticate: authentication failed")
		writeSaslAuthenticateResponse(enc, version, ErrSaslAuthenticationFailed, "Authentication failed", nil)
	}

	return enc.Bytes(), nil
}

// handleSaslSCRAM drives one step of a SCRAM exchange.
//
// SCRAM spans three SaslAuthenticate requests, so which step this is has to be
// derived from the exchange's own state rather than assumed: the first message
// is a client-first and yields a challenge, the second is a client-final and
// yields the server signature. Getting that wrong in either direction is
// invisible locally and fatal to every client.
//
// Errors here are deliberately coarse on the wire. A missing user, a wrong
// password and a malformed final message all answer as ErrSaslAuthenticationFailed,
// because distinguishing them would let an unauthenticated peer enumerate
// accounts. The detail goes to the log, which is authenticated-server-side and
// where the operator can act on it.
func handleSaslSCRAM(ctx context.Context, enc *Encoder, version int16, session *Session, cfg ServerConfig, authBytes []byte) ([]byte, error) {
	ex := session.scram
	if ex == nil {
		// The handshake should have created this. Reaching here means a client
		// sent SaslAuthenticate without a preceding SaslHandshake, so there is
		// no mechanism to interpret the message under.
		writeSaslAuthenticateResponse(enc, version, ErrSaslAuthenticationFailed, "SCRAM exchange not started", nil)
		return enc.Bytes(), nil
	}
	if cfg.Auth.Credentials == nil {
		// The credential store went away between handshake and authenticate,
		// which means the advertised mechanism is no longer honourable.
		writeSaslAuthenticateResponse(enc, version, ErrSaslAuthenticationFailed, "SCRAM is not configured", nil)
		return enc.Bytes(), nil
	}

	if !ex.Challenged() {
		// Not yet challenged: this message must be the client-first.
		challenge, err := ex.First(ctx, authBytes, cfg.Auth.Credentials)
		if err != nil {
			log.Printf("SaslAuthenticate: SCRAM client-first rejected: %v", err)
			writeSaslAuthenticateResponse(enc, version, ErrSaslAuthenticationFailed, "authentication failed", nil)
			return enc.Bytes(), nil
		}
		// A challenge is carried in auth_bytes with a success code: that is what
		// tells the client the exchange is continuing rather than finished.
		writeSaslAuthenticateResponse(enc, version, ErrNone, "", challenge)
		return enc.Bytes(), nil
	}

	// Already challenged: this message must be the client-final. A failure here
	// is terminal, so do not answer with a challenge the client cannot use.
	serverFinal, err := ex.Final(authBytes)
	if err != nil {
		log.Printf("SaslAuthenticate: SCRAM client-final rejected: %v", err)
		writeSaslAuthenticateResponse(enc, version, ErrSaslAuthenticationFailed, "authentication failed", nil)
		return enc.Bytes(), nil
	}

	session.Authenticated = true
	session.User = ex.Username()
	writeSaslAuthenticateResponse(enc, version, ErrNone, "", []byte(serverFinal))
	return enc.Bytes(), nil
}

// writeSaslAuthenticateResponse emits SaslAuthenticate Response in the shape
// the requested version requires.
//
// V0 is error_code, error_message, auth_bytes. V1 appends session_lifetime_ms,
// and a client that asked for v1 will read the response with that field
// present; leaving it off truncates the message and the client sees a short
// read rather than a clean answer. Every exit path goes through here so the two
// versions cannot drift apart, which is the failure mode of writing the shape
// inline at each return.
//
// session_lifetime_ms is always 0, meaning the authenticated session does not
// expire. Clients treat a positive value as "reauthenticate before continuing",
// and the broker does not implement mid-connection reauthentication -- a
// connection stays authenticated until it closes -- so advertising a lifetime
// would commit the broker to a mechanism it does not have.
func writeSaslAuthenticateResponse(enc *Encoder, version int16, errCode int16, errMsg string, authBytes []byte) {
	enc.Int16(errCode)
	// ErrorMessage is non-nullable in v0 and nullable in v1. An empty string
	// satisfies both, so no separate nullable encoding is needed here.
	enc.String(errMsg)
	enc.PutBytes(authBytes)
	if version >= 1 {
		enc.Int64(0) // session_lifetime_ms: no expiry
	}
}

// Required reports whether any SASL authentication is configured.
func (a AuthConfig) Required() bool { return len(a.Mechanisms()) > 0 }
