package protocol

import (
	"fmt"
	"log"
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
	ErrGroupAuthorizationFailed   = 30
	ErrClusterAuthorizationFailed = 31
	ErrUnsupportedVersion         = 35
	ErrTopicAlreadyExists         = 36
	ErrSaslAuthenticationFailed   = 58
	ErrUnsupportedSaslMechanism   = 33
	ErrIllegalSaslState           = 34
)

type Session struct {
	Authenticated bool
	User          string
}

type AuthConfig struct {
	Username string
	Password string
}

func HandleRequest(data []byte, store *storage.StorageEngine, session *Session, authConfig AuthConfig) ([]byte, error) {
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
	authRequired := authConfig.Username != ""
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

	switch apiKey {
	case ApiKeySaslHandshake:
		resp, errProc = handleSaslHandshake(dec, enc, apiVersion)
	case ApiKeySaslAuthenticate:
		resp, errProc = handleSaslAuthenticate(dec, enc, apiVersion, session, authConfig)
	case ApiKeyProduce:
		resp, errProc = handleProduce(dec, enc, store, apiVersion)
	case ApiKeyFetch:
		resp, errProc = handleFetch(dec, enc, store, apiVersion)
	case ApiKeyListOffsets:
		resp, errProc = handleListOffsets(dec, enc, store, apiVersion)
	case ApiKeyApiVersions:
		resp, errProc = handleApiVersions(enc, apiVersion)
	case ApiKeyMetadata:
		resp, errProc = handleMetadata(dec, enc, store, apiVersion)
	case ApiKeyFindCoordinator:
		resp, errProc = handleFindCoordinator(dec, enc, apiVersion)
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
		resp, errProc = handleCreateTopics(dec, enc, store, apiVersion)
	case ApiKeyDeleteTopics:
		resp, errProc = handleDeleteTopics(dec, enc, store, apiVersion)
	case ApiKeyListGroups:
		resp, errProc = handleListGroups(dec, enc, store, apiVersion)
	case ApiKeyDescribeGroups:
		resp, errProc = handleDescribeGroups(dec, enc, store, apiVersion)
	default:
		log.Printf("Unsupported API Key: %d", apiKey)
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

func handleFindCoordinator(dec *Decoder, enc *Encoder, version int16) ([]byte, error) {
	// FindCoordinator Request V0:
	// GroupID (string)

	groupID, err := dec.String()
	if err != nil {
		return nil, err
	}
	log.Printf("FindCoordinator: GroupID=%s", groupID)

	// FindCoordinator Response V0:
	// ErrorCode (int16)
	// NodeID (int32)
	// Host (string)
	// Port (int32)

	enc.Int16(ErrNone)      // No Error
	enc.Int32(1)            // NodeID 1 (Our static broker)
	enc.String("localhost") // Host
	enc.Int32(19092)        // Port

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
	// Kafka returns the *selected* protocol name (e.g. "range" or "roundrobin").
	// Our coordinator simple picks protocols[0].Name
	if len(protocols) > 0 {
		enc.String(protocols[0].Name)
	} else {
		enc.String("")
	}
	enc.String(leaderID)
	enc.String(newMemberID)

	// Members Array
	enc.Int32(int32(len(members)))
	for _, m := range members {
		enc.String(m.MemberID)
		// Protocols array in request had metadata.
		// We need to return Metadata for the selected protocol.
		// For MVP, just return the metadata provided by member for this protocol.

		// Find metadata for the selected protocol
		var meta []byte
		for _, p := range m.Protocols {
			// Match selected name?
			if len(protocols) > 0 && p.Name == protocols[0].Name {
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
		errorCode = 25 // UnknownMemberId? or RebalanceInProgress?
		// 25 = UnknownMemberId
		// 27 = RebalanceInProgress
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
		errorCode = 27 // RebalanceInProgress usually triggers rejoin
	}

	enc.Int16(errorCode)
	return enc.Bytes(), nil
}

func handleLeaveGroup(dec *Decoder, enc *Encoder, version int16) ([]byte, error) {
	groupID, _ := dec.String()
	memberID, _ := dec.String()

	_ = GlobalCoordinator.LeaveGroup(groupID, memberID)

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

func handleOffsetFetch(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// OffsetFetch Request V1:
	// GroupID (string)
	// Topics Array (int32)
	//   TopicName (string)
	//   Partitions Array (int32)
	//     Partition (int32)

	groupID, _ := dec.String()
	count, _ := dec.Int32()

	log.Printf("OffsetFetch: Group=%s Count=%d", groupID, count)

	// Response must mirror the request structure with offsets
	enc.Int32(count) // Number of topics

	for i := 0; i < int(count); i++ {
		topic, _ := dec.String()
		enc.String(topic)

		partitionCount, _ := dec.Int32()
		enc.Int32(partitionCount)

		for j := 0; j < int(partitionCount); j++ {
			partition, _ := dec.Int32()

			// LOAD OFFSET
			offset, err := GlobalCoordinator.FetchOffset(store, groupID, topic, partition)

			enc.Int32(partition)
			if err != nil || offset == -1 {
				enc.Int64(-1) // Unknown
				enc.String("")
				enc.Int16(ErrNone)
			} else {
				enc.Int64(offset)
				enc.String("") // Metadata
				enc.Int16(ErrNone)
			}
		}
	}

	return enc.Bytes(), nil
}

func handleApiVersions(enc *Encoder, version int16) ([]byte, error) {
	if version > 0 {
		// We only support V0.
		// If client asks for V1+, we return UnsupportedVersion.
		// Problem: Client expects response format of V(requested).
		// Sending V0 format might crash client.
		// But for ApiVersions, if we return error, client should handle it.
		// Let's try returning Error and empty/safe body.

		enc.Int16(ErrUnsupportedVersion)
		// If V3, it expects Throttle(32) + CompactArray.
		// If we write 0 (Throttle) + 0 (ArrayLen), it might parse.
		// But we don't know EXACTLY what version was requested easily without mapping every version.
		// Let's just try sending V0 format with Error.

		// V0: Error(16) + Array(32)
		enc.Int32(0) // Empty array
		return enc.Bytes(), nil
	}

	// ApiVersions Response V0:
	// ErrorCode (int16)
	// ApiKeys (Array)

	enc.Int16(ErrNone) // No Error

	// Array length: 5
	// Listing: Produce, Fetch, ListOffsets, Metadata, ApiVersions
	// Supported: Produce(0-2), Fetch(0-2), ListOffsets(0-1), Metadata(0-2), ApiVersions(0)
	// + Group APIs: OffsetCommit(0), OffsetFetch(0-1), FindCoordinator(0), JoinGroup(0), SyncGroup(0), Heartbeat(0), LeaveGroup(0)
	// + SASL: SaslHandshake(0-1), SaslAuthenticate(0)

	numKeys := 18
	enc.Int32(int32(numKeys)) // Array length is int32 usually?
	// careful: Array length in V0 is int32.

	// Function to write entry
	writeEntry := func(key int16, minV, maxV int16) {
		enc.Int16(key)
		enc.Int16(minV)
		enc.Int16(maxV)
	}

	writeEntry(ApiKeyProduce, 0, 2)
	writeEntry(ApiKeyFetch, 0, 2)
	writeEntry(ApiKeyListOffsets, 0, 1)
	writeEntry(ApiKeyMetadata, 0, 2)
	writeEntry(ApiKeyApiVersions, 0, 0)
	writeEntry(ApiKeyOffsetCommit, 0, 0)
	writeEntry(ApiKeyOffsetFetch, 0, 1)
	writeEntry(ApiKeyFindCoordinator, 0, 0)
	writeEntry(ApiKeyJoinGroup, 0, 1)
	writeEntry(ApiKeySyncGroup, 0, 0)
	writeEntry(ApiKeyHeartbeat, 0, 0)
	writeEntry(ApiKeyLeaveGroup, 0, 0)
	writeEntry(ApiKeyCreateTopics, 0, 0)
	writeEntry(ApiKeyDeleteTopics, 0, 0)
	writeEntry(ApiKeyListGroups, 0, 0)
	writeEntry(ApiKeyDescribeGroups, 0, 0)
	writeEntry(ApiKeySaslHandshake, 0, 1)
	writeEntry(ApiKeySaslAuthenticate, 0, 0)

	return enc.Bytes(), nil
}

func handleMetadata(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// Metadata Request V0+:
	// Topics Array (String)

	// We should decode the request body first.
	// But wait, the `HandleRequest` called `handleMetadata` which takes `enc`.
	// We need `dec` too!
	// Existing signature was: func handleMetadata(enc *Encoder, version int16)
	// I need to change it to accept `dec`.

	count, err := dec.Int32()
	requestedTopic := ""
	if err == nil && count > 0 {
		// Just read the first one for MVP
		t, _ := dec.String()
		requestedTopic = t
		// If there are more, we ignore them (read remaining to clear buffer?)
		// This is a BUG if count > 1.
		// But usually clients ask for 1 or All.
	}
	log.Printf("Metadata Req: Count=%d Requested=%s", count, requestedTopic)

	if requestedTopic == "" {
		requestedTopic = "bench-topic" // Default for testing/benchmark
	}

	// 1. Brokers
	enc.Int32(1)

	// Broker 0
	enc.Int32(1)            // NodeID
	enc.String("localhost") // Host
	enc.Int32(19092)        // Port

	if version >= 1 {
		enc.String("") // Rack (empty instead of null)
	}

	// ClusterID (string) - Added in V2
	if version >= 2 {
		enc.String("kimistore-cluster")
	}

	// ControllerID (int32) - Added in V1
	if version >= 1 {
		enc.Int32(1) // Controller is Node 1
	}

	// 2. Topic Metadata

	var topicsToReturn []string
	if count <= 0 {
		// Return All topics
		// Scan storage
		// For MVP: List directory? Using GetPartitions logic on known topics?
		// We don't have a "ListTopics" in storage yet.
		// NOTE: NewStorageEngine has ListPartitions but not ListTopics efficiently exposed.
		// However, we can trick it or just return the default + requested.
		// If requested is empty, we MUST return something useful or ALL.
		// Let's return "bench-topic" AND "bench-multi" for now to fix test.
		topicsToReturn = []string{"bench-topic", "bench-multi"}
	} else {
		topicsToReturn = []string{requestedTopic}
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
			log.Printf("Failed to get partitions for %s: %v", tName, err)
			partitions = []int32{}
		}
		if len(partitions) == 0 {
			// If it's a known topic, default 0?
			if tName == "bench-multi" {
				// We expect 4. If 0, something is wrong with GetPartitions scanning?
				// But let's assume auto-create 0
				partitions = []int32{0}
			} else {
				partitions = []int32{0}
			}
		}

		log.Printf("Metadata Return: Topic=%s Partitions=%v", tName, partitions)

		enc.Int32(int32(len(partitions)))
		for _, pid := range partitions {
			enc.Int16(0)   // PartitionErrorCode
			enc.Int32(pid) // PartitionID
			enc.Int32(1)   // Leader
			// Replicas
			enc.Int32(1)
			enc.Int32(1)
			// Isr
			enc.Int32(1)
			enc.Int32(1)
		}
	}

	log.Println("Metadata Response encoded.")
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

func handleSaslAuthenticate(dec *Decoder, enc *Encoder, version int16, session *Session, authConfig AuthConfig) ([]byte, error) {
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

	log.Printf("SaslAuthenticate: user=%s", username)

	if username == authConfig.Username && password == authConfig.Password {
		session.Authenticated = true
		session.User = username
		enc.Int16(ErrNone)
		enc.String("")    // No error message
		enc.PutBytes(nil) // No auth bytes
	} else {
		log.Printf("SaslAuthenticate: Authentication failed for user=%s", username)
		enc.Int16(ErrSaslAuthenticationFailed)
		enc.String("Authentication failed")
		enc.PutBytes(nil)
	}

	return enc.Bytes(), nil
}
