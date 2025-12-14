package protocol

import (
	"fmt"
	"log"

	"go-stream/internal/coordinator"
	"go-stream/internal/storage"
)

// ... constants ...
const (
	ApiKeyProduce         = 0
	ApiKeyFetch           = 1
	ApiKeyListOffsets     = 2
	ApiKeyMetadata        = 3
	ApiKeyOffsetCommit    = 8
	ApiKeyOffsetFetch     = 9
	ApiKeyFindCoordinator = 10
	ApiKeyJoinGroup       = 11
	ApiKeyHeartbeat       = 12
	ApiKeyLeaveGroup      = 13
	ApiKeySyncGroup       = 14
	ApiKeyApiVersions     = 18
)

const (
	ErrNone                     = 0
	ErrUnknown                  = -1
	ErrUnsupportedVersion       = 35
	ErrGroupAuthorizationFailed = 30
)

func HandleRequest(data []byte, store *storage.StorageEngine) ([]byte, error) {
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

	log.Printf("Request: ApiKey=%d Version=%d CorrelationID=%d ClientID=%s",
		apiKey, apiVersion, correlationID, clientID)

	enc := NewEncoder()
	// Response Header: CorrelationID
	enc.Int32(correlationID)

	switch apiKey {
	case ApiKeyProduce:
		return handleProduce(dec, enc, store, apiVersion)
	case ApiKeyFetch:
		return handleFetch(dec, enc, store, apiVersion)
	case ApiKeyListOffsets:
		return handleListOffsets(dec, enc, store, apiVersion)
	case ApiKeyApiVersions:
		return handleApiVersions(enc, apiVersion)
	case ApiKeyMetadata:
		return handleMetadata(dec, enc, store, apiVersion)
	case ApiKeyFindCoordinator:
		return handleFindCoordinator(dec, enc, apiVersion)
	case ApiKeyJoinGroup:
		return handleJoinGroup(dec, enc, apiVersion)
	case ApiKeySyncGroup:
		return handleSyncGroup(dec, enc, apiVersion)
	case ApiKeyHeartbeat:
		return handleHeartbeat(dec, enc, apiVersion)
	case ApiKeyLeaveGroup:
		return handleLeaveGroup(dec, enc, apiVersion)
	case ApiKeyOffsetCommit:
		return handleOffsetCommit(dec, enc, store, apiVersion)
	case ApiKeyOffsetFetch:
		return handleOffsetFetch(dec, enc, store, apiVersion)
	default:
		// Unsupported API?
		// We should return some error code, but since formatting depends on API...
		// For now just close or return empty.
		// Real Kafka returns a response with ErrorCode if it can parse it,
		// but generic error handling is per-api.
		log.Printf("Unsupported API Key: %d", apiKey)
		return nil, fmt.Errorf("unsupported api key: %d", apiKey)
	}
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

	log.Printf("JoinGroup: Group=%s Member=%s ProtocolType=%s", groupID, memberID, protocolType)

	// Call Coordinator
	newMemberID, generationID, leaderID, members, err := GlobalCoordinator.JoinGroup(groupID, memberID, protocolType, protocols, sessionTimeout)

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

	numKeys := 12
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
	writeEntry(ApiKeyJoinGroup, 0, 0)
	writeEntry(ApiKeySyncGroup, 0, 0)
	writeEntry(ApiKeyHeartbeat, 0, 0)
	writeEntry(ApiKeyLeaveGroup, 0, 0)

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
	}
	log.Println("Handling Metadata Request...")

	if requestedTopic == "" {
		requestedTopic = "my-topic" // Default for testing
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
		enc.String("warpstream-cluster")
	}

	// ControllerID (int32) - Added in V1
	if version >= 1 {
		enc.Int32(1) // Controller is Node 1
	}

	// 2. Topic Metadata
	enc.Int32(1) // Return 1 topic

	// Topic "my-topic" (or requested)
	enc.Int16(0) // TopicErrorCode (0 = OK)
	enc.String(requestedTopic)
	if version >= 1 {
		enc.Int8(0) // IsInternal (false)
	}

	// Partitions
	// Query storage for partitions
	partitions, err := store.GetPartitions(requestedTopic)
	if err != nil {
		log.Printf("Failed to get partitions: %v", err)
		// Fallback to 0 partitions or error?
		// If topic doesn't exist, we usually auto-create implicitly on Produce.
		// Metadata often returns LeaderNotAvailable if new?
		// Let's default to Partition 0 if empty list.
		partitions = []int32{}
	}

	// Auto-create Partition-0 default if none?
	if len(partitions) == 0 {
		partitions = []int32{0}
	}

	enc.Int32(int32(len(partitions)))

	for _, pid := range partitions {
		enc.Int16(0)   // PartitionErrorCode
		enc.Int32(pid) // PartitionID
		enc.Int32(1)   // Leader
		// Replicas (Array int32)
		enc.Int32(1)
		enc.Int32(1)
		// Isr (Array int32)
		enc.Int32(1)
		enc.Int32(1)
	}

	log.Println("Metadata Response encoded.")
	return enc.Bytes(), nil
}
