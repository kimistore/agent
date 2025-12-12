package protocol

import (
	"fmt"
	"log"

	"go-stream/internal/storage"
)

// ... constants ...
const (
	ApiKeyProduce     = 0
	ApiKeyFetch       = 1
	ApiKeyListOffsets = 2
	ApiKeyMetadata    = 3
	ApiKeyApiVersions = 18
)

const (
	ErrNone               = 0
	ErrUnsupportedVersion = 35
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
		return handleMetadata(dec, enc, apiVersion)
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

	numKeys := 5
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

	return enc.Bytes(), nil
}

func handleMetadata(dec *Decoder, enc *Encoder, version int16) ([]byte, error) {
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
	enc.Int32(1) // 1 Partition (ID 0)

	enc.Int16(0) // PartitionErrorCode
	enc.Int32(0) // PartitionID
	enc.Int32(1) // Leader

	// Replicas (Array int32)
	enc.Int32(1)
	enc.Int32(1)

	// Isr (Array int32)
	enc.Int32(1)
	enc.Int32(1)

	log.Println("Metadata Response encoded.")
	return enc.Bytes(), nil
}
