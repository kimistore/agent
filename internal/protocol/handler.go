package protocol

import (
	"fmt"
	"log"
)

const (
	ApiKeyProduce     = 0
	ApiKeyFetch       = 1
	ApiKeyMetadata    = 3
	ApiKeyApiVersions = 18
)

func HandleRequest(data []byte) ([]byte, error) {
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
	case ApiKeyApiVersions:
		return handleApiVersions(enc, apiVersion)
	case ApiKeyMetadata:
		return handleMetadata(enc, apiVersion)
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
	// ApiVersions Response V0:
	// ErrorCode (int16)
	// ApiKeys (Array)

	enc.Int16(0) // No Error

	// Array length: 3 (Produce, Fetch, Metadata, ApiVersions... wait, let's list them)
	// Listing: Produce(0), Fetch(1), Metadata(3), ApiVersions(18)
	// We'll support V0 for all for now.

	numKeys := 4
	enc.Int32(int32(numKeys)) // Array length is int32 usually?
	// careful: Array length in V0 is int32.

	// Function to write entry
	writeEntry := func(key int16, minV, maxV int16) {
		enc.Int16(key)
		enc.Int16(minV)
		enc.Int16(maxV)
	}

	writeEntry(ApiKeyProduce, 0, 0)
	writeEntry(ApiKeyFetch, 0, 0)
	writeEntry(ApiKeyMetadata, 0, 1)
	writeEntry(ApiKeyApiVersions, 0, 0)

	// ThrottleTimeMs (int32)? Only in V1+. If request was V0, we end here.
	// But newer clients usually send V3.
	// We should probably check the request version.
	// If version >= 1, add ThrottleTimeMs
	if version >= 1 {
		enc.Int32(0) // ThrottleTimeMs
	}
	// Note: V3 uses compact arrays. This is naive V0-V2 support.

	return enc.Bytes(), nil
}

func handleMetadata(enc *Encoder, version int16) ([]byte, error) {
	// Metadata Response V0:
	// Brokers Array
	// Topic Metadata Array

	// 1. Brokers
	// Length (int32)
	enc.Int32(1)

	// Broker 0
	enc.Int32(1)            // NodeID
	enc.String("localhost") // Host
	enc.Int32(19092)        // Port

	if version >= 1 {
		enc.Int16(-1) // Rack (null)
	}

	// ControllerID (int32) - Added in V1
	if version >= 1 {
		enc.Int32(1) // Controller is Node 1
	}

	// 2. Topic Metadata
	// Length (int32)
	enc.Int32(0) // No topics yet

	return enc.Bytes(), nil
}
