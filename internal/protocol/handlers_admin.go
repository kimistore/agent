package protocol

import (
	"log"

	"go-stream/internal/storage"
)

func handleCreateTopics(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// Request V0
	// Array of CreateTopicRequests

	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	type TopicResult struct {
		Topic string
		Error int16
	}
	results := make([]TopicResult, 0, count)

	for i := int32(0); i < count; i++ {
		topic, _ := dec.String()
		numPartitions, _ := dec.Int32()
		replicationFactor, _ := dec.Int16()

		// Replica Assignemnt
		assignCount, _ := dec.Int32()
		for j := int32(0); j < assignCount; j++ {
			dec.Int32() // PartitionID
			repCount, _ := dec.Int32()
			for k := int32(0); k < repCount; k++ {
				dec.Int32() // Replica
			}
		}

		// Configs
		configCount, _ := dec.Int32()
		for j := int32(0); j < configCount; j++ {
			dec.String() // Key
			dec.String() // Value (nullable)
		}

		log.Printf("CreateTopic: Name=%s Partitions=%d RepFactor=%d", topic, numPartitions, replicationFactor)

		// Impl
		errCode := int16(ErrNone)

		// Check exists?
		existingParts, _ := store.GetPartitions(topic)
		if len(existingParts) > 0 {
			// Already exists
			errCode = ErrTopicAlreadyExists
		} else {
			// Default partitions if -1? Kafka usually requires > 0
			if numPartitions < 1 {
				numPartitions = 1
			}
			if err := store.CreateTopic(topic, numPartitions); err != nil {
				log.Printf("Failed to create topic %s: %v", topic, err)
				errCode = ErrUnknown
			}
		}

		results = append(results, TopicResult{Topic: topic, Error: errCode})
	}

	timeoutMs, _ := dec.Int32()
	_ = timeoutMs

	// Response V0
	enc.Int32(int32(len(results)))
	for _, r := range results {
		enc.String(r.Topic)
		enc.Int16(r.Error)
	}

	return enc.Bytes(), nil
}

func handleDeleteTopics(dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16) ([]byte, error) {
	// Request V0
	// Array of Topics (String)
	count, err := dec.Int32()
	if err != nil {
		return nil, err
	}

	topics := make([]string, 0, count)
	for i := int32(0); i < count; i++ {
		t, _ := dec.String()
		topics = append(topics, t)
	}

	timeoutMs, _ := dec.Int32()
	_ = timeoutMs

	type TopicResult struct {
		Topic string
		Error int16
	}
	results := make([]TopicResult, 0, count)

	for _, topic := range topics {
		log.Printf("DeleteTopic: Name=%s", topic)

		errCode := int16(ErrNone)

		// Check existence?
		parts, _ := store.GetPartitions(topic)
		if len(parts) == 0 {
			errCode = ErrUnknownTopicOrPartition
		} else {
			if err := store.DeleteTopic(topic); err != nil {
				log.Printf("Failed to delete topic %s: %v", topic, err)
				errCode = ErrUnknown
			}
		}
		results = append(results, TopicResult{Topic: topic, Error: errCode})
	}

	// Response V0
	enc.Int32(int32(len(results)))
	for _, r := range results {
		enc.String(r.Topic)
		enc.Int16(r.Error)
	}

	return enc.Bytes(), nil
}
