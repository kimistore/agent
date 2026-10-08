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
	"log"

	"kimistore/internal/auth"
	"kimistore/internal/storage"
)

func handleCreateTopics(ctx context.Context, dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16, session *Session, cfg ServerConfig) ([]byte, error) {
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
		topic, err := dec.String()
		if err != nil {
			return nil, err
		}

		// Checked as each topic is read rather than after the whole request,
		// because the response array has to be built either way and this way the
		// refusal is already in the right position.
		if code := authorize(cfg, session, auth.OpCreate, auth.Topic(topic)); code != ErrNone {
			results = append(results, TopicResult{Topic: topic, Error: code})
			// The rest of this topic's fields still have to be consumed or the
			// stream desynchronises, but they are no longer interesting.
			if err := skipCreateTopicRemainder(dec); err != nil {
				return nil, err
			}
			continue
		}

		numPartitions, err := dec.Int32()
		if err != nil {
			return nil, err
		}
		replicationFactor, err := dec.Int16()
		if err != nil {
			return nil, err
		}

		// Replica Assignment
		assignCount, err := dec.Int32()
		if err != nil {
			return nil, err
		}
		for j := int32(0); j < assignCount; j++ {
			if _, err := dec.Int32(); err != nil { // PartitionID
				return nil, err
			}
			repCount, err := dec.Int32()
			if err != nil {
				return nil, err
			}
			for k := int32(0); k < repCount; k++ {
				if _, err := dec.Int32(); err != nil { // Replica
					return nil, err
				}
			}
		}

		// Configs
		configCount, err := dec.Int32()
		if err != nil {
			return nil, err
		}
		for j := int32(0); j < configCount; j++ {
			if _, err := dec.String(); err != nil { // Key
				return nil, err
			}
			if _, err := dec.String(); err != nil { // Value (nullable)
				return nil, err
			}
		}

		log.Printf("CreateTopic: Name=%s Partitions=%d RepFactor=%d", topic, numPartitions, replicationFactor)

		// Impl
		errCode := int16(ErrNone)

		// Existence is decided by the durable topic registry, not by whether
		// a local directory happens to exist. A topic whose segments have all
		// been offloaded has no local directory, so a directory-based check
		// would re-create it and report success on a topic that already
		// holds data.
		if store.TopicExists(topic) {
			errCode = ErrTopicAlreadyExists
		} else {
			// Default partitions if -1? Kafka usually requires > 0
			if numPartitions < 1 {
				numPartitions = 1
			}
			if err := store.CreateTopicContext(ctx, topic, numPartitions); err != nil {
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

func handleDeleteTopics(ctx context.Context, dec *Decoder, enc *Encoder, store *storage.StorageEngine, version int16, session *Session, cfg ServerConfig) ([]byte, error) {
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

	// Delete is the one operation whose absence of a grant must be a hard
	// refusal: a caller that cannot delete a topic it can read must not be
	// able to delete one it cannot either.
	if code := authorizeTopics(cfg, session, auth.OpDelete, topics); code != ErrNone {
		log.Printf("DeleteTopics: %s denied Delete", session.principal())
		results := make([]struct {
			Topic string
			Error int16
		}, 0, len(topics))
		for _, t := range topics {
			results = append(results, struct {
				Topic string
				Error int16
			}{t, code})
		}
		enc.Int32(int32(len(results)))
		for _, r := range results {
			enc.String(r.Topic)
			enc.Int16(r.Error)
		}
		return enc.Bytes(), nil
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
			if err := store.DeleteTopicContext(ctx, topic); err != nil {
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

// skipCreateTopicRemainder consumes the rest of a CreateTopics entry so the
// decoder stays aligned with the request stream.
//
// A refused topic still has its fields on the wire. Stopping early would leave
// them to be read as the next topic, which turns one unauthorised request into a
// stream of nonsense responses.
func skipCreateTopicRemainder(dec *Decoder) error {
	// numPartitions, replicationFactor
	if _, err := dec.Int32(); err != nil {
		return err
	}
	if _, err := dec.Int16(); err != nil {
		return err
	}
	// replica_assignment: array of (partition, [replicas])
	n, err := dec.Int32()
	if err != nil {
		return err
	}
	for i := int32(0); i < n; i++ {
		if _, err := dec.Int32(); err != nil {
			return err
		}
		r, err := dec.Int32()
		if err != nil {
			return err
		}
		for j := int32(0); j < r; j++ {
			if _, err := dec.Int32(); err != nil {
				return err
			}
		}
	}
	// config_entries: array of (name, nullable value)
	c, err := dec.Int32()
	if err != nil {
		return err
	}
	for i := int32(0); i < c; i++ {
		if _, err := dec.String(); err != nil {
			return err
		}
		if _, err := dec.String(); err != nil {
			return err
		}
	}
	return nil
}
