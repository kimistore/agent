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
	"errors"

	"kimistore/internal/storage"
)

// Peek helpers for authorization.
//
// Produce and Fetch both carry a topic array inside a larger request, and both
// have to answer in a schema that is not simply "error code". So the
// authorization check reads the topic names from a clone of the decoder,
// refuses if any topic is not permitted, and the handler then either encodes a
// complete refusal or proceeds to decode the request for real.

// peekProduceTopics collects the topic names from a Produce request that has
// already had its header decoded.
//
// The transactional id, acks and timeout are NOT read here. handleProduce has
// consumed them by the time this runs, so re-reading them walks the peek two
// fields past the topic array and every partition size after that is read from
// the wrong offset.
func peekProduceTopics(dec *Decoder, topicCount int32) ([]string, error) {
	peek := dec.Clone()
	topics := make([]string, 0, topicCount)
	for i := int32(0); i < topicCount; i++ {
		name, err := peek.String()
		if err != nil {
			return nil, err
		}
		pCount, err := peek.Int32()
		if err != nil {
			return nil, err
		}
		for j := int32(0); j < pCount; j++ {
			if _, err := peek.Int32(); err != nil { // partition index
				return nil, err
			}
			// Records are a nullable bytes: int32 length then the payload.
			size, err := peek.Int32()
			if err != nil {
				return nil, err
			}
			if size < 0 {
				return nil, errors.New("negative record batch size")
			}
			if err := peek.SkipN(int(size)); err != nil {
				return nil, err
			}
		}
		topics = append(topics, name)
	}
	return topics, nil
}

// peekFetchTopics walks a Fetch request far enough to collect its topic names.
func peekFetchTopics(dec *Decoder, version int16) ([]string, error) {
	peek := dec.Clone()
	// ReplicaId
	if _, err := peek.Int32(); err != nil {
		return nil, err
	}
	// MaxWaitMs
	if _, err := peek.Int32(); err != nil {
		return nil, err
	}
	// MinBytes
	if _, err := peek.Int32(); err != nil {
		return nil, err
	}
	if version >= 3 {
		// MaxBytes (v3+)
		if _, err := peek.Int32(); err != nil {
			return nil, err
		}
	}
	if version >= 4 {
		// IsolationLevel, SessionId, SessionEpoch
		if _, err := peek.Int8(); err != nil {
			return nil, err
		}
		if _, err := peek.Int32(); err != nil {
			return nil, err
		}
		if _, err := peek.Int32(); err != nil {
			return nil, err
		}
	}
	if version >= 7 {
		// RackId
		if _, err := peek.String(); err != nil {
			return nil, err
		}
	}
	if version >= 11 {
		// ReplicaState: array of (topic, partition)
		n, err := peek.Int32()
		if err != nil {
			return nil, err
		}
		for i := int32(0); i < n; i++ {
			if _, err := peek.String(); err != nil {
				return nil, err
			}
			if _, err := peek.Int32(); err != nil {
				return nil, err
			}
		}
	}

	count, err := peek.Int32()
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0, count)
	for i := int32(0); i < count; i++ {
		name, err := peek.String()
		if err != nil {
			return nil, err
		}
		pCount, err := peek.Int32()
		if err != nil {
			return nil, err
		}
		for j := int32(0); j < pCount; j++ {
			if _, err := peek.Int32(); err != nil { // partition
				return nil, err
			}
			if _, err := peek.Int64(); err != nil { // current_leader_epoch
				return nil, err
			}
			if _, err := peek.Int64(); err != nil { // fetch_offset
				return nil, err
			}
			if _, err := peek.Int32(); err != nil { // partition_max_bytes
				return nil, err
			}
		}
		topics = append(topics, name)
	}
	return topics, nil
}

// refuseProduce encodes a ProduceResponse refusing every topic in the request.
//
// The shape matters. A client that expects a response array and gets a bare
// error code may treat it as a protocol error and reconnect, which hides the
// real answer. Kafka answers a refused produce with the topic array present and
// every partition carrying an error, so the refusal is expressed that way.
func refuseProduce(enc *Encoder, version int16, topics []string, code int16) []byte {
	enc.Int32(int32(len(topics)))
	for _, t := range topics {
		enc.String(t)
		enc.Int32(1)    // one partition so the error has somewhere to live
		enc.Int32(0)    // partition 0; the refusal is about the topic, not a partition
		enc.Int16(code) // error code
		enc.Int64(-1)   // base offset
		enc.Int64(-1)   // log append time / log start offset
	}
	if version >= 1 {
		// ThrottleTimeMs trails the response array in ProduceResponse.
		enc.Int32(0)
	}
	return enc.Bytes()
}

// refuseFetch encodes a FetchResponse refusing every requested topic.
//
// FetchResponse leads with ThrottleTimeMs from v1, unlike ProduceResponse which
// trails it. Getting that backwards shifts every following field by four bytes,
// which the client reads as a garbled topic array.
func refuseFetch(enc *Encoder, version int16, topics []string, code int16) []byte {
	if version >= 1 {
		enc.Int32(0) // ThrottleTimeMs
	}
	enc.Int32(int32(len(topics)))
	for _, t := range topics {
		enc.String(t)
		enc.Int32(1)    // one partition so the error has somewhere to live
		enc.Int32(0)    // partition 0
		enc.Int16(code) // error code
		enc.Int64(-1)   // high watermark: unknown rather than guessed
		if version >= 4 {
			enc.Int64(-1) // last stable offset
		}
		if version >= 5 {
			enc.Int64(-1) // log start offset
		}
		if version >= 4 {
			enc.Int32(0) // aborted transactions: none
		}
		enc.PutBytes(nil) // no records
	}
	return enc.Bytes()
}

// refuseMetadata encodes a MetadataResponse that refuses every requested topic.
//
// The whole prelude has to be present. MetadataResponse is the one response
// where the broker list, cluster id and controller id all come BEFORE the topic
// array. A refusal that wrote only the topics left the client reading the topic
// count as a broker count; it then retried metadata forever and the caller saw a
// timeout instead of an authorisation error. That is worse than not enforcing,
// because it looks like an outage.
//
// The broker list is the live view, not an empty one. A client that has nowhere
// to send a request because of a refusal has to be able to tell that apart from
// a cluster with no brokers, and the two look different on the wire.
func refuseMetadata(enc *Encoder, version int16, topics []string, brokers []storage.Broker, nodeID int32, code int16) []byte {
	if version >= 3 {
		enc.Int32(0) // ThrottleTimeMs
	}

	// Brokers
	enc.Int32(int32(len(brokers)))
	for _, b := range brokers {
		enc.Int32(b.NodeID)
		enc.String(b.Host)
		enc.Int32(b.Port)
		if version >= 1 {
			enc.String("") // Rack
		}
	}

	// ClusterId: nullable from v2.
	if version >= 2 {
		enc.String(clusterID)
	}

	// ControllerId: must be an id from the broker list above.
	if version >= 1 {
		enc.Int32(nodeID)
	}

	// Topics
	// Field order is the schema's declaration order, not a convenient one.
	// At topic level the error code precedes the name; at partition level it
	// precedes the partition number. Both were written after the name here, and
	// a client then read a two-byte code as the high half of a string length and
	// failed to parse the whole response.
	enc.Int32(int32(len(topics)))
	for _, t := range topics {
		enc.Int16(code) // topic-level error, first
		enc.String(t)   // then the name
		if version >= 1 {
			// is_internal sits between the name and the partition array.
			enc.Int8(0)
		}
		// One partition, carrying the same error. A client reads the partition
		// error codes, so a topic-level code alone can leave it retrying.
		enc.Int32(1)    // partition count
		enc.Int16(code) // partition-level error, first
		enc.Int32(0)    // partition 0
		enc.Int32(-1)   // Leader: unknown rather than a broker that would refuse
		if version >= 7 {
			enc.Int32(-1) // LeaderEpoch
		}
		enc.Int32(0) // replicas
		enc.Int32(0) // in-sync replicas
		if version >= 5 {
			enc.Int32(0) // offline replicas
		}
	}
	return enc.Bytes()
}

// readErrorCode maps a storage read failure to the code that tells a client what
// to do about it.
//
// The distinction that matters is between "your offset is wrong" and "the broker
// cannot serve right now". Only the first justifies a consumer resetting its
// position, and answering the second with a reset turns a transient storage
// fault into an unbounded retry loop.
func readErrorCode(err error) int16 {
	switch {
	case err == nil:
		return ErrNone
	case errors.Is(err, storage.ErrOffsetUnavailable):
		// The offset is genuinely gone: retention reclaimed it, or it is before
		// the start of the log. The client should reset.
		return ErrOffsetOutOfRange
	case errors.Is(err, context.Canceled):
		// The client went away. Nothing to report and nothing to retry; the
		// connection is going to close regardless.
		return ErrUnknown
	case errors.Is(err, context.DeadlineExceeded):
		return ErrRequestTimedOut
	default:
		// The object store failed. KAFKA_STORAGE_ERROR is retriable, so the
		// client asks again rather than resetting its position, which is the
		// correct response to a broker that is broken rather than to a log
		// that has moved.
		return ErrKafkaStorageError
	}
}
