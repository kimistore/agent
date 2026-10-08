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
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"kimistore/internal/storage"
)

// TestReadErrorCode_DistinguishesTheThreeFailures pins the mapping that stops a
// storage fault from being reported as a bad offset.
//
// Reporting OFFSET_OUT_OF_RANGE for every read failure tells a consumer its
// position is invalid when the position is fine and the broker is broken. The
// consumer resets and retries, and one record cost 1222 round trips before this
// was traced to a store that could not serve a range read.
func TestReadErrorCode_DistinguishesTheThreeFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int16
	}{
		{"no error", nil, ErrNone},
		{"offset genuinely gone", fmt.Errorf("%w: offset 5", storage.ErrOffsetUnavailable), ErrOffsetOutOfRange},
		{"offset gone, wrapped further", fmt.Errorf("read: %w", storage.ErrOffsetUnavailable), ErrOffsetOutOfRange},
		{"client disconnected", context.Canceled, ErrUnknown},
		{"deadline passed", context.DeadlineExceeded, ErrRequestTimedOut},
		{"object store failed", errors.New("dial tcp: connection refused"), ErrKafkaStorageError},
		{"store does not support range reads", storage.ErrUnsupported, ErrKafkaStorageError},
		{"agent lost the partition", storage.ErrPartitionLost, ErrKafkaStorageError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := readErrorCode(tc.err); got != tc.want {
				t.Errorf("readErrorCode(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

// TestReadErrorCode_StorageFailureIsNotAReset is the property that matters to a
// consumer: only a genuinely missing offset may make it move.
func TestReadErrorCode_StorageFailureIsNotAReset(t *testing.T) {
	for _, err := range []error{
		errors.New("S3Error: NoSuchBucket"),
		context.DeadlineExceeded,
		storage.ErrLeaseLost,
	} {
		if got := readErrorCode(err); got == ErrOffsetOutOfRange {
			t.Errorf("readErrorCode(%v) reports OFFSET_OUT_OF_RANGE, which would make a consumer reset", err)
		}
	}
}

// TestRefuseMetadata_IsParseableAtEveryVersion decodes the refusal with kmsg.
//
// MetadataResponse puts the broker list, cluster id and controller id BEFORE
// the topic array. The first attempt at this wrote only the topics, so a client
// read the topic count as a broker count, retried metadata forever, and the
// caller saw a timeout instead of an authorization error. Decoding here is what
// makes that failure mode a failing test rather than a field report.
func TestRefuseMetadata_IsParseableAtEveryVersion(t *testing.T) {
	brokers := []storage.Broker{{NodeID: 1, Host: "broker-1", Port: 19092}}
	const code = ErrTopicAuthorizationFailed

	for _, version := range []int16{0, 1, 2, 3, 6, 7} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			topics := []string{"orders", "payments"}
			enc := NewEncoder()
			body := refuseMetadata(enc, version, topics, brokers, 1, code)

			var mr kmsg.MetadataResponse
			mr.SetVersion(version)
			if err := mr.ReadFrom(body); err != nil {
				t.Fatalf("kmsg could not parse the refusal: %v", err)
			}

			// The broker list has to survive: a client with nowhere to send a
			// request cannot tell an authorization failure from a dead cluster.
			if len(mr.Brokers) != 1 {
				t.Fatalf("got %d brokers, want 1", len(mr.Brokers))
			}
			if mr.Brokers[0].Host != "broker-1" || mr.Brokers[0].Port != 19092 {
				t.Errorf("broker = %s:%d, want broker-1:19092", mr.Brokers[0].Host, mr.Brokers[0].Port)
			}
			if mr.Brokers[0].NodeID != 1 {
				t.Errorf("broker node id = %d, want 1", mr.Brokers[0].NodeID)
			}

			if len(mr.Topics) != len(topics) {
				t.Fatalf("got %d topics, want %d", len(mr.Topics), len(topics))
			}
			for i, topic := range topics {
				if mr.Topics[i].Topic == nil || *mr.Topics[i].Topic != topic {
					t.Errorf("topic[%d] = %v, want %q", i, mr.Topics[i].Topic, topic)
				}
				if mr.Topics[i].ErrorCode != code {
					t.Errorf("topic %q error = %d, want %d", topic, mr.Topics[i].ErrorCode, code)
				}
				// A client reads the partition errors, so the topic-level code
				// alone can leave it retrying.
				if len(mr.Topics[i].Partitions) == 0 {
					t.Fatalf("topic %q has no partitions, so the error has nowhere to live", topic)
				}
				if mr.Topics[i].Partitions[0].ErrorCode != code {
					t.Errorf("partition error = %d, want %d", mr.Topics[i].Partitions[0].ErrorCode, code)
				}
				if mr.Topics[i].Partitions[0].Leader != -1 {
					t.Errorf("leader = %d, want -1: a refused topic must not route to a broker that would refuse again",
						mr.Topics[i].Partitions[0].Leader)
				}
			}

			// The controller id must name a broker from the list, or a client
			// will look up an id nobody advertised.
			if version >= 1 && mr.ControllerID != 1 {
				t.Errorf("controller id = %d, want 1", mr.ControllerID)
			}
		})
	}
}
