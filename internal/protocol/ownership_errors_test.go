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
	"errors"
	"testing"

	"kimistore/internal/storage"
)

// A produce this agent cannot fence has to come back as NOT_LEADER_OR_FOLLOWER,
// not as a generic failure. That is the code that tells a producer to refresh its
// metadata and retry against whichever agent does own the partition; a generic
// error tells it the request failed for a reason a retry will not fix, so the
// record is lost instead of retried.
func TestProduceError_PartitionRefusalsAreRetriable(t *testing.T) {
	for _, err := range []error{
		storage.ErrPartitionHeld,
		storage.ErrPartitionNotOwned,
		storage.ErrPartitionLost,
	} {
		if got := produceError(err); got != ErrNotLeaderForPartition {
			t.Errorf("produceError(%v) = %d, want %d (NOT_LEADER_OR_FOLLOWER)",
				err, got, ErrNotLeaderForPartition)
		}
		// Wrapped errors must map the same way: the storage layer adds the
		// partition and epoch to the message, so callers see a wrapped sentinel.
		wrapped := errors.Join(err, errors.New("orders/3 lost its claim at epoch 4"))
		if got := produceError(wrapped); got != ErrNotLeaderForPartition {
			t.Errorf("produceError(wrapped %v) = %d, want %d", err, got, ErrNotLeaderForPartition)
		}
	}
}

// The other fences still map to the same code: a client cannot act differently
// depending on which claim it lost.
func TestProduceError_LostFencesAreRetriable(t *testing.T) {
	for _, err := range []error{storage.ErrLeaseLost, storage.ErrLeaseHeld} {
		if got := produceError(err); got != ErrNotLeaderForPartition {
			t.Errorf("produceError(%v) = %d, want %d", err, got, ErrNotLeaderForPartition)
		}
	}
}
