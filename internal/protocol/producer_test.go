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
	"encoding/binary"
	"testing"
)

// TestInitProducerIdAllocates checks the request an idempotence-enabled client
// sends before its first produce is answered with a fresh producer id and epoch
// 0, and that a second call does not hand out the same id.
func TestInitProducerIdAllocates(t *testing.T) {
	se := testStore(t)
	cfg := testConfig()

	// V1 request: transactional_id (null string) | transaction_timeout_ms.
	body := []byte{0xFF, 0xFF}
	var timeout [4]byte
	binary.BigEndian.PutUint32(timeout[:], 60_000)
	body = append(body, timeout[:]...)

	getID := func() (int64, int16) {
		t.Helper()
		dec := dispatch(t, se, cfg, ApiKeyInitProducerID, 1, body)
		if throttle, _ := dec.Int32(); throttle != 0 {
			t.Errorf("throttle = %d, want 0", throttle)
		}
		code, _ := dec.Int16()
		if code != ErrNone {
			t.Fatalf("InitProducerId error = %d, want 0", code)
		}
		pid, _ := dec.Int64()
		epoch, _ := dec.Int16()
		return pid, epoch
	}

	pid, epoch := getID()
	if pid < 1 {
		t.Errorf("producer id = %d, want >= 1", pid)
	}
	if epoch != 0 {
		t.Errorf("producer epoch = %d, want 0", epoch)
	}

	pid2, _ := getID()
	if pid2 == pid {
		t.Errorf("second producer id = %d, same as the first (%d)", pid2, pid)
	}
}

// TestInitProducerIdIsAdvertised checks a client can discover the API before
// using it; without this an idempotence-enabled client disables idempotence.
func TestInitProducerIdIsAdvertised(t *testing.T) {
	se := testStore(t)

	dec := dispatch(t, se, testConfig(), ApiKeyApiVersions, 0, nil)
	if code, _ := dec.Int16(); code != ErrNone {
		t.Fatalf("ApiVersions error = %d", code)
	}
	n, _ := dec.Int32()
	found := false
	for i := int32(0); i < n; i++ {
		key, _ := dec.Int16()
		_, _ = dec.Int16()
		_, _ = dec.Int16()
		if key == ApiKeyInitProducerID {
			found = true
		}
	}
	if !found {
		t.Error("ApiVersions does not advertise InitProducerId")
	}
}
