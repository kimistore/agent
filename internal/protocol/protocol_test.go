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
	"testing"
)

func TestEncoderDecoder_Primitives(t *testing.T) {
	enc := NewEncoder()

	enc.Int16(123)
	enc.Int32(456789)
	enc.Int64(9876543210)
	enc.String("hello")

	data := enc.Bytes()
	dec := NewDecoder(data)

	v16, err := dec.Int16()
	if err != nil || v16 != 123 {
		t.Errorf("Int16 failed: %v, %d", err, v16)
	}

	v32, err := dec.Int32()
	if err != nil || v32 != 456789 {
		t.Errorf("Int32 failed: %v, %d", err, v32)
	}

	v64, err := dec.Int64()
	if err != nil || v64 != 9876543210 {
		t.Errorf("Int64 failed: %v, %d", err, v64)
	}

	str, err := dec.String()
	if err != nil || str != "hello" {
		t.Errorf("String failed: %v, %s", err, str)
	}
}
