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

package wal

import (
	"fmt"
	"strconv"
	"strings"
)

// A sealed segment is named after the offset it starts at, so a directory (or
// an object-store prefix) lists in log order. Under a single writer that is the
// whole story: base offsets never repeat, so the name identifies the segment.
//
// With per-partition ownership they can. The epoch that wrote a segment goes
// into the name, "<baseOffset>-e<epoch>", because a stale owner that has lost
// its claim must not be able to overwrite the segment its successor wrote at
// the same base offset. Different names mean the two uploads land side by side
// and the collision the writer lease exists to prevent cannot happen even if
// the fence is momentarily believed to still hold.
//
// The cost is that a name is no longer a bare integer, so every reader of a
// segment name has to parse it rather than ParseInt it. ParseSegmentName
// accepts both shapes, so segments written before ownership existed keep
// working: they are epoch 0, which sorts below any real epoch.
const epochSeparator = "-e"

// SegmentName is the file and object name for a sealed segment. An epoch of
// zero or less omits the suffix, which is what an agent running without
// ownership writes and what every pre-ownership segment already in a bucket is
// named.
func SegmentName(baseOffset, epoch int64) string {
	if epoch <= 0 {
		return fmt.Sprintf("%020d.log", baseOffset)
	}
	return fmt.Sprintf("%020d-e%d.log", baseOffset, epoch)
}

// ParseSegmentName reads a sealed segment name back into its base offset and
// the epoch that wrote it. A name without an epoch suffix parses as epoch 0.
//
// ok is false for anything that is not a sealed segment name, including
// "active.log", so a caller listing a partition directory can skip the active
// segment with the same check that used to compare the name literally.
func ParseSegmentName(name string) (baseOffset, epoch int64, ok bool) {
	base := strings.TrimSuffix(name, ".log")
	if base == name {
		return 0, 0, false
	}
	if i := strings.LastIndex(base, epochSeparator); i >= 0 {
		off, err := strconv.ParseInt(base[:i], 10, 64)
		if err != nil {
			return 0, 0, false
		}
		ep, err := strconv.ParseInt(base[i+len(epochSeparator):], 10, 64)
		if err != nil {
			return 0, 0, false
		}
		return off, ep, true
	}
	off, err := strconv.ParseInt(base, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return off, 0, true
}
