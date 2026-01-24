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

package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Entry represents one index entry mapping relative offset to file position
type Entry struct {
	RelativeOffset int32
	Position       int32
}

const EntrySize = 8

// GenerateIndex scans a log file and produces an index buffer.
// For MVP, we index every message (or every N bytes).
// Kafka usually indexes every 4KB.
func GenerateIndex(logPath string, baseOffset int64) ([]byte, error) {
	f, err := os.Open(logPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var buf bytes.Buffer
	var position int64 = 0

	// Reusing WAL reading logic or simple scanning
	// We need to parse batches to know offsets.

	// Naive scan: Read header, skip body.
	header := make([]byte, 12) // Offset(8) + Size(4)

	// Threshold for indexing: Index at least every 4KB of data
	var lastIndexedPos int64 = 0

	for {
		// Record start of message
		msgStartPos := position

		if _, err := io.ReadFull(f, header); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		offset := int64(binary.BigEndian.Uint64(header[0:8]))
		size := binary.BigEndian.Uint32(header[8:12])

		// Skip body
		if _, err := f.Seek(int64(size), io.SeekCurrent); err != nil {
			return nil, err
		}

		// Update position
		position += 12 + int64(size)

		// Decide to index?
		if position-lastIndexedPos >= 4096 || lastIndexedPos == 0 {
			relOffset := int32(offset - baseOffset)
			pos := int32(msgStartPos)

			// Write Index Entry
			binary.Write(&buf, binary.BigEndian, relOffset)
			binary.Write(&buf, binary.BigEndian, pos)

			lastIndexedPos = position
		}
	}

	return buf.Bytes(), nil
}

// Lookup finds the file position for the given target offset using the index data.
// It returns the position of the nearest offset <= targetOffset.
func Lookup(indexData []byte, targetOffset int64, baseOffset int64) (int64, error) {
	if len(indexData)%EntrySize != 0 {
		return 0, fmt.Errorf("corrupt index data")
	}

	count := len(indexData) / EntrySize
	targetRel := int32(targetOffset - baseOffset)

	// Binary Search
	// Find largest entry.RelOffset <= targetRel

	var bestPos int32 = 0

	low := 0
	high := count - 1

	for low <= high {
		mid := (low + high) / 2
		offset := mid * EntrySize

		relOff := int32(binary.BigEndian.Uint32(indexData[offset : offset+4]))
		pos := int32(binary.BigEndian.Uint32(indexData[offset+4 : offset+8]))

		if relOff <= targetRel {
			bestPos = pos
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return int64(bestPos), nil
}
