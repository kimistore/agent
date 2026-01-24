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
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"log"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

func CountMessageSet(data []byte) int {
	count := 0
	pos := 0
	// MessageSet Entry: Offset(8) + Size(4) + Msg(Size)
	for pos <= len(data)-12 {
		// Offset is data[pos : pos+8] (We don't need value)
		// Size is data[pos+8 : pos+12]
		size := binary.BigEndian.Uint32(data[pos+8 : pos+12])

		totalLen := 12 + int(size)
		if pos+totalLen > len(data) {
			break
		}

		// Check for compression in the Message
		// Message: CRC(4) | Magic(1) | Attributes(1) ...
		// MsgContent starts at pos + 12
		msgStart := pos + 12
		msgLen := int(size)
		if msgLen >= 6 {
			// attributes is at msgStart + 5
			attributes := data[msgStart+5]
			compression := attributes & 0x07

			if compression != 0 {
				// Decompress and recursively count
				// We need to extract the Value field from the message.
				// Based on Magic byte (msgStart+4)
				magic := data[msgStart+4]
				headerSize := 6 // CRC(4) + Magic(1) + Attribute(1)
				if magic >= 1 {
					headerSize += 8 // Timestamp(8)
				}

				if msgLen > headerSize+4 { // At least KeySize needed
					keySize := int32(binary.BigEndian.Uint32(data[msgStart+headerSize : msgStart+headerSize+4]))
					headerSize += 4
					if keySize != -1 {
						headerSize += int(keySize)
					}

					if msgLen >= headerSize+4 { // ValueSize needed
						valSize := int32(binary.BigEndian.Uint32(data[msgStart+headerSize : msgStart+headerSize+4]))
						headerSize += 4

						if valSize != -1 && msgLen >= headerSize+int(valSize) {
							valData := data[msgStart+headerSize : msgStart+headerSize+int(valSize)]
							decompressed, err := decompress(compression, valData)
							if err == nil {
								// log.Printf("Decompressed %d bytes using codec %d", len(decompressed), compression)
								// Recursively count the inner message set
								innerCount := CountMessageSet(decompressed)
								// log.Printf("Inner count: %d", innerCount)
								count += innerCount
								pos += totalLen
								continue
							} else {
								log.Printf("Decompression error: %v", err)
								// Fallback to counting the wrapper as 1
							}
						}
					}
				}
			}
		}

		count++
		pos += totalLen
	}
	// log.Printf("Total count: %d", count)
	return count
}

func decompress(codec byte, in []byte) ([]byte, error) {
	switch codec {
	case 1: // GZIP
		r, err := gzip.NewReader(bytes.NewReader(in))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	case 2: // Snappy
		return snappy.Decode(nil, in)
	case 3: // LZ4
		r := lz4.NewReader(bytes.NewReader(in))
		return io.ReadAll(r)
	default:
		return nil, fmt.Errorf("unsupported compression codec: %d", codec)
	}
}
