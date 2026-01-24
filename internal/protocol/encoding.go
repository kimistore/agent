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
	"errors"
)

type Decoder struct {
	data []byte
	off  int
}

func NewDecoder(data []byte) *Decoder {
	return &Decoder{data: data, off: 0}
}

func (d *Decoder) Int16() (int16, error) {
	if d.remaining() < 2 {
		return 0, errors.New("insufficient data for int16")
	}
	val := int16(binary.BigEndian.Uint16(d.data[d.off:]))
	d.off += 2
	return val, nil
}

func (d *Decoder) Int32() (int32, error) {
	if d.remaining() < 4 {
		return 0, errors.New("insufficient data for int32")
	}
	val := int32(binary.BigEndian.Uint32(d.data[d.off:]))
	d.off += 4
	return val, nil
}

func (d *Decoder) String() (string, error) {
	lenVal, err := d.Int16()
	if err != nil {
		return "", err
	}
	if lenVal == -1 {
		return "", nil // Null string
	}
	length := int(lenVal)
	if d.remaining() < length {
		return "", errors.New("insufficient data for string")
	}
	str := string(d.data[d.off : d.off+length])
	d.off += length
	return str, nil
}

func (d *Decoder) remaining() int {
	return len(d.data) - d.off
}

func (d *Decoder) Int64() (int64, error) {
	if d.remaining() < 8 {
		return 0, errors.New("insufficient data for int64")
	}
	val := int64(binary.BigEndian.Uint64(d.data[d.off:]))
	d.off += 8
	return val, nil
}

func (d *Decoder) Bytes() ([]byte, error) {
	lenVal, err := d.Int32()
	if err != nil {
		return nil, err
	}
	if lenVal == -1 {
		return nil, nil // Null bytes
	}
	length := int(lenVal)
	if d.remaining() < length {
		return nil, errors.New("insufficient data for bytes")
	}
	b := d.data[d.off : d.off+length]
	d.off += length
	return b, nil
}

type Encoder struct {
	data []byte
}

func NewEncoder() *Encoder {
	return &Encoder{data: make([]byte, 0, 1024)}
}

func (e *Encoder) Int8(val int8) {
	e.data = append(e.data, byte(val))
}

func (e *Encoder) Int16(val int16) {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(val))
	e.data = append(e.data, buf...)
}

func (e *Encoder) Int32(val int32) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(val))
	e.data = append(e.data, buf...)
}

func (e *Encoder) Int64(val int64) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(val))
	e.data = append(e.data, buf...)
}

func (e *Encoder) String(val string) {
	e.Int16(int16(len(val)))
	e.data = append(e.data, []byte(val)...)
}

func (e *Encoder) PutBytes(val []byte) {
	if val == nil {
		e.Int32(-1)
		return
	}
	e.Int32(int32(len(val)))
	e.data = append(e.data, val...)
}

func (e *Encoder) Bytes() []byte {
	return e.data
}
