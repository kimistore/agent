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

type Encoder struct {
	data []byte
}

func NewEncoder() *Encoder {
	return &Encoder{data: make([]byte, 0, 1024)}
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

func (e *Encoder) String(val string) {
	e.Int16(int16(len(val)))
	e.data = append(e.data, []byte(val)...)
}

func (e *Encoder) Bytes() []byte {
	return e.data
}
