package protocol

import (
	"encoding/binary"
	"testing"
)

func TestCountMessageSet(t *testing.T) {
	// 1. Empty
	if n := countMessageSet([]byte{}); n != 0 {
		t.Errorf("expected 0 for empty, got %d", n)
	}

	// 2. Single Message
	// Offset(8) + Size(4) + Body(Size)
	msg1Body := []byte("hello")
	msg1 := make([]byte, 12+len(msg1Body))
	binary.BigEndian.PutUint64(msg1[0:8], 0)
	binary.BigEndian.PutUint32(msg1[8:12], uint32(len(msg1Body)))
	copy(msg1[12:], msg1Body)

	if n := countMessageSet(msg1); n != 1 {
		t.Errorf("expected 1, got %d", n)
	}

	// 3. Two Messages
	msg2Body := []byte("world")
	msg2 := make([]byte, 12+len(msg2Body))
	binary.BigEndian.PutUint64(msg2[0:8], 1)
	binary.BigEndian.PutUint32(msg2[8:12], uint32(len(msg2Body)))
	copy(msg2[12:], msg2Body)

	batch := append(msg1, msg2...)
	if n := countMessageSet(batch); n != 2 {
		t.Errorf("expected 2, got %d", n)
	}

	// 4. Corrupt/Incomplete (Trailing bytes)
	incomplete := append(batch, []byte{0, 0, 0}...)
	if n := countMessageSet(incomplete); n != 2 {
		t.Errorf("expected 2 for incomplete tail, got %d", n)
	}
}

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
