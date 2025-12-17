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
