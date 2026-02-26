package auth

import (
	"bytes"
	"testing"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"golang.org/x/text/transform"
)

// "SAP HANA SQL Command Network Protocol Reference" v1.2, chapter 2.3.7.20
// AUTHENTICATION Part Data Format
//
// The specification defines two encodings, one for data <= 250 bytes and one for data > 250 bytes.
func TestDecoderBytesSmallerOrEqual250(t *testing.T) {
	tests := []struct {
		name        string
		size_header []byte
		size        int
	}{
		{"100 bytes", []byte{0x64}, 100},
		{"250 bytes", []byte{0xfa}, 250},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			payload := bytes.Repeat([]byte{0}, tt.size)
			wire := append(tt.size_header, payload...)
			dec := NewDecoder(encoding.NewDecoder(bytes.NewReader(wire), transform.Nop, false))
			
			// Act
			result := dec.bytes()

			// Assert
			if !bytes.Equal(result, payload) {
				t.Fatalf("got %d bytes, want %d", len(result), len(payload))
			}
			
		})
	}
}

func TestDecoderBytesGreater250(t *testing.T) {
	// Arrange
	payload := bytes.Repeat([]byte{0}, 400)

	// subPrmsSize wire encoding for size 400:
	//   0xff       = 2-byte size follows
	//   0x01, 0x90 = 400 (big-endian uint16)
	wire := append([]byte{0xff, 0x01, 0x90}, payload...)

	dec := NewDecoder(encoding.NewDecoder(bytes.NewReader(wire), transform.Nop, false))

	// Act
	result := dec.bytes()

	// Assert
	if !bytes.Equal(result, payload) {
		t.Fatalf("got %d bytes, want %d", len(result), len(payload))
	}
}
