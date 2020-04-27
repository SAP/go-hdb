/*
Copyright 2020 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package encoding

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/SAP/go-hdb/internal/unicode"
	"golang.org/x/text/transform"
)

const writeScratchSize = 4096

// Encoder encodes hdb protocol datatypes an basis of an io.Writer.
type Encoder struct {
	wr  io.Writer
	err error
	b   []byte // scratch buffer (min 8 Bytes)
	tr  transform.Transformer
}

// NewEncoder creates a new Encoder instance.
func NewEncoder(wr io.Writer) *Encoder {
	return &Encoder{
		wr: wr,
		b:  make([]byte, writeScratchSize),
		tr: unicode.Utf8ToCesu8Transformer,
	}
}

// Zeroes writes cnt zero byte values.
func (e *Encoder) Zeroes(cnt int) {
	if e.err != nil {
		return
	}

	// zero out scratch area
	l := cnt
	if l > len(e.b) {
		l = len(e.b)
	}
	for i := 0; i < l; i++ {
		e.b[i] = 0
	}

	for i := 0; i < cnt; {
		j := cnt - i
		if j > len(e.b) {
			j = len(e.b)
		}
		n, _ := e.wr.Write(e.b[:j])
		if n != j {
			return
		}
		i += n
	}
}

// Bytes writes a bytes slice.
func (e *Encoder) Bytes(p []byte) {
	if e.err != nil {
		return
	}
	e.wr.Write(p)
}

// Byte writes a byte.
func (e *Encoder) Byte(b byte) { // WriteB as sig differs from WriteByte (vet issues)
	if e.err != nil {
		return
	}
	e.b[0] = b
	e.Bytes(e.b[:1])
}

// Bool writes a boolean.
func (e *Encoder) Bool(v bool) {
	if e.err != nil {
		return
	}
	if v {
		e.Byte(1)
	} else {
		e.Byte(0)
	}
}

// Int8 writes an int8.
func (e *Encoder) Int8(i int8) {
	if e.err != nil {
		return
	}
	e.Byte(byte(i))
}

// Int16 writes an int16.
func (e *Encoder) Int16(i int16) {
	if e.err != nil {
		return
	}
	binary.LittleEndian.PutUint16(e.b[:2], uint16(i))
	e.wr.Write(e.b[:2])
}

// Uint16 writes an uint16.
func (e *Encoder) Uint16(i uint16) {
	if e.err != nil {
		return
	}
	binary.LittleEndian.PutUint16(e.b[:2], i)
	e.wr.Write(e.b[:2])
}

// Int32 writes an int32.
func (e *Encoder) Int32(i int32) {
	if e.err != nil {
		return
	}
	binary.LittleEndian.PutUint32(e.b[:4], uint32(i))
	e.wr.Write(e.b[:4])
}

// Uint32 writes an uint32.
func (e *Encoder) Uint32(i uint32) {
	if e.err != nil {
		return
	}
	binary.LittleEndian.PutUint32(e.b[:4], i)
	e.wr.Write(e.b[:4])
}

// Int64 writes an int64.
func (e *Encoder) Int64(i int64) {
	if e.err != nil {
		return
	}
	binary.LittleEndian.PutUint64(e.b[:8], uint64(i))
	e.wr.Write(e.b[:8])
}

// Uint64 writes an uint64.
func (e *Encoder) Uint64(i uint64) {
	if e.err != nil {
		return
	}
	binary.LittleEndian.PutUint64(e.b[:8], i)
	e.wr.Write(e.b[:8])
}

// Float32 writes a float32.
func (e *Encoder) Float32(f float32) {
	if e.err != nil {
		return
	}
	bits := math.Float32bits(f)
	binary.LittleEndian.PutUint32(e.b[:4], bits)
	e.wr.Write(e.b[:4])
}

// Float64 writes a float64.
func (e *Encoder) Float64(f float64) {
	if e.err != nil {
		return
	}
	bits := math.Float64bits(f)
	binary.LittleEndian.PutUint64(e.b[:8], bits)
	e.wr.Write(e.b[:8])
}

// String writes a string.
func (e *Encoder) String(s string) {
	if e.err != nil {
		return
	}
	e.Bytes([]byte(s))
}

// CESU8Bytes writes an UTF-8 byte slice as CESU-8 and returns the CESU-8 bytes written.
func (e *Encoder) CESU8Bytes(p []byte) int {
	if e.err != nil {
		return 0
	}
	e.tr.Reset()
	cnt := 0
	i := 0
	for i < len(p) {
		m, n, err := e.tr.Transform(e.b, p[i:], true)
		if err != nil && err != transform.ErrShortDst {
			e.err = err
			return cnt
		}
		if m == 0 {
			e.err = transform.ErrShortDst
			return cnt
		}
		o, _ := e.wr.Write(e.b[:m])
		cnt += o
		i += n
	}
	return cnt
}

// CESU8String is like WriteCesu8 with an UTF-8 string as parameter.
func (e *Encoder) CESU8String(s string) int {
	return e.CESU8Bytes([]byte(s))
}
