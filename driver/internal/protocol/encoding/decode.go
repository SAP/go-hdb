// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/big"

	"golang.org/x/text/transform"
)

const readScratchSize = 4096

// Decoder decodes hdb protocol datatypes an basis of an io.Reader.
type Decoder struct {
	rd io.Reader
	/* err: fatal read error
	- not set by conversion errors
	- conversion errors are returned by the reader function itself
	*/
	err error
	b   []byte // scratch buffer (used for skip, CESU8Bytes - define size not too small!)
	tr  transform.Transformer
	cnt int
	dfv int
}

// NewDecoder creates a new Decoder instance based on an io.Reader.
func NewDecoder(rd io.Reader, decoder func() transform.Transformer) *Decoder {
	return &Decoder{
		rd: rd,
		b:  make([]byte, readScratchSize),
		tr: decoder(),
	}
}

// Dfv returns the data format version.
func (d *Decoder) Dfv() int {
	return d.dfv
}

// SetDfv sets the data format version.
func (d *Decoder) SetDfv(dfv int) {
	d.dfv = dfv
}

// ResetCnt resets the byte read counter.
func (d *Decoder) ResetCnt() {
	d.cnt = 0
}

// Cnt returns the value of the byte read counter.
func (d *Decoder) Cnt() int {
	return d.cnt
}

// Error returns the reader error.
func (d *Decoder) Error() error {
	return d.err
}

// ResetError return and resets reader error.
func (d *Decoder) ResetError() error {
	err := d.err
	d.err = nil
	return err
}

// readFull reads data from reader + read counter and error handling
func (d *Decoder) readFull(buf []byte) (int, error) {
	if d.err != nil {
		return 0, d.err
	}
	var n int
	n, d.err = io.ReadFull(d.rd, buf)
	d.cnt += n
	if d.err != nil {
		return n, d.err
	}
	return n, nil
}

// Skip skips cnt bytes from reading.
func (d *Decoder) Skip(cnt int) {
	var n int
	for n < cnt {
		to := cnt - n
		if to > readScratchSize {
			to = readScratchSize
		}
		m, err := d.readFull(d.b[:to])
		n += m
		if err != nil {
			return
		}
	}
}

// Byte reads and returns a byte.
func (d *Decoder) Byte() byte {
	if _, err := d.readFull(d.b[:1]); err != nil {
		return 0
	}
	return d.b[0]
}

// Bytes reads into a byte slice.
func (d *Decoder) Bytes(p []byte) {
	d.readFull(p)
}

// Bool reads and returns a boolean.
func (d *Decoder) Bool() bool {
	return d.Byte() != 0
}

// Int8 reads and returns an int8.
func (d *Decoder) Int8() int8 {
	return int8(d.Byte())
}

// Int16 reads and returns an int16.
func (d *Decoder) Int16() int16 {
	if _, err := d.readFull(d.b[:2]); err != nil {
		return 0
	}
	return int16(binary.LittleEndian.Uint16(d.b[:2]))
}

// Uint16 reads and returns an uint16.
func (d *Decoder) Uint16() uint16 {
	if _, err := d.readFull(d.b[:2]); err != nil {
		return 0
	}
	return binary.LittleEndian.Uint16(d.b[:2])
}

// Int32 reads and returns an int32.
func (d *Decoder) Int32() int32 {
	if _, err := d.readFull(d.b[:4]); err != nil {
		return 0
	}
	return int32(binary.LittleEndian.Uint32(d.b[:4]))
}

// Uint32 reads and returns an uint32.
func (d *Decoder) Uint32() uint32 {
	if _, err := d.readFull(d.b[:4]); err != nil {
		return 0
	}
	return binary.LittleEndian.Uint32(d.b[:4])
}

// Uint32ByteOrder reads and returns an uint32 in given byte order.
func (d *Decoder) Uint32ByteOrder(byteOrder binary.ByteOrder) uint32 {
	if _, err := d.readFull(d.b[:4]); err != nil {
		return 0
	}
	return byteOrder.Uint32(d.b[:4])
}

// Int64 reads and returns an int64.
func (d *Decoder) Int64() int64 {
	if _, err := d.readFull(d.b[:8]); err != nil {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(d.b[:8]))
}

// Uint64 reads and returns an uint64.
func (d *Decoder) Uint64() uint64 {
	if _, err := d.readFull(d.b[:8]); err != nil {
		return 0
	}
	return binary.LittleEndian.Uint64(d.b[:8])
}

// Float32 reads and returns a float32.
func (d *Decoder) Float32() float32 {
	if _, err := d.readFull(d.b[:4]); err != nil {
		return 0
	}
	bits := binary.LittleEndian.Uint32(d.b[:4])
	return math.Float32frombits(bits)
}

// Float64 reads and returns a float64.
func (d *Decoder) Float64() float64 {
	if _, err := d.readFull(d.b[:8]); err != nil {
		return 0
	}
	bits := binary.LittleEndian.Uint64(d.b[:8])
	return math.Float64frombits(bits)
}

// Decimal reads and returns a decimal.
// - error is only returned in case of conversion errors.
func (d *Decoder) Decimal() (*big.Int, int, error) { // m, exp
	bs := d.b[:decSize]

	if _, err := d.readFull(bs); err != nil {
		return nil, 0, nil
	}

	if (bs[15] & 0x70) == 0x70 { //null value (bit 4,5,6 set)
		return nil, 0, nil
	}

	if (bs[15] & 0x60) == 0x60 {
		return nil, 0, fmt.Errorf("decimal: format (infinity, nan, ...) not supported : %v", bs)
	}

	neg := (bs[15] & 0x80) != 0
	exp := int((((uint16(bs[15])<<8)|uint16(bs[14]))<<1)>>2) - dec128Bias

	// b14 := b[14]  // save b[14]
	bs[14] &= 0x01 // keep the mantissa bit (rest: sign and exp)

	//most significand byte
	msb := 14
	for msb > 0 && bs[msb] == 0 {
		msb--
	}

	//calc number of words
	numWords := (msb / _S) + 1
	ws := make([]big.Word, numWords)

	bs = bs[:msb+1]
	for i, b := range bs {
		ws[i/_S] |= (big.Word(b) << (i % _S * 8))
	}

	m := new(big.Int).SetBits(ws)
	if neg {
		m = m.Neg(m)
	}
	return m, exp, nil
}

// Fixed reads and returns a fixed decimal.
func (d *Decoder) Fixed(size int) *big.Int { // m, exp
	bs := d.b[:size]

	if _, err := d.readFull(bs); err != nil {
		return nil
	}

	neg := (bs[size-1] & 0x80) != 0 // is negative number (2s complement)

	//most significand byte
	msb := size - 1
	for msb > 0 && bs[msb] == 0 {
		msb--
	}

	//calc number of words
	numWords := (msb / _S) + 1
	ws := make([]big.Word, numWords)

	bs = bs[:msb+1]
	for i, b := range bs {
		// if negative: invert byte (2s complement)
		if neg {
			b = ^b
		}
		ws[i/_S] |= (big.Word(b) << (i % _S * 8))
	}

	m := new(big.Int).SetBits(ws)

	if neg {
		m.Add(m, natOne) // 2s complement - add 1
		m.Neg(m)         // set sign
	}
	return m
}

// CESU8Bytes reads a size CESU-8 encoded byte sequence and returns an UTF-8 byte slice.
// - error is only returned in case of conversion errors.
func (d *Decoder) CESU8Bytes(size int) ([]byte, error) {
	if d.err != nil {
		return nil, nil
	}

	var p []byte
	if size > readScratchSize {
		p = make([]byte, size) //TODO: optimize via piece wise reading
	} else {
		p = d.b[:size]
	}

	if _, err := d.readFull(p); err != nil {
		return nil, nil
	}

	r, _, err := transform.Bytes(d.tr, p)
	return r, err
}
