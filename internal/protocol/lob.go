/*
Copyright 2014 SAP SE

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

package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
)

const (
	locatorIDSize       = 8
	writeLobRequestSize = 21
)

// variable (unit testing)
//var lobChunkSize = 1 << 14 //TODO: check size
//var lobChunkSize int32 = 4096 //TODO: check size

// lob options
type lobOptions int8

const (
	loNullindicator lobOptions = 0x01
	loDataincluded  lobOptions = 0x02
	loLastdata      lobOptions = 0x04
)

var lobOptionsText = map[lobOptions]string{
	loNullindicator: "null indicator",
	loDataincluded:  "data included",
	loLastdata:      "last data",
}

func (k lobOptions) String() string {
	t := make([]string, 0, len(lobOptionsText))

	for option, text := range lobOptionsText {
		if (k & option) != 0 {
			t = append(t, text)
		}
	}
	return fmt.Sprintf("%v", t)
}

// not used
// type lobFlags bool

// func (f lobFlags) String() string { return fmt.Sprintf("%t", f) }
// func (f *lobFlags) decode(dec *encoding.Decoder, ph *partHeader) error {
// 	*f = lobFlags(dec.Bool())
// 	return dec.Error()
// }
// func (f lobFlags) encode(enc *encoding.Encoder) error { enc.Bool(bool(f)); return nil }

// write lob fields to db
// reply: returns locator ids to write content to
type writeLobReply struct {
	ids []locatorID
}

func (r *writeLobReply) String() string { return fmt.Sprintf("ids %v", r.ids) }

func (r *writeLobReply) reset(numArg int) {
	if r.ids == nil || cap(r.ids) < numArg {
		r.ids = make([]locatorID, numArg)
	} else {
		r.ids = r.ids[:numArg]
	}
}

func (r *writeLobReply) decode(dec *encoding.Decoder, ph *partHeader) error {
	numArg := ph.numArg()
	r.reset(numArg)

	for i := 0; i < numArg; i++ {
		r.ids[i] = locatorID(dec.Uint64())
	}
	return dec.Error()
}

//write lob request
type writeLobRequest struct {
	chunkReaders []chunkReader
}

func (r *writeLobRequest) String() string {
	return fmt.Sprintf("readers %v", r.chunkReaders)
}

func (r *writeLobRequest) size() int {
	// TODO: check size limit
	size := 0
	for _, chunkReader := range r.chunkReaders {
		size += (writeLobRequestSize + chunkReader.next())
	}
	return size
}

func (r *writeLobRequest) numArg() int {
	return len(r.chunkReaders)
}

func (r *writeLobRequest) decode(dec *encoding.Decoder, ph *partHeader) error {
	panic("not yet implemented")
}

func (r *writeLobRequest) encode(enc *encoding.Encoder) error {
	for _, chunkReader := range r.chunkReaders {
		enc.Uint64(uint64(chunkReader.locatorID()))
		opt := int8(0x02) // data included
		if chunkReader.eof() {
			opt |= 0x04 // last data
		}
		enc.Int8(opt)
		enc.Int64(-1) //offset (-1 := append)
		b, err := chunkReader.bytes()
		if err != nil {
			return err
		}
		enc.Int32(int32(len(b))) // size
		enc.Bytes(b)
	}
	return nil
}

type readLobRequest struct {
	writer chunkWriter
}

func (r *readLobRequest) String() string {
	readOfs, readLen := r.writer.readOfsLen()
	return fmt.Sprintf("id %d readOfs %d readLen %d", r.writer.id(), readOfs, readLen)
}

func (r *readLobRequest) decode(dec *encoding.Decoder, ph *partHeader) error {
	panic("not yet implemented")
}

func (r *readLobRequest) encode(enc *encoding.Encoder) error {
	enc.Uint64(uint64(r.writer.id()))

	readOfs, readLen := r.writer.readOfsLen()
	enc.Int64(readOfs + 1) //1-based
	enc.Int32(readLen)
	enc.Zeroes(4)

	return nil
}

// read lob reply
// - seems like readLobreply returns only a result for one lob - even if more then one is requested
// --> read single lobs
type readLobReply struct {
	writer chunkWriter
}

func (r *readLobReply) String() string { return fmt.Sprintf("id %d", r.writer.id()) }

func (r *readLobReply) decode(dec *encoding.Decoder, ph *partHeader) error {
	if ph.numArg() != 1 {
		panic("numArg == 1 expected")
	}

	id := dec.Uint64()

	if r.writer.id() != locatorID(id) {
		return fmt.Errorf("internal error: invalid lob locator %d - expected %d", id, r.writer.id())
	}

	opt := dec.Int8()
	chunkLen := dec.Int32()
	dec.Skip(3)
	eof := (lobOptions(opt) & loLastdata) != 0

	if err := r.writer.write(dec, int(chunkLen), eof); err != nil {
		return err
	}
	return dec.Error()
}
