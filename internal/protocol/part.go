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
	"math"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
)

const (
	partHeaderSize = 16
	maxPartNum     = math.MaxInt16
)

type part interface {
	kind() partKind
}

// part kind methods
func (*hdbErrors) kind() partKind                 { return pkError }
func (*scramsha256InitialRequest) kind() partKind { return pkAuthentication }
func (*scramsha256InitialReply) kind() partKind   { return pkAuthentication }
func (*scramsha256FinalRequest) kind() partKind   { return pkAuthentication }
func (*scramsha256FinalReply) kind() partKind     { return pkAuthentication }
func (clientID) kind() partKind                   { return pkClientID }
func (connectOptions) kind() partKind             { return pkConnectOptions }
func (*topologyInformation) kind() partKind       { return pkTopologyInformation }
func (command) kind() partKind                    { return pkCommand }
func (*rowsAffected) kind() partKind              { return pkRowsAffected }
func (transactionFlags) kind() partKind           { return pkTransactionFlags }
func (statementContext) kind() partKind           { return pkStatementContext }
func (statementID) kind() partKind                { return pkStatementID }
func (*parameterMetadata) kind() partKind         { return pkParameterMetadata }
func (*inputParameters) kind() partKind           { return pkParameters }
func (*outputParameters) kind() partKind          { return pkOutputParameters }
func (*resultMetadata) kind() partKind            { return pkResultMetadata }
func (resultsetID) kind() partKind                { return pkResultsetID }
func (*resultset) kind() partKind                 { return pkResultset }
func (fetchsize) kind() partKind                  { return pkFetchSize }
func (*readLobRequest) kind() partKind            { return pkReadLobRequest }
func (*readLobReply) kind() partKind              { return pkReadLobReply }
func (*writeLobReply) kind() partKind             { return pkWriteLobReply }
func (*writeLobRequest) kind() partKind           { return pkWriteLobRequest }

// func (lobFlags) kind() partKind                   { return pkLobFlags }

// check if part types implement part interface
var (
	_ part = (*hdbErrors)(nil)
	_ part = (*scramsha256InitialRequest)(nil)
	_ part = (*scramsha256InitialReply)(nil)
	_ part = (*scramsha256FinalRequest)(nil)
	_ part = (*scramsha256FinalReply)(nil)
	_ part = (*clientID)(nil)
	_ part = (*connectOptions)(nil)
	_ part = (*topologyInformation)(nil)
	_ part = (*command)(nil)
	_ part = (*rowsAffected)(nil)
	_ part = (*transactionFlags)(nil)
	_ part = (*statementContext)(nil)
	_ part = (*statementID)(nil)
	_ part = (*parameterMetadata)(nil)
	_ part = (*inputParameters)(nil)
	_ part = (*outputParameters)(nil)
	_ part = (*resultMetadata)(nil)
	_ part = (*resultsetID)(nil)
	_ part = (*resultset)(nil)
	_ part = (*fetchsize)(nil)
	_ part = (*readLobReply)(nil)
	_ part = (*writeLobReply)(nil)

//	_ part = (*lobFlags)(nil)
)

type partWriter interface {
	part
	size() int
	numArg() int
	encode(*encoding.Encoder) error
}

// numArg methods (result == 1)
func (*scramsha256InitialRequest) numArg() int { return 1 }
func (*scramsha256FinalRequest) numArg() int   { return 1 }
func (clientID) numArg() int                   { return 1 }
func (command) numArg() int                    { return 1 }
func (statementID) numArg() int                { return 1 }
func (resultsetID) numArg() int                { return 1 }
func (fetchsize) numArg() int                  { return 1 }
func (*readLobRequest) numArg() int            { return 1 }

// func (lobFlags) numArg() int                   { return 1 }

// size methods (fixed size)
const (
	statementIDSize    = 8
	resultsetIDSize    = 8
	fetchsizeSize      = 4
	readLobRequestSize = 24
)

func (statementID) size() int    { return statementIDSize }
func (resultsetID) size() int    { return resultsetIDSize }
func (fetchsize) size() int      { return fetchsizeSize }
func (readLobRequest) size() int { return readLobRequestSize }

// func (lobFlags) size() int       { return tinyintFieldSize }

// check if part types implement partWriter interface
var (
	_ partWriter = (*scramsha256InitialRequest)(nil)
	_ partWriter = (*scramsha256FinalRequest)(nil)
	_ partWriter = (*clientID)(nil)
	_ partWriter = (*connectOptions)(nil)
	_ partWriter = (*command)(nil)
	_ partWriter = (*statementID)(nil)
	_ partWriter = (*inputParameters)(nil)
	_ partWriter = (*resultsetID)(nil)
	_ partWriter = (*fetchsize)(nil)

//	_ partWriter = (*lobFlags)(nil)
)

type partReader interface {
	part
	decode(*encoding.Decoder, *partHeader) error
}

// check if part types implement partReader interface
var (
	_ partReader = (*hdbErrors)(nil)
	_ partReader = (*scramsha256InitialRequest)(nil)
	_ partReader = (*scramsha256InitialReply)(nil)
	_ partReader = (*scramsha256FinalRequest)(nil)
	_ partReader = (*scramsha256FinalReply)(nil)
	_ partReader = (*clientID)(nil)
	_ partReader = (*connectOptions)(nil)
	_ partReader = (*topologyInformation)(nil)
	_ partReader = (*command)(nil)
	_ partReader = (*rowsAffected)(nil)
	_ partReader = (*transactionFlags)(nil)
	_ partReader = (*statementContext)(nil)
	_ partReader = (*statementID)(nil)
	_ partReader = (*parameterMetadata)(nil)
	_ partReader = (*inputParameters)(nil)
	_ partReader = (*outputParameters)(nil)
	_ partReader = (*resultMetadata)(nil)
	_ partReader = (*resultsetID)(nil)
	_ partReader = (*resultset)(nil)
	_ partReader = (*fetchsize)(nil)
	_ partReader = (*readLobRequest)(nil)
	_ partReader = (*writeLobRequest)(nil)
	_ partReader = (*readLobReply)(nil)
	_ partReader = (*writeLobReply)(nil)
)

// some partReader needs additional parameter set before reading
type prmPartReader interface {
	partReader
	prm() // marker interface
}

// prm marker methods
func (*inputParameters) prm()  {}
func (*outputParameters) prm() {}
func (*readLobReply) prm()     {}

var (
	_ prmPartReader = (*inputParameters)(nil)
	_ prmPartReader = (*outputParameters)(nil)
	// _ prmPartReader = (*readLobRequest)(nil)  // TODO implement partReader (sniffer)
	// _ prmPartReader = (*writeLobRequest)(nil) // TODO implement partReader (sniffer)
	_ prmPartReader = (*readLobReply)(nil)
)

//

type partAttributes int8

const (
	paLastPacket      partAttributes = 0x01
	paNextPacket      partAttributes = 0x02
	paFirstPacket     partAttributes = 0x04
	paRowNotFound     partAttributes = 0x08
	paResultsetClosed partAttributes = 0x10
)

var partAttributesText = map[partAttributes]string{
	paLastPacket:      "lastPacket",
	paNextPacket:      "nextPacket",
	paFirstPacket:     "firstPacket",
	paRowNotFound:     "rowNotFound",
	paResultsetClosed: "resultsetClosed",
}

func (k partAttributes) String() string {
	t := make([]string, 0, len(partAttributesText))

	for attr, text := range partAttributesText {
		if (k & attr) != 0 {
			t = append(t, text)
		}
	}
	return fmt.Sprintf("%v", t)
}

func (k partAttributes) ResultsetClosed() bool {
	return (k & paResultsetClosed) == paResultsetClosed
}

func (k partAttributes) LastPacket() bool {
	return (k & paLastPacket) == paLastPacket
}

func (k partAttributes) NoRows() bool {
	attrs := paLastPacket | paRowNotFound
	return (k & attrs) == attrs
}

// part header
type partHeader struct {
	partKind         partKind
	partAttributes   partAttributes
	argumentCount    int16
	bigArgumentCount int32
	bufferLength     int32
	bufferSize       int32
}

func (h *partHeader) String() string {
	return fmt.Sprintf("kind %s partAttributes %s argumentCount %d bigArgumentCount %d bufferLength %d bufferSize %d",
		h.partKind,
		h.partAttributes,
		h.argumentCount,
		h.bigArgumentCount,
		h.bufferLength,
		h.bufferSize,
	)
}

func (h *partHeader) setNumArg(numArg int) error {
	switch {
	default:
		return fmt.Errorf("maximum number of arguments %d exceeded", numArg)
	case numArg <= maxPartNum:
		h.argumentCount = int16(numArg)
		h.bigArgumentCount = 0

		// TODO: seems not to work: see bulk insert test
		// case numArg <= math.MaxInt32:
		// 	s.ph.argumentCount = 0
		// 	s.ph.bigArgumentCount = int32(numArg)
		//
	}
	return nil
}

func (h *partHeader) numArg() int {
	if h.bigArgumentCount != 0 {
		panic("part header: bigArgumentCount is set")
	}
	return int(h.argumentCount)
}

func (h *partHeader) encode(enc *encoding.Encoder) error {
	enc.Int8(int8(h.partKind))
	enc.Int8(int8(h.partAttributes))
	enc.Int16(h.argumentCount)
	enc.Int32(h.bigArgumentCount)
	enc.Int32(h.bufferLength)
	enc.Int32(h.bufferSize)
	//no filler
	return nil
}

func (h *partHeader) decode(dec *encoding.Decoder) error {
	h.partKind = partKind(dec.Int8())
	h.partAttributes = partAttributes(dec.Int8())
	h.argumentCount = dec.Int16()
	h.bigArgumentCount = dec.Int32()
	h.bufferLength = dec.Int32()
	h.bufferSize = dec.Int32()
	// no filler
	return dec.Error()
}
