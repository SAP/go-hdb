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

package protocol

import (
	"fmt"
	"reflect"

	"github.com/SAP/go-hdb/internal/protocol/encoding"
)

type partEncoder interface {
	size() int
	encode(*encoding.Encoder) error
}

type partDecoder interface {
	decode(*encoding.Decoder, *partHeader) error
}

type partDecodeEncoder interface {
	partDecoder
	partEncoder
}

// TODO: uncomment after go1.13 compatibility is dropped.
// type partReadWriter interface {
// 	partReader
// 	partWriter
// }

type part interface {
	String() string // should support Stringer interface
	kind() partKind
}

// part kind methods
func (*hdbErrors) kind() partKind           { return pkError }
func (*authInitReq) kind() partKind         { return pkAuthentication }
func (*authInitRep) kind() partKind         { return pkAuthentication }
func (*authFinalReq) kind() partKind        { return pkAuthentication }
func (*authFinalRep) kind() partKind        { return pkAuthentication }
func (clientID) kind() partKind             { return pkClientID }
func (connectOptions) kind() partKind       { return pkConnectOptions }
func (*topologyInformation) kind() partKind { return pkTopologyInformation }
func (command) kind() partKind              { return pkCommand }
func (*rowsAffected) kind() partKind        { return pkRowsAffected }
func (transactionFlags) kind() partKind     { return pkTransactionFlags }
func (statementContext) kind() partKind     { return pkStatementContext }
func (statementID) kind() partKind          { return pkStatementID }
func (*parameterMetadata) kind() partKind   { return pkParameterMetadata }
func (*inputParameters) kind() partKind     { return pkParameters }
func (*outputParameters) kind() partKind    { return pkOutputParameters }
func (*resultMetadata) kind() partKind      { return pkResultMetadata }
func (resultsetID) kind() partKind          { return pkResultsetID }
func (*resultset) kind() partKind           { return pkResultset }
func (fetchsize) kind() partKind            { return pkFetchSize }
func (*readLobRequest) kind() partKind      { return pkReadLobRequest }
func (*readLobReply) kind() partKind        { return pkReadLobReply }
func (*writeLobRequest) kind() partKind     { return pkWriteLobRequest }
func (*writeLobReply) kind() partKind       { return pkWriteLobReply }

// func (lobFlags) kind() partKind                   { return pkLobFlags }

// check if part types implement part interface
var (
	_ part = (*hdbErrors)(nil)
	_ part = (*authInitReq)(nil)
	_ part = (*authInitRep)(nil)
	_ part = (*authFinalReq)(nil)
	_ part = (*authFinalRep)(nil)
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
	_ part = (*readLobRequest)(nil)
	_ part = (*readLobReply)(nil)
	_ part = (*writeLobRequest)(nil)
	_ part = (*writeLobReply)(nil)

//	_ part = (*lobFlags)(nil)
)

type partWriter interface {
	part
	numArg() int
	partEncoder
}

// numArg methods (result == 1)
func (*authInitReq) numArg() int    { return 1 }
func (*authInitRep) numArg() int    { return 1 }
func (*authFinalReq) numArg() int   { return 1 }
func (*authFinalRep) numArg() int   { return 1 }
func (clientID) numArg() int        { return 1 }
func (command) numArg() int         { return 1 }
func (statementID) numArg() int     { return 1 }
func (resultsetID) numArg() int     { return 1 }
func (fetchsize) numArg() int       { return 1 }
func (*readLobRequest) numArg() int { return 1 }

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
	_ partWriter = (*authInitReq)(nil)
	_ partWriter = (*authFinalReq)(nil)
	_ partWriter = (*clientID)(nil)
	_ partWriter = (*connectOptions)(nil)
	_ partWriter = (*command)(nil)
	_ partWriter = (*statementID)(nil)
	_ partWriter = (*inputParameters)(nil)
	_ partWriter = (*resultsetID)(nil)
	_ partWriter = (*fetchsize)(nil)
	_ partReader = (*readLobRequest)(nil)
	_ partReader = (*writeLobRequest)(nil)

//	_ partWriter = (*lobFlags)(nil)
)

type partReader interface {
	part
	partDecoder
}

// check if part types implement partReader interface
var (
	_ partReader = (*hdbErrors)(nil)
	_ partReader = (*authInitReq)(nil)
	_ partReader = (*authInitRep)(nil)
	_ partReader = (*authFinalReq)(nil)
	_ partReader = (*authFinalRep)(nil)
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
func (*resultset) prm()        {}

var (
	_ prmPartReader = (*inputParameters)(nil)
	_ prmPartReader = (*outputParameters)(nil)
	_ prmPartReader = (*resultset)(nil)
)

var partTypeMap = map[partKind]reflect.Type{
	pkError:               reflect.TypeOf((*hdbErrors)(nil)).Elem(),
	pkClientID:            reflect.TypeOf((*clientID)(nil)).Elem(),
	pkConnectOptions:      reflect.TypeOf((*connectOptions)(nil)).Elem(),
	pkTopologyInformation: reflect.TypeOf((*topologyInformation)(nil)).Elem(),
	pkCommand:             reflect.TypeOf((*command)(nil)).Elem(),
	pkRowsAffected:        reflect.TypeOf((*rowsAffected)(nil)).Elem(),
	pkTransactionFlags:    reflect.TypeOf((*transactionFlags)(nil)).Elem(),
	pkStatementContext:    reflect.TypeOf((*statementContext)(nil)).Elem(),
	pkStatementID:         reflect.TypeOf((*statementID)(nil)).Elem(),
	pkParameterMetadata:   reflect.TypeOf((*parameterMetadata)(nil)).Elem(),
	pkParameters:          reflect.TypeOf((*inputParameters)(nil)).Elem(),
	pkOutputParameters:    reflect.TypeOf((*outputParameters)(nil)).Elem(),
	pkResultMetadata:      reflect.TypeOf((*resultMetadata)(nil)).Elem(),
	pkResultsetID:         reflect.TypeOf((*resultsetID)(nil)).Elem(),
	pkResultset:           reflect.TypeOf((*resultset)(nil)).Elem(),
	pkFetchSize:           reflect.TypeOf((*fetchsize)(nil)).Elem(),
	pkReadLobRequest:      reflect.TypeOf((*readLobRequest)(nil)).Elem(),
	pkReadLobReply:        reflect.TypeOf((*readLobReply)(nil)).Elem(),
	pkWriteLobReply:       reflect.TypeOf((*writeLobReply)(nil)).Elem(),
	pkWriteLobRequest:     reflect.TypeOf((*writeLobRequest)(nil)).Elem(),
}

var partReaderType = reflect.TypeOf((*partReader)(nil)).Elem()
var prmPartReaderType = reflect.TypeOf((*prmPartReader)(nil)).Elem()

func newPartReader(pk partKind) (partReader, error) {
	pt, ok := partTypeMap[pk]
	if !ok {
		return nil, fmt.Errorf("part type map - part kind %s not found", pk)
	}
	part := reflect.New(pt).Interface()
	if _, ok := part.(prmPartReader); ok {
		return nil, fmt.Errorf("part kind %s does implement parameter part reader interface", pk)
	}
	partReader, ok := part.(partReader)
	if !ok {
		return nil, fmt.Errorf("part kind %s does not implement part reader interface", pk)
	}
	return partReader, nil
}
