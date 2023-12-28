package protocol

import (
	"fmt"
	"reflect"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

// Part represents a protocol part.
type Part interface {
	String() string // should support Stringer interface
	kind() PartKind
}

type defPart interface {
	Part
	decode(dec *encoding.Decoder) error
}
type numArgPart interface {
	Part
	decodeNumArg(dec *encoding.Decoder, numArg int) error
}
type bufLenPart interface {
	Part
	decodeBufLen(dec *encoding.Decoder, bufLen int) error
}

// WritablePart represents a protocol part the driver is able to write.
type WritablePart interface {
	Part
	numArg() int
	size() int
	encode(enc *encoding.Encoder) error
}

func (*HdbErrors) kind() PartKind           { return PkError }
func (*AuthInitRequest) kind() PartKind     { return PkAuthentication }
func (*AuthInitReply) kind() PartKind       { return PkAuthentication }
func (*AuthFinalRequest) kind() PartKind    { return PkAuthentication }
func (*AuthFinalReply) kind() PartKind      { return PkAuthentication }
func (ClientID) kind() PartKind             { return PkClientID }
func (clientInfo) kind() PartKind           { return PkClientInfo }
func (*topologyInformation) kind() PartKind { return PkTopologyInformation }
func (Command) kind() PartKind              { return PkCommand }
func (*RowsAffected) kind() PartKind        { return PkRowsAffected }
func (StatementID) kind() PartKind          { return PkStatementID }
func (*ParameterMetadata) kind() PartKind   { return PkParameterMetadata }
func (*InputParameters) kind() PartKind     { return PkParameters }
func (*OutputParameters) kind() PartKind    { return PkOutputParameters }
func (*ResultMetadata) kind() PartKind      { return PkResultMetadata }
func (ResultsetID) kind() PartKind          { return PkResultsetID }
func (*Resultset) kind() PartKind           { return PkResultset }
func (Fetchsize) kind() PartKind            { return PkFetchSize }
func (*ReadLobRequest) kind() PartKind      { return PkReadLobRequest }
func (*ReadLobReply) kind() PartKind        { return PkReadLobReply }
func (*WriteLobRequest) kind() PartKind     { return PkWriteLobRequest }
func (*WriteLobReply) kind() PartKind       { return PkWriteLobReply }

// numArg methods (result == 1).
func (*AuthInitRequest) numArg() int  { return 1 }
func (*AuthFinalRequest) numArg() int { return 1 }
func (ClientID) numArg() int          { return 1 }
func (Command) numArg() int           { return 1 }
func (StatementID) numArg() int       { return 1 }
func (ResultsetID) numArg() int       { return 1 }
func (Fetchsize) numArg() int         { return 1 }
func (*ReadLobRequest) numArg() int   { return 1 }

// size methods (fixed size).
const (
	statementIDSize    = 8
	resultsetIDSize    = 8
	fetchsizeSize      = 4
	readLobRequestSize = 24
)

func (StatementID) size() int    { return statementIDSize }
func (ResultsetID) size() int    { return resultsetIDSize }
func (Fetchsize) size() int      { return fetchsizeSize }
func (ReadLobRequest) size() int { return readLobRequestSize }

// func (lobFlags) size() int       { return tinyintFieldSize }

// check if part types implement WritablePart interface.
var (
	_ WritablePart = (*AuthInitRequest)(nil)
	_ WritablePart = (*AuthFinalRequest)(nil)
	_ WritablePart = (*ClientID)(nil)
	_ WritablePart = (*clientInfo)(nil)
	_ WritablePart = (*Command)(nil)
	_ WritablePart = (*StatementID)(nil)
	_ WritablePart = (*InputParameters)(nil)
	_ WritablePart = (*ResultsetID)(nil)
	_ WritablePart = (*Fetchsize)(nil)
	_ WritablePart = (*ReadLobRequest)(nil)
	_ WritablePart = (*WriteLobRequest)(nil)
	_ WritablePart = (*Options[ClientContextOption])(nil) // sufficient to check one option.
)

// check if part types implement the right part interface.
var (
	_ numArgPart = (*HdbErrors)(nil)
	_ defPart    = (*AuthInitRequest)(nil)
	_ defPart    = (*AuthInitReply)(nil)
	_ defPart    = (*AuthFinalRequest)(nil)
	_ defPart    = (*AuthFinalReply)(nil)
	_ bufLenPart = (*ClientID)(nil)
	_ numArgPart = (*clientInfo)(nil)
	_ numArgPart = (*topologyInformation)(nil)
	_ bufLenPart = (*Command)(nil)
	_ numArgPart = (*RowsAffected)(nil)
	_ defPart    = (*StatementID)(nil)
	_ numArgPart = (*ParameterMetadata)(nil)
	_ numArgPart = (*InputParameters)(nil)
	_ numArgPart = (*OutputParameters)(nil)
	_ numArgPart = (*ResultMetadata)(nil)
	_ defPart    = (*ResultsetID)(nil)
	_ numArgPart = (*Resultset)(nil)
	_ defPart    = (*Fetchsize)(nil)
	_ defPart    = (*ReadLobRequest)(nil)
	_ numArgPart = (*WriteLobRequest)(nil)
	_ numArgPart = (*ReadLobReply)(nil)
	_ numArgPart = (*WriteLobReply)(nil)
	_ numArgPart = (*Options[ClientContextOption])(nil) // sufficient to check one option.
)

var genPartTypeMap = map[PartKind]reflect.Type{
	PkError:               reflect.TypeOf((*HdbErrors)(nil)).Elem(),
	PkClientID:            reflect.TypeOf((*ClientID)(nil)).Elem(),
	PkClientInfo:          reflect.TypeOf((*clientInfo)(nil)).Elem(),
	PkTopologyInformation: reflect.TypeOf((*topologyInformation)(nil)).Elem(),
	PkCommand:             reflect.TypeOf((*Command)(nil)).Elem(),
	PkRowsAffected:        reflect.TypeOf((*RowsAffected)(nil)).Elem(),
	PkStatementID:         reflect.TypeOf((*StatementID)(nil)).Elem(),
	PkResultsetID:         reflect.TypeOf((*ResultsetID)(nil)).Elem(),
	PkFetchSize:           reflect.TypeOf((*Fetchsize)(nil)).Elem(),
	PkReadLobRequest:      reflect.TypeOf((*ReadLobRequest)(nil)).Elem(),
	PkReadLobReply:        reflect.TypeOf((*ReadLobReply)(nil)).Elem(),
	PkWriteLobReply:       reflect.TypeOf((*WriteLobReply)(nil)).Elem(),
	PkWriteLobRequest:     reflect.TypeOf((*WriteLobRequest)(nil)).Elem(),
	PkClientContext:       reflect.TypeOf((*Options[ClientContextOption])(nil)).Elem(),
	PkConnectOptions:      reflect.TypeOf((*Options[ConnectOption])(nil)).Elem(),
	PkTransactionFlags:    reflect.TypeOf((*Options[transactionFlagType])(nil)).Elem(),
	PkStatementContext:    reflect.TypeOf((*Options[statementContextType])(nil)).Elem(),
	PkDBConnectInfo:       reflect.TypeOf((*Options[DBConnectInfoType])(nil)).Elem(),
	/*
	   parts that cannot be used generically as additional parameters are needed

	   PkParameterMetadata:   reflect.TypeOf((*ParameterMetadata)(nil)).Elem(),
	   PkParameters:          reflect.TypeOf((*InputParameters)(nil)).Elem(),
	   PkOutputParameters:    reflect.TypeOf((*OutputParameters)(nil)).Elem(),
	   PkResultMetadata:      reflect.TypeOf((*ResultMetadata)(nil)).Elem(),
	   PkResultset:           reflect.TypeOf((*Resultset)(nil)).Elem(),
	*/
}

// newGenPartReader returns a generic part reader.
func newGenPartReader(kind PartKind) Part {
	if kind == PkAuthentication {
		return nil // cannot instantiate generically
	}
	pt, ok := genPartTypeMap[kind]
	if !ok {
		// whether part cannot be instantiated generically or
		// part is not (yet) known to the driver
		return nil
	}
	// create instance
	part, ok := reflect.New(pt).Interface().(Part)
	if !ok {
		panic(fmt.Sprintf("part kind %s does not implement part reader interface", kind)) // should never happen
	}
	return part
}
