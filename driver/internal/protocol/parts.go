package protocol

import (
	"fmt"
	"reflect"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	hdbreflect "github.com/SAP/go-hdb/driver/internal/reflect"
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

// writablePart represents a protocol part the driver is able to write.
type writablePart interface {
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
func (*TopologyInformation) kind() PartKind { return PkTopologyInformation }
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
func (*ClientContext) kind() PartKind       { return PkClientContext }
func (*ConnectOptions) kind() PartKind      { return PkConnectOptions }
func (*DBConnectInfo) kind() PartKind       { return PkDBConnectInfo }
func (*statementContext) kind() PartKind    { return PkStatementContext }
func (*transactionFlags) kind() PartKind    { return PkTransactionFlags }

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
	_ writablePart = (*AuthInitRequest)(nil)
	_ writablePart = (*AuthFinalRequest)(nil)
	_ writablePart = (*ClientID)(nil)
	_ writablePart = (*clientInfo)(nil)
	_ writablePart = (*Command)(nil)
	_ writablePart = (*StatementID)(nil)
	_ writablePart = (*InputParameters)(nil)
	_ writablePart = (*ResultsetID)(nil)
	_ writablePart = (*Fetchsize)(nil)
	_ writablePart = (*ReadLobRequest)(nil)
	_ writablePart = (*WriteLobRequest)(nil)
	_ writablePart = (*ClientContext)(nil)
	_ writablePart = (*ConnectOptions)(nil)
	_ writablePart = (*DBConnectInfo)(nil)
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
	_ numArgPart = (*TopologyInformation)(nil)
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
	_ numArgPart = (*ClientContext)(nil)
	_ numArgPart = (*ConnectOptions)(nil)
	_ numArgPart = (*DBConnectInfo)(nil)
	_ numArgPart = (*statementContext)(nil)
	_ numArgPart = (*transactionFlags)(nil)
)

var genPartTypeMap = map[PartKind]reflect.Type{
	PkError:               hdbreflect.TypeFor[HdbErrors](),
	PkClientID:            hdbreflect.TypeFor[ClientID](),
	PkClientInfo:          hdbreflect.TypeFor[clientInfo](),
	PkTopologyInformation: hdbreflect.TypeFor[TopologyInformation](),
	PkCommand:             hdbreflect.TypeFor[Command](),
	PkRowsAffected:        hdbreflect.TypeFor[RowsAffected](),
	PkStatementID:         hdbreflect.TypeFor[StatementID](),
	PkResultsetID:         hdbreflect.TypeFor[ResultsetID](),
	PkFetchSize:           hdbreflect.TypeFor[Fetchsize](),
	PkReadLobRequest:      hdbreflect.TypeFor[ReadLobRequest](),
	PkReadLobReply:        hdbreflect.TypeFor[ReadLobReply](),
	PkWriteLobReply:       hdbreflect.TypeFor[WriteLobReply](),
	PkWriteLobRequest:     hdbreflect.TypeFor[WriteLobRequest](),
	PkClientContext:       hdbreflect.TypeFor[ClientContext](),
	PkConnectOptions:      hdbreflect.TypeFor[ConnectOptions](),
	PkTransactionFlags:    hdbreflect.TypeFor[transactionFlags](),
	PkStatementContext:    hdbreflect.TypeFor[statementContext](),
	PkDBConnectInfo:       hdbreflect.TypeFor[DBConnectInfo](),
	/*
	   parts that cannot be used generically as additional parameters are needed

	   PkParameterMetadata
	   PkParameters
	   PkOutputParameters
	   PkResultMetadata
	   PkResultset
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
