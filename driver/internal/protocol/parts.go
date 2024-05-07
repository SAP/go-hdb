package protocol

import (
	"fmt"
	"reflect"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	hdbreflect "github.com/SAP/go-hdb/driver/internal/reflect"
)

// decodePrms is an (extentable) structure of optional decoding parameters.
type decodePrms struct {
	readFn lobReadFn
	numArg int
	bufLen int
}

// part represents a protocol part.
type part interface {
	String() string // should support Stringer interface
	kind() PartKind
}

// DecodePart represents a protocol part the driver is able to decode.
type DecodePart interface {
	part
	decode(dec *encoding.Decoder, prms *decodePrms) error
}

// encodePart represents a protocol part the driver is able to encode.
type encodePart interface {
	part
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

// check if part types implement encodePart interface.
var (
	_ encodePart = (*AuthInitRequest)(nil)
	_ encodePart = (*AuthFinalRequest)(nil)
	_ encodePart = (*ClientID)(nil)
	_ encodePart = (*clientInfo)(nil)
	_ encodePart = (*Command)(nil)
	_ encodePart = (*StatementID)(nil)
	_ encodePart = (*InputParameters)(nil)
	_ encodePart = (*ResultsetID)(nil)
	_ encodePart = (*Fetchsize)(nil)
	_ encodePart = (*ReadLobRequest)(nil)
	_ encodePart = (*WriteLobRequest)(nil)
	_ encodePart = (*ClientContext)(nil)
	_ encodePart = (*ConnectOptions)(nil)
	_ encodePart = (*DBConnectInfo)(nil)
)

// check if part types implement the decode part interface.
var (
	_ DecodePart = (*HdbErrors)(nil)
	_ DecodePart = (*AuthInitRequest)(nil)
	_ DecodePart = (*AuthInitReply)(nil)
	_ DecodePart = (*AuthFinalRequest)(nil)
	_ DecodePart = (*AuthFinalReply)(nil)
	_ DecodePart = (*ClientID)(nil)
	_ DecodePart = (*clientInfo)(nil)
	_ DecodePart = (*TopologyInformation)(nil)
	_ DecodePart = (*Command)(nil)
	_ DecodePart = (*RowsAffected)(nil)
	_ DecodePart = (*StatementID)(nil)
	_ DecodePart = (*ParameterMetadata)(nil)
	_ DecodePart = (*InputParameters)(nil)
	_ DecodePart = (*OutputParameters)(nil)
	_ DecodePart = (*ResultMetadata)(nil)
	_ DecodePart = (*ResultsetID)(nil)
	_ DecodePart = (*Resultset)(nil)
	_ DecodePart = (*Fetchsize)(nil)
	_ DecodePart = (*ReadLobRequest)(nil)
	_ DecodePart = (*WriteLobRequest)(nil)
	_ DecodePart = (*ReadLobReply)(nil)
	_ DecodePart = (*WriteLobReply)(nil)
	_ DecodePart = (*ClientContext)(nil)
	_ DecodePart = (*ConnectOptions)(nil)
	_ DecodePart = (*DBConnectInfo)(nil)
	_ DecodePart = (*statementContext)(nil)
	_ DecodePart = (*transactionFlags)(nil)
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
func newGenPartReader(kind PartKind) DecodePart {
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
	part, ok := reflect.New(pt).Interface().(DecodePart)
	if !ok {
		panic(fmt.Sprintf("part kind %s does not implement part reader interface", kind)) // should never happen
	}
	return part
}
