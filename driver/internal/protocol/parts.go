package protocol

import (
	"reflect"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
	"golang.org/x/text/transform"
)

// Part represents a protocol part.
type Part interface {
	String() string // should support Stringer interface
	kind() PartKind
}

// PartDecoder represents a protocol part decoder.
type PartDecoder interface {
	Part
	decode(dec *encoding.Decoder, header *PartHeader, attrs *ReaderAttrs) error
}

// ResultPartDecoder represents a protocol result part decoder.
type ResultPartDecoder interface {
	Part
	decodeResult(dec *encoding.Decoder, header *PartHeader, attrs *ReaderAttrs, lobReader LobReader) error
}

// PartEncoder represents a protocol part the driver is able to encode.
type PartEncoder interface {
	Part
	numArg() int
	encode(enc *encoding.Encoder, tr transform.Transformer) error
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

// check if part types implement the part encoder interface.
var (
	_ PartEncoder = (*AuthInitRequest)(nil)
	_ PartEncoder = (*AuthFinalRequest)(nil)
	_ PartEncoder = (*ClientID)(nil)
	_ PartEncoder = (*clientInfo)(nil)
	_ PartEncoder = (*Command)(nil)
	_ PartEncoder = (*StatementID)(nil)
	_ PartEncoder = (*InputParameters)(nil)
	_ PartEncoder = (*ResultsetID)(nil)
	_ PartEncoder = (*Fetchsize)(nil)
	_ PartEncoder = (*ReadLobRequest)(nil)
	_ PartEncoder = (*WriteLobRequest)(nil)
	_ PartEncoder = (*ClientContext)(nil)
	_ PartEncoder = (*ConnectOptions)(nil)
	_ PartEncoder = (*DBConnectInfo)(nil)
)

// check if part types implement the right part decoder interface.
var (
	_ PartDecoder       = (*HdbErrors)(nil)
	_ PartDecoder       = (*AuthInitRequest)(nil)
	_ PartDecoder       = (*AuthInitReply)(nil)
	_ PartDecoder       = (*AuthFinalRequest)(nil)
	_ PartDecoder       = (*AuthFinalReply)(nil)
	_ PartDecoder       = (*ClientID)(nil)
	_ PartDecoder       = (*clientInfo)(nil)
	_ PartDecoder       = (*TopologyInformation)(nil)
	_ PartDecoder       = (*Command)(nil)
	_ PartDecoder       = (*RowsAffected)(nil)
	_ PartDecoder       = (*StatementID)(nil)
	_ PartDecoder       = (*ParameterMetadata)(nil)
	_ PartDecoder       = (*InputParameters)(nil)
	_ ResultPartDecoder = (*OutputParameters)(nil)
	_ PartDecoder       = (*ResultMetadata)(nil)
	_ PartDecoder       = (*ResultsetID)(nil)
	_ ResultPartDecoder = (*Resultset)(nil)
	_ PartDecoder       = (*Fetchsize)(nil)
	_ PartDecoder       = (*ReadLobRequest)(nil)
	_ PartDecoder       = (*WriteLobRequest)(nil)
	_ PartDecoder       = (*ReadLobReply)(nil)
	_ PartDecoder       = (*WriteLobReply)(nil)
	_ PartDecoder       = (*ClientContext)(nil)
	_ PartDecoder       = (*ConnectOptions)(nil)
	_ PartDecoder       = (*DBConnectInfo)(nil)
	_ PartDecoder       = (*statementContext)(nil)
	_ PartDecoder       = (*transactionFlags)(nil)
)

var genPartTypeMap = map[PartKind]reflect.Type{
	PkError:               reflect.TypeFor[HdbErrors](),
	PkClientID:            reflect.TypeFor[ClientID](),
	PkClientInfo:          reflect.TypeFor[clientInfo](),
	PkTopologyInformation: reflect.TypeFor[TopologyInformation](),
	PkCommand:             reflect.TypeFor[Command](),
	PkRowsAffected:        reflect.TypeFor[RowsAffected](),
	PkStatementID:         reflect.TypeFor[StatementID](),
	PkResultsetID:         reflect.TypeFor[ResultsetID](),
	PkFetchSize:           reflect.TypeFor[Fetchsize](),
	PkReadLobRequest:      reflect.TypeFor[ReadLobRequest](),
	PkReadLobReply:        reflect.TypeFor[ReadLobReply](),
	PkWriteLobReply:       reflect.TypeFor[WriteLobReply](),
	PkWriteLobRequest:     reflect.TypeFor[WriteLobRequest](),
	PkClientContext:       reflect.TypeFor[ClientContext](),
	PkConnectOptions:      reflect.TypeFor[ConnectOptions](),
	PkTransactionFlags:    reflect.TypeFor[transactionFlags](),
	PkStatementContext:    reflect.TypeFor[statementContext](),
	PkDBConnectInfo:       reflect.TypeFor[DBConnectInfo](),
	/*
	   parts that cannot be used generically as additional parameters are needed

	   PkParameterMetadata
	   PkParameters
	   PkOutputParameters
	   PkResultMetadata
	   PkResultset
	*/
}

// to be implemented by parts needing initialization
// in case the part is instantiated generically.
type initer interface {
	init()
}
