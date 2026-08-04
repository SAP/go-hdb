package protocol

import (
	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

func decodeResult(tc typeCode, dec *encoding.Decoder, attrs *ReaderAttrs, lobReader LobReader, scale int) (any, error) { //nolint: gocyclo
	switch tc {
	case tcBoolean:
		return dec.BooleanField()
	case tcTinyint:
		if !dec.Bool() { // null value
			return nil, nil
		}
		return int64(dec.Byte()), nil
	case tcSmallint:
		if !dec.Bool() { // null value
			return nil, nil
		}
		return int64(dec.Int16()), nil
	case tcInteger:
		if !dec.Bool() { // null value
			return nil, nil
		}
		return int64(dec.Int32()), nil
	case tcBigint:
		if !dec.Bool() { // null value
			return nil, nil
		}
		return dec.Int64(), nil
	case tcReal:
		return dec.RealField()
	case tcDouble:
		return dec.DoubleField()
	case tcDate:
		return dec.DateField()
	case tcTime:
		return dec.TimeField()
	case tcTimestamp:
		return dec.TimestampField()
	case tcLongdate:
		return dec.LongdateField()
	case tcSeconddate:
		return dec.SeconddateField()
	case tcDaydate:
		return dec.DaydateField(attrs.emptyDateAsNull)
	case tcSecondtime:
		return dec.SecondtimeField()
	case tcDecimal:
		return dec.DecimalField()
	case tcFixed8:
		return dec.Fixed8Field(scale)
	case tcFixed12:
		return dec.Fixed12Field(scale)
	case tcFixed16:
		return dec.Fixed16Field(scale)
	case tcChar, tcVarchar, tcString, tcBstring, tcBinary, tcVarbinary:
		return dec.VarField()
	case tcAlphanum:
		return dec.AlphanumField(attrs.alphanumDfv1)
	case tcNchar, tcNvarchar, tcNstring, tcShorttext:
		return dec.Cesu8Field(attrs.tr)
	case tcStPoint, tcStGeometry:
		return dec.HexField()
	case tcBlob, tcClob, tcLocator, tcBintext:
		descr := newLobOutDescr(nil, lobReader, attrs.lobChunkSize)
		if descr.decode(dec) {
			return nil, nil
		}
		return descr, nil
	case tcText, tcNclob, tcNlocator:
		descr := newLobOutDescr(attrs.tr, lobReader, attrs.lobChunkSize)
		if descr.decode(dec) {
			return nil, nil
		}
		return descr, nil
	default:
		panic("invalid type code")
	}
}

func decodeLobParameter(d *encoding.Decoder) (any, error) {
	// real decoding (sniffer) not yet supported
	// descr := &LobInDescr{}
	// descr.Opt = LobOptions(d.Byte())
	// descr._size = int(d.Int32())
	// descr.pos = int(d.Int32())
	d.Byte()
	d.Int32()
	d.Int32()
	return nil, nil
}

func decodeParameter(tc typeCode, d *encoding.Decoder, attrs *ReaderAttrs, scale int) (any, error) {
	switch tc {
	case tcBoolean:
		return d.BooleanField()
	case tcTinyint:
		return int64(d.Byte()), nil
	case tcSmallint:
		return int64(d.Int16()), nil
	case tcInteger:
		return int64(d.Int32()), nil
	case tcBigint:
		return d.Int64(), nil
	case tcReal:
		return d.RealField()
	case tcDouble:
		return d.DoubleField()
	case tcDate:
		return d.DateField()
	case tcTime:
		return d.TimeField()
	case tcTimestamp:
		return d.TimestampField()
	case tcLongdate:
		return d.LongdateField()
	case tcSeconddate:
		return d.SeconddateField()
	case tcDaydate:
		return d.DaydateField(attrs.emptyDateAsNull)
	case tcSecondtime:
		return d.SecondtimeField()
	case tcDecimal:
		return d.DecimalField()
	case tcFixed8:
		return d.Fixed8Field(scale)
	case tcFixed12:
		return d.Fixed12Field(scale)
	case tcFixed16:
		return d.Fixed16Field(scale)
	case tcChar, tcVarchar, tcString, tcBstring, tcBinary, tcVarbinary:
		return d.VarField()
	case tcAlphanum:
		return d.AlphanumField(attrs.alphanumDfv1)
	case tcNchar, tcNvarchar, tcNstring, tcShorttext:
		return d.Cesu8Field(attrs.tr)
	case tcStPoint, tcStGeometry:
		return d.HexField()
	case tcBlob, tcClob, tcLocator, tcBintext:
		return decodeLobParameter(d)
	case tcText, tcNclob, tcNlocator:
		return decodeLobParameter(d)
	default:
		panic("invalid type code")
	}
}
