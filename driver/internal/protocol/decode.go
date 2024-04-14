package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

func decodeLobResult(d *encoding.Decoder, isCharBased bool) (any, error) {
	descr := &LobOutDescr{IsCharBased: isCharBased}
	descr.ltc = lobTypecode(d.Int8())
	descr.Opt = LobOptions(d.Int8())
	if descr.Opt.isNull() {
		return nil, nil
	}
	d.Skip(2)
	descr.NumChar = d.Int64()
	descr.numByte = d.Int64()
	descr.ID = LocatorID(d.Uint64())
	size := int(d.Int32())
	descr.B = make([]byte, size)
	d.Bytes(descr.B)
	return descr, nil
}

func decodeResult(tc typeCode, d *encoding.Decoder, scale int) (any, error) {
	switch tc {
	case tcBoolean:
		return d.BooleanField()
	case tcTinyint:
		if !d.Bool() { // null value
			return nil, nil
		}
		return int64(d.Byte()), nil
	case tcSmallint:
		if !d.Bool() { // null value
			return nil, nil
		}
		return int64(d.Int16()), nil
	case tcInteger:
		if !d.Bool() { // null value
			return nil, nil
		}
		return int64(d.Int32()), nil
	case tcBigint:
		if !d.Bool() { // null value
			return nil, nil
		}
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
		return d.DaydateField()
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
	case tcChar, tcVarchar, tcString, tcBinary, tcVarbinary:
		return d.VarField()
	case tcAlphanum:
		return d.AlphanumField()
	case tcNchar, tcNvarchar, tcNstring, tcShorttext:
		return d.Cesu8Field()
	case tcStPoint, tcStGeometry:
		return d.HexField()
	case tcBlob, tcClob, tcLocator, tcBintext:
		return decodeLobResult(d, false)
	case tcText, tcNclob, tcNlocator:
		return decodeLobResult(d, true)
	default:
		panic(fmt.Sprintf("invalid type code %s", tc))
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

func decodeParameter(tc typeCode, d *encoding.Decoder, scale int) (any, error) {
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
		return d.DaydateField()
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
	case tcChar, tcVarchar, tcString, tcBinary, tcVarbinary:
		return d.VarField()
	case tcAlphanum:
		return d.AlphanumField()
	case tcNchar, tcNvarchar, tcNstring, tcShorttext:
		return d.Cesu8Field()
	case tcStPoint, tcStGeometry:
		return d.HexField()
	case tcBlob, tcClob, tcLocator, tcBintext:
		return decodeLobParameter(d)
	case tcText, tcNclob, tcNlocator:
		return decodeLobParameter(d)
	default:
		panic(fmt.Sprintf("invalid type code %s", tc))
	}
}
