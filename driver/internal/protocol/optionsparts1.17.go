//go:build !go1.18
// +build !go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"fmt"
	"sort"

	"github.com/SAP/go-hdb/driver/internal/protocol/encoding"
)

// ClientContext represents a client context part.
type ClientContext map[ClientContextOption]interface{}

// ConnectOptions represents the set of connect options.
type ConnectOptions map[ConnectOption]interface{}

// DBConnectInfo represents a database connect info part.
type DBConnectInfo map[DBConnectInfoType]interface{}

// statementContext represents a statemant context part.
type statementContext map[statementContextType]interface{}

// transactionFlags represents a transaction flags part.
type transactionFlags map[transactionFlagType]interface{}

func (ops ClientContext) String() string {
	s := []string{}
	for i, typ := range ops {
		s = append(s, fmt.Sprintf("%s: %v", ClientContextOption(i), typ))
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return fmt.Sprintf("%v", s)
}

func (ops ConnectOptions) String() string {
	s := []string{}
	for i, typ := range ops {
		s = append(s, fmt.Sprintf("%s: %v", ConnectOption(i), typ))
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return fmt.Sprintf("%v", s)
}

func (ops DBConnectInfo) String() string {
	s := []string{}
	for i, typ := range ops {
		s = append(s, fmt.Sprintf("%s: %v", DBConnectInfoType(i), typ))
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return fmt.Sprintf("%v", s)
}

func (ops statementContext) String() string {
	s := []string{}
	for i, typ := range ops {
		s = append(s, fmt.Sprintf("%s: %v", statementContextType(i), typ))
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return fmt.Sprintf("%v", s)
}

func (ops transactionFlags) String() string {
	s := []string{}
	for i, typ := range ops {
		s = append(s, fmt.Sprintf("%s: %v", transactionFlagType(i), typ))
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return fmt.Sprintf("%v", s)
}

func (ops ClientContext) size() int {
	size := 2 * len(ops) //option + type
	for _, v := range ops {
		ot := getOptType(v)
		size += ot.size(v)
	}
	return size
}
func (ops ConnectOptions) size() int {
	size := 2 * len(ops) //option + type
	for _, v := range ops {
		ot := getOptType(v)
		size += ot.size(v)
	}
	return size
}
func (ops DBConnectInfo) size() int {
	size := 2 * len(ops) //option + type
	for _, v := range ops {
		ot := getOptType(v)
		size += ot.size(v)
	}
	return size
}
func (ops statementContext) size() int {
	size := 2 * len(ops) //option + type
	for _, v := range ops {
		ot := getOptType(v)
		size += ot.size(v)
	}
	return size
}
func (ops transactionFlags) size() int {
	size := 2 * len(ops) //option + type
	for _, v := range ops {
		ot := getOptType(v)
		size += ot.size(v)
	}
	return size
}

func (ops ClientContext) numArg() int    { return len(ops) }
func (ops ConnectOptions) numArg() int   { return len(ops) }
func (ops DBConnectInfo) numArg() int    { return len(ops) }
func (ops statementContext) numArg() int { return len(ops) }
func (ops transactionFlags) numArg() int { return len(ops) }

func (ops *ClientContext) decode(dec *encoding.Decoder, ph *PartHeader) error {
	*ops = ClientContext{} // no reuse of maps - create new one
	for i := 0; i < ph.numArg(); i++ {
		k := ClientContextOption(dec.Int8())
		tc := typeCode(dec.Byte())
		ot := tc.optType()
		(*ops)[k] = ot.decode(dec)
	}
	return dec.Error()
}

func (ops *ConnectOptions) decode(dec *encoding.Decoder, ph *PartHeader) error {
	*ops = ConnectOptions{} // no reuse of maps - create new one
	for i := 0; i < ph.numArg(); i++ {
		k := ConnectOption(dec.Int8())
		tc := typeCode(dec.Byte())
		ot := tc.optType()
		(*ops)[k] = ot.decode(dec)
	}
	return dec.Error()
}

func (ops *DBConnectInfo) decode(dec *encoding.Decoder, ph *PartHeader) error {
	*ops = DBConnectInfo{} // no reuse of maps - create new one
	for i := 0; i < ph.numArg(); i++ {
		k := DBConnectInfoType(dec.Int8())
		tc := typeCode(dec.Byte())
		ot := tc.optType()
		(*ops)[k] = ot.decode(dec)
	}
	return dec.Error()
}

func (ops *statementContext) decode(dec *encoding.Decoder, ph *PartHeader) error {
	*ops = statementContext{} // no reuse of maps - create new one
	for i := 0; i < ph.numArg(); i++ {
		k := statementContextType(dec.Int8())
		tc := typeCode(dec.Byte())
		ot := tc.optType()
		(*ops)[k] = ot.decode(dec)
	}
	return dec.Error()
}

func (ops *transactionFlags) decode(dec *encoding.Decoder, ph *PartHeader) error {
	*ops = transactionFlags{} // no reuse of maps - create new one
	for i := 0; i < ph.numArg(); i++ {
		k := transactionFlagType(dec.Int8())
		tc := typeCode(dec.Byte())
		ot := tc.optType()
		(*ops)[k] = ot.decode(dec)
	}
	return dec.Error()
}

func (ops ClientContext) encode(enc *encoding.Encoder) error {
	for k, v := range ops {
		enc.Int8(int8(k))
		ot := getOptType(v)
		enc.Int8(int8(ot.typeCode()))
		ot.encode(enc, v)
	}
	return nil
}

func (ops ConnectOptions) encode(enc *encoding.Encoder) error {
	for k, v := range ops {
		enc.Int8(int8(k))
		ot := getOptType(v)
		enc.Int8(int8(ot.typeCode()))
		ot.encode(enc, v)
	}
	return nil
}

func (ops DBConnectInfo) encode(enc *encoding.Encoder) error {
	for k, v := range ops {
		enc.Int8(int8(k))
		ot := getOptType(v)
		enc.Int8(int8(ot.typeCode()))
		ot.encode(enc, v)
	}
	return nil
}

func (ops statementContext) encode(enc *encoding.Encoder) error {
	for k, v := range ops {
		enc.Int8(int8(k))
		ot := getOptType(v)
		enc.Int8(int8(ot.typeCode()))
		ot.encode(enc, v)
	}
	return nil
}

func (ops transactionFlags) encode(enc *encoding.Encoder) error {
	for k, v := range ops {
		enc.Int8(int8(k))
		ot := getOptType(v)
		enc.Int8(int8(ot.typeCode()))
		ot.encode(enc, v)
	}
	return nil
}
