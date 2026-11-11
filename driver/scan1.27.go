//go:build go1.27

package driver

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
	"github.com/SAP/go-hdb/driver/internal/unsafe"
)

// scanColumn scans a single column value into the destination. Lob values are scanned
// directly into the destination, all other values are converted via convertAssign
// (no-copy for string and []byte values).
func scanColumn(scanCtx driver.ScanContext, session *session, isLob bool, dest any, src driver.Value) error {
	if isLob {
		return convertAssignLob(scanCtx, session, dest, src)
	}
	return convertAssign(scanCtx, dest, src)
}

func convertAssign(scanCtx driver.ScanContext, dest, src any) error {
	switch src := src.(type) {
	case []byte:
		switch d := dest.(type) {
		case *[]byte:
			*d = src
			return nil
		case *sql.RawBytes:
			*d = src
			return nil
		case *string:
			*d = unsafe.ByteSlice2String(src)
			return nil
		}
	case string:
		switch d := dest.(type) {
		case *string:
			*d = src
			return nil
		case *sql.RawBytes:
			*d = unsafe.String2ByteSlice(src)
			return nil
		case *[]byte:
			*d = unsafe.String2ByteSlice(src)
			return nil
		}
	default:
		if src != nil {
			return sql.ConvertAssign(scanCtx, dest, src)
		}
	}

	if handled, err := scanDestination(scanCtx, nil, dest, src); handled {
		return err
	}
	if src == nil {
		return sql.ConvertAssign(scanCtx, dest, src)
	}

	// reflect tail: string- and []byte-kind named types and pointer chains,
	// aliasing the driver-owned source buffer (never copied).
	dpv := reflect.ValueOf(dest)
	if dpv.Kind() != reflect.Pointer {
		return sql.ConvertAssign(scanCtx, dest, src)
	}
	dv := reflect.Indirect(dpv)
	for dv.Kind() == reflect.Pointer {
		if dv.IsNil() {
			dv.Set(reflect.New(dv.Type().Elem()))
		}
		if handled, err := scanDestination(scanCtx, nil, dv.Interface(), src); handled {
			return err
		}
		dv = dv.Elem()
	}
	switch dv.Kind() {
	case reflect.String:
		if s, ok := src.(string); ok {
			dv.SetString(s)
		} else {
			dv.SetString(unsafe.ByteSlice2String(src.([]byte)))
		}
		return nil
	case reflect.Slice:
		if dv.Type().Elem().Kind() == reflect.Uint8 {
			if b, ok := src.([]byte); ok {
				dv.SetBytes(b)
			} else {
				dv.SetBytes(unsafe.String2ByteSlice(src.(string)))
			}
			return nil
		}
	}
	return sql.ConvertAssign(scanCtx, dest, src)
}

func convertAssignLob(scanCtx driver.ScanContext, session *session, dest, src any) error {
	if src == nil {
		return convertAssign(scanCtx, dest, src)
	}
	switch d := dest.(type) {
	case *string:
		switch s := src.(type) {
		case *p.LobOutDescr:
			// Use fresh storage to preserve any strings retained by the caller.
			// See TestLob/stringRetention. Pre-size from the reported byte count
			// to avoid append regrowth (upper bound for character lobs).
			b := make([]byte, 0, s.ByteSize())
			wr := (*byteSliceWriter)(&b)
			err := session.readLobComplete(s, wr)
			*d = unsafe.ByteSlice2String(b)
			return err
		case string: // mock source
			*d = s
			return nil
		case []byte: // mock source
			*d = string(s)
			return nil
		}
	case *[]byte:
		switch s := src.(type) {
		case *p.LobOutDescr:
			// Reuse the caller's capacity; grow once to the reported byte count
			// if short, to avoid append regrowth (upper bound for character lobs).
			if n := s.ByteSize(); int64(cap(*d)) < n {
				*d = make([]byte, 0, n)
			} else {
				*d = (*d)[:0]
			}
			wr := (*byteSliceWriter)(d)
			return session.readLobComplete(s, wr)
		case string: // mock source
			*d = []byte(s)
			return nil
		case []byte: // mock source
			*d = s
			return nil
		}
	}

	if handled, err := scanDestination(scanCtx, session, dest, src); handled {
		return err
	}

	dpv := reflect.ValueOf(dest)
	if dpv.Kind() != reflect.Pointer {
		return errors.New("destination not a pointer")
	}
	if dpv.IsNil() {
		return errors.New("destination pointer is nil")
	}

	dv := reflect.Indirect(dpv)

	// dereference nested pointers, allocating intermediate values if necessary
	if dv.Kind() == reflect.Pointer {
		if dv.IsNil() {
			dv.Set(reflect.New(dv.Type().Elem()))
		}
		return convertAssignLob(scanCtx, session, dv.Interface(), src)
	}

	// Preserve support for structs shaped like sql.Null[T] without a Scanner.
	if isGenNull, _ := isGenericNull(dv.Type()); isGenNull {
		vField := dv.FieldByName("V")
		validField := dv.FieldByName("Valid")
		if err := convertAssignLob(scanCtx, session, vField.Addr().Interface(), src); err != nil {
			return err
		}
		validField.SetBool(true)
		return nil
	}

	if descr, ok := src.(*p.LobOutDescr); ok {
		// string- and []byte-kind named types: stream pre-sized, mirroring the
		// exact *string / *[]byte cases above (ByteSize is an upper bound for
		// character lobs).
		switch dv.Kind() {
		case reflect.String:
			b := make([]byte, 0, descr.ByteSize())
			wr := (*byteSliceWriter)(&b)
			if err := session.readLobComplete(descr, wr); err != nil {
				return err
			}
			dv.SetString(unsafe.ByteSlice2String(b))
			return nil
		case reflect.Slice:
			if dv.Type().Elem().Kind() == reflect.Uint8 {
				b := make([]byte, 0, descr.ByteSize())
				wr := (*byteSliceWriter)(&b)
				if err := session.readLobComplete(descr, wr); err != nil {
					return err
				}
				dv.SetBytes(b)
				return nil
			}
		}
		wr := &bytes.Buffer{}
		if err := session.readLobComplete(descr, wr); err != nil {
			return err
		}
		return sql.ConvertAssign(scanCtx, dest, wr.Bytes())
	}

	return sql.ConvertAssign(scanCtx, dest, src)
}

// scanDestination handles destinations whose methods control assignment.
// Standard sql.Null[T] is unwrapped first so its V can receive streamed data.
func scanDestination(scanCtx driver.ScanContext, session *session, dest, src any) (bool, error) {
	if scanner, ok := dest.(sql.Scanner); ok {
		if src == nil {
			return true, scanner.Scan(src)
		}
		dv := reflect.ValueOf(dest)
		if dv.Kind() == reflect.Pointer && !dv.IsNil() {
			dv = dv.Elem()
			t := dv.Type()
			if t.PkgPath() == "database/sql" && strings.HasPrefix(t.Name(), "Null[") {
				_, isLob := src.(*p.LobOutDescr)
				vField := dv.FieldByName("V")
				// An interface-typed V (e.g. io.Writer) must receive the value it
				// holds, not its address: *io.Writer is not itself a writer. A
				// concrete V is written through its address, as usual.
				var vDest any
				if vField.Kind() == reflect.Interface && !vField.IsNil() {
					vDest = vField.Interface()
				} else {
					vDest = vField.Addr().Interface()
				}
				err := scanColumn(scanCtx, session, isLob, vDest, src)
				dv.FieldByName("Valid").SetBool(err == nil)
				return true, err
			}
		}
		if descr, ok := src.(*p.LobOutDescr); ok {
			var b bytes.Buffer
			if err := session.readLobComplete(descr, &b); err != nil {
				return true, err
			}
			src = b.Bytes()
		}
		return true, scanner.Scan(src)
	}
	if src == nil {
		if n, ok := dest.(interface{ setNull() }); ok {
			n.setNull()
			return true, nil
		}
		return false, nil
	}
	wr, ok := dest.(io.Writer)
	if !ok {
		return false, nil
	}
	switch src := src.(type) {
	case *p.LobOutDescr:
		return true, session.readLobComplete(src, wr)
	case []byte:
		_, err := wr.Write(src)
		return true, err
	case string:
		_, err := wr.Write([]byte(src))
		return true, err
	default:
		return false, nil
	}
}
