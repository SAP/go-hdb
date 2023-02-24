package driver

import (
	"database/sql/driver"
	"io"
	"reflect"

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// check if rows types do implement all driver row interfaces.
var (
	_ driver.Rows = (*noResultType)(nil)

	_ driver.Rows                           = (*queryResult)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*queryResult)(nil)
	_ driver.RowsColumnTypeLength           = (*queryResult)(nil)
	_ driver.RowsColumnTypeNullable         = (*queryResult)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*queryResult)(nil)
	_ driver.RowsColumnTypeScanType         = (*queryResult)(nil)
	/*
		currently not used
		could be implemented as pointer to next queryResult (advancing by copying data from next)
		_ driver.RowsNextResultSet = (*queryResult)(nil)
	*/

	_ driver.Rows = (*callResult)(nil)
)

type prepareResult struct {
	fc              p.FunctionCode
	stmtID          uint64
	parameterFields []*p.ParameterField
	resultFields    []*p.ResultField
}

// isProcedureCall returns true if the statement is a call statement.
func (pr *prepareResult) isProcedureCall() bool { return pr.fc.IsProcedureCall() }

// numField returns the number of parameter fields in a database statement.
func (pr *prepareResult) numField() int { return len(pr.parameterFields) }

// NoResult is the driver.Rows drop-in replacement if driver Query or QueryRow is used for statements that do not return rows.
var noResult = new(noResultType)

var noColumns = []string{}

type noResultType struct{}

func (r *noResultType) Columns() []string              { return noColumns }
func (r *noResultType) Close() error                   { return nil }
func (r *noResultType) Next(dest []driver.Value) error { return io.EOF }

// queryResult represents the resultset of a query.
type queryResult struct {
	// field alignment
	fields       []*p.ResultField
	fieldValues  []driver.Value
	decodeErrors p.DecodeErrors
	_columns     []string
	lastErr      error
	conn         *conn
	rsID         uint64
	pos          int
	attributes   p.PartAttributes
}

// Columns implements the driver.Rows interface.
func (qr *queryResult) Columns() []string {
	if qr._columns == nil {
		numField := len(qr.fields)
		qr._columns = make([]string, numField)
		for i := 0; i < numField; i++ {
			qr._columns[i] = qr.fields[i].Name()
		}
	}
	return qr._columns
}

// Close implements the driver.Rows interface.
func (qr *queryResult) Close() error {
	if qr.attributes.ResultsetClosed() {
		return nil
	}
	// if lastError is set, attrs are nil
	if qr.lastErr != nil {
		return qr.lastErr
	}
	return qr.conn._closeResultsetID(qr.rsID)
}

func (qr *queryResult) numRow() int {
	if len(qr.fieldValues) == 0 {
		return 0
	}
	return len(qr.fieldValues) / len(qr.fields)
}

func (qr *queryResult) copyRow(idx int, dest []driver.Value) {
	cols := len(qr.fields)
	copy(dest, qr.fieldValues[idx*cols:(idx+1)*cols])
}

// Next implements the driver.Rows interface.
func (qr *queryResult) Next(dest []driver.Value) error {
	if qr.pos >= qr.numRow() {
		if qr.attributes.LastPacket() {
			return io.EOF
		}
		if err := qr.conn._fetchNext(qr); err != nil {
			qr.lastErr = err //fieldValues and attrs are nil
			return err
		}
		if qr.numRow() == 0 {
			return io.EOF
		}
		qr.pos = 0
	}

	qr.copyRow(qr.pos, dest)
	err := qr.decodeErrors.RowError(qr.pos)
	qr.pos++

	for _, v := range dest {
		if v, ok := v.(p.LobDecoderSetter); ok {
			v.SetDecoder(qr.conn.decodeLob)
		}
	}
	return err
}

// ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeDatabaseTypeName interface.
func (qr *queryResult) ColumnTypeDatabaseTypeName(idx int) string { return qr.fields[idx].TypeName() }

// ColumnTypeLength implements the driver.RowsColumnTypeLength interface.
func (qr *queryResult) ColumnTypeLength(idx int) (int64, bool) { return qr.fields[idx].TypeLength() }

// ColumnTypeNullable implements the driver.RowsColumnTypeNullable interface.
func (qr *queryResult) ColumnTypeNullable(idx int) (bool, bool) {
	return qr.fields[idx].Nullable(), true
}

// ColumnTypePrecisionScale implements the driver.RowsColumnTypePrecisionScale interface.
func (qr *queryResult) ColumnTypePrecisionScale(idx int) (int64, int64, bool) {
	return qr.fields[idx].TypePrecisionScale()
}

// ColumnTypeScanType implements the driver.RowsColumnTypeScanType interface.
func (qr *queryResult) ColumnTypeScanType(idx int) reflect.Type {
	return qr.fields[idx].ScanType()
}

type callResult struct { // call output parameters
	conn         *conn
	outputFields []*p.ParameterField
	fieldValues  []driver.Value
	decodeErrors p.DecodeErrors
	_columns     []string
	eof          bool
}

// Columns implements the driver.Rows interface.
func (cr *callResult) Columns() []string {
	if cr._columns == nil {
		numField := len(cr.outputFields)
		cr._columns = make([]string, numField)
		for i := 0; i < numField; i++ {
			cr._columns[i] = cr.outputFields[i].Name()
		}
	}
	return cr._columns
}

// Next implements the driver.Rows interface.
func (cr *callResult) Next(dest []driver.Value) error {
	if len(cr.fieldValues) == 0 || cr.eof {
		return io.EOF
	}

	copy(dest, cr.fieldValues)
	err := cr.decodeErrors.RowError(0)
	cr.eof = true
	for _, v := range dest {
		if v, ok := v.(p.LobDecoderSetter); ok {
			v.SetDecoder(cr.conn.decodeLob)
		}
	}
	return err
}

// Close implements the driver.Rows interface.
func (cr *callResult) Close() error { return nil }
