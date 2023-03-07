package driver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
)

// ErrEndOfRows is the error to be returned using a function based bulk exec to indicate
// the end of rows.
var ErrEndOfRows = errors.New("end of rows")

type argsScanner interface {
	scan(nvargs []driver.NamedValue) error
}

type singleArgs struct {
	i      int
	nvargs []driver.NamedValue
}

func (it *singleArgs) scan(nvargs []driver.NamedValue) error {
	if it.i != 0 {
		return ErrEndOfRows
	}
	copy(nvargs, it.nvargs)
	it.i++
	return nil
}

type multiArgs struct {
	i      int
	nvargs []driver.NamedValue
}

func (it *multiArgs) scan(nvargs []driver.NamedValue) error {
	if it.i >= len(it.nvargs) {
		return ErrEndOfRows
	}
	n := copy(nvargs, it.nvargs[it.i:])
	it.i += n
	return nil
}

type fctArgs struct {
	fct  func(args []any) error
	args []any
}

func (it *fctArgs) scan(nvargs []driver.NamedValue) error {
	if it.args == nil {
		it.args = make([]any, len(nvargs))
	}
	if err := it.fct(it.args); err != nil {
		return err
	}
	for i, arg := range it.args {
		if t, ok := arg.(sql.NamedArg); ok {
			nvargs[i] = driver.NamedValue{Name: t.Name, Ordinal: i + 1, Value: t.Value}
		} else {
			nvargs[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
		}
	}
	return nil
}

type argsMismatchError struct {
	numArg int
	numPrm int
}

func newArgsMismatchError(numArg, numPrm int) *argsMismatchError {
	return &argsMismatchError{numArg: numArg, numPrm: numPrm}
}

func (e *argsMismatchError) Error() string {
	return fmt.Sprintf("argument parameter mismatch - number of arguments %d number of parameters %d", e.numArg, e.numPrm)
}

func newArgsScanner(numField int, nvargs []driver.NamedValue) (argsScanner, error) {
	numArg := len(nvargs)

	switch numArg {

	case 0:
		if numField == 0 {
			return nil, nil
		}
		return nil, newArgsMismatchError(numArg, numField)

	case 1:
		arg := nvargs[0].Value

		switch numField {
		case 0:
			return nil, newArgsMismatchError(numArg, numField)
		case 1:
			if v, ok := arg.(func(args []any) error); ok {
				return &fctArgs{fct: v}, nil
			}
			return &singleArgs{nvargs: nvargs}, nil
		default:
			if v, ok := arg.(func(args []any) error); ok {
				return &fctArgs{fct: v}, nil
			}
			return nil, fmt.Errorf("invalid argument type %T", arg)
		}

	default:
		if numField == 0 {
			return nil, newArgsMismatchError(numArg, numField)
		}
		switch {
		case numArg == numField:
			return &singleArgs{nvargs: nvargs}, nil
		case numArg%numField == 0:
			return &multiArgs{nvargs: nvargs}, nil
		default:
			return nil, newArgsMismatchError(numArg, numField)
		}
	}
}
