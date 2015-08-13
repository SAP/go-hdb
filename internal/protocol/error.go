/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package protocol

import (
	"fmt"

	"github.com/SAP/go-hdb/internal/bufio"
)

const (
	sqlStateSize = 5
)

type sqlState [sqlStateSize]byte

type hdbError struct {
	errorCode       int32
	errorPosition   int32
	errorTextLength int32
	errorLevel      ErrorLevel
	sqlState        sqlState
	errorText       []byte
}

func newHdbError() *hdbError {
	return &hdbError{}
}

// String implements the Stringer interface.
func (e *hdbError) String() string {
	return fmt.Sprintf("errorCode %d, errorPosition %d, errorTextLength % d errorLevel %s, sqlState %s errorText %s",
		e.errorCode,
		e.errorPosition,
		e.errorTextLength,
		e.errorLevel,
		e.sqlState,
		e.errorText,
	)
}

// Error implements the Error interface.
func (e *hdbError) Error() string {
	return fmt.Sprintf("SQL Error %d - %s", e.errorCode, e.errorText)
}

// Code implements the hdb.Error interface.
func (e *hdbError) Code() int {
	return int(e.errorCode)
}

// Position implements the hdb.Error interface.
func (e *hdbError) Position() int {
	return int(e.errorPosition)
}

// Level Position implements the hdb.Error interface.
func (e *hdbError) Level() ErrorLevel {
	return e.errorLevel
}

// Text Position implements the hdb.Error interface.
func (e *hdbError) Text() string {
	return string(e.errorText)
}

func (e *hdbError) kind() partKind {
	return pkError
}

func (e *hdbError) setNumArg(int) {
	// not needed
}

func (e *hdbError) read(rd *bufio.Reader) error {
	var err error

	if e.errorCode, err = rd.ReadInt32(); err != nil {
		return err
	}
	if e.errorPosition, err = rd.ReadInt32(); err != nil {
		return err
	}
	if e.errorTextLength, err = rd.ReadInt32(); err != nil {
		return err
	}

	el, err := rd.ReadInt8()
	if err != nil {
		return err
	}
	e.errorLevel = ErrorLevel(el)

	if err := rd.ReadFull(e.sqlState[:]); err != nil {
		return err
	}

	if e.errorText, err = rd.ReadCesu8(int(e.errorTextLength)); err != nil {
		return err
	}

	// part bufferlength is by one greater than real error length? --> read filler byte
	if _, err := rd.ReadByte(); err != nil {
		return err
	}
	if trace {
		outLogger.Printf("error: %s", e)
	}

	return nil
}
