// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Package trace implements a very simple tracing package.
package trace

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

// A Trace reprecents a tracing object.
type Trace struct {
	*log.Logger
}

// NewTrace returns a new trace object.
func NewTrace(prefix ...string) *Trace {
	return &Trace{Logger: log.New(io.Discard, fmt.Sprintf("%s ", strings.Join(prefix, " ")), log.Ldate|log.Ltime|log.Lshortfile)}
}

// On returns true if the tracing output is enabled, else otherwise.
func (t *Trace) On() bool { return t.Writer() != io.Discard }

// SetOn enables or disabled the tracing output.
func (t *Trace) SetOn(on bool) {
	if on {
		t.SetOutput(os.Stdout)
	} else {
		t.SetOutput(io.Discard)
	}
}

// A Flag represents a boolean value to be used as flag to enable or disable tracing output.
type Flag struct {
	trace *Trace
}

// NewFlag returns a new Flag instance.
func NewFlag(trace *Trace) *Flag { return &Flag{trace: trace} }

func (f *Flag) String() string {
	/*
		The flag package does create flags via reflection to determine default values.
		As this is not using the constructor the flag attributes are not set.
	*/
	if f.trace == nil {
		return strconv.FormatBool(false) // default value
	}
	return strconv.FormatBool(f.trace.On())
}

// IsBoolFlag implements the flag.Value interface.
func (f *Flag) IsBoolFlag() bool { return true }

// Set implements the flag.Value interface.
func (f *Flag) Set(s string) error {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	f.trace.SetOn(b)
	return nil
}
