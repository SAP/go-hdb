// +build go1.10

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

/*
Package scanner implements a HANA SQL query scanner.

For a detailed HANA SQL query syntax please see
https://help.sap.com/doc/6254b3bb439c4f409a979dc407b49c9b/2.0.00/en-US/SAP_HANA_SQL_Script_Reference_en.pdf
*/
package scanner

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"unicode"
)

// ErrToken is raised when a token is malformed (e.g. string with missing ending quote).
var ErrToken = errors.New("Invalid token")

// Token constants.
const (
	EOF = -(iota + 1)
	Error
	Undefined
	Operator
	Delimiter
	IdentifierDelimiter
	Identifier
	QuotedIdentifier
	Variable
	PosVariable
	NamedVariable
	String
	Number
)

var tokenString = map[rune]string{
	EOF:                 "EOF",
	Error:               "Error",
	Undefined:           "Undefined",
	Operator:            "Operator",
	Delimiter:           "Delimiter",
	IdentifierDelimiter: "IdentifierDelimiter",
	Identifier:          "Identifier",
	QuotedIdentifier:    "QuotedIdentifier",
	Variable:            "Variable",
	PosVariable:         "PosVariable",
	NamedVariable:       "NamedVariable",
	String:              "String",
	Number:              "Number",
}

// TokenString returns a printable string for a token or Unicode character.
func TokenString(tok rune) string {
	if s, ok := tokenString[tok]; ok {
		return s
	}
	return fmt.Sprintf("%q", string(tok))
}

var compositeOperators = map[string]struct{}{"<>": struct{}{}, "<=": struct{}{}, ">=": struct{}{}, "!=": struct{}{}}

func isOperator(ch rune) bool           { return strings.ContainsRune("<>=!", ch) }
func isCompositeOperator(s string) bool { _, ok := compositeOperators[s]; return ok }
func isDelimiter(ch rune) bool          { return strings.ContainsRune(",;(){}[]", ch) }
func isNameDelimiter(ch rune) bool      { return ch == '.' }
func isDigit(ch rune) bool              { return unicode.IsDigit(ch) }
func isNumber(ch rune) bool             { return ch == '+' || ch == '-' || isDigit(ch) }
func isExp(ch rune) bool                { return ch == 'e' || ch == 'E' }
func isDecimalSeparator(ch rune) bool   { return ch == '.' }
func isIdentifier(ch rune) bool         { return ch == '_' || unicode.IsLetter(ch) }
func isAlpha(ch rune) bool              { return ch == '#' || ch == '$' || isIdentifier(ch) || isDigit(ch) }
func isSingleQuote(ch rune) bool        { return ch == '\'' }
func isDoubleQuote(ch rune) bool        { return ch == '"' }
func isQuestionMark(ch rune) bool       { return ch == '?' }
func isColon(ch rune) bool              { return ch == ':' }

var scannerPool = sync.Pool{}

// Scanner is a HANA SQL query string scanner.
type Scanner struct {
	rd  *strings.Reader
	b   strings.Builder
	ch  rune  // next char
	err error // last error
	cnt int
}

// NewScanner creates a new Scaner instance.
func NewScanner(str string) *Scanner {
	s, _ := scannerPool.Get().(*Scanner)
	if s == nil {
		s = &Scanner{rd: strings.NewReader(str)}
	} else {
		s.rd.Reset(str)
	}
	s.ch = -2
	s.err = nil
	return s
}

/*
FreeScanner pools the Scanner instance for later usage.
Scanner instances are not thread safe.
After the call of FreeScanner calls of Scanner methods lead to undefined behavior.
*/
func (s *Scanner) FreeScanner() {
	scannerPool.Put(s)
}

func (s *Scanner) read() rune {
	ch, _, err := s.rd.ReadRune()
	switch err {
	case nil:
		return ch
	case io.EOF:
		return EOF
	default:
		return Error
	}
}

func (s *Scanner) peek() rune {
	if s.ch == -2 {
		s.ch = s.read()
	}
	return s.ch
}

func (s *Scanner) next() rune {
	ch := s.peek()
	if ch != EOF {
		s.ch = s.read()
	}
	return ch
}

// Next returns the next Token.
func (s *Scanner) Next() rune {

	s.b.Reset()

	ch := s.next()

	for unicode.IsSpace(ch) {
		ch = s.next()
	}

	tok := ch

	switch {

	default:
		tok = Undefined
		ch = s.next()

	case ch == EOF:
		break

	case isOperator(ch):
		tok = Operator
		ch = s.scanOperator(ch)

	case isDelimiter(ch):
		tok = Delimiter
		s.b.WriteRune(ch)
		ch = s.next()

	case isNameDelimiter(ch):
		tok = IdentifierDelimiter
		s.b.WriteRune(ch)
		ch = s.next()

	case isIdentifier(ch):
		tok = Identifier
		s.b.WriteRune(ch)
		ch = s.scanAlpha()

	case isSingleQuote(ch):
		tok = String
		ch = s.scanQuotedIdentifier(ch)

	case isDoubleQuote(ch):
		tok = QuotedIdentifier
		ch = s.scanQuotedIdentifier(ch)

	case isQuestionMark(ch):
		tok = Variable
		ch = s.next()

	case isColon(ch):
		ch = s.peek()
		if isDigit(ch) {
			tok = PosVariable
			ch = s.scanNumeric()
		} else {
			tok = NamedVariable
			ch = s.scanAlpha()
		}

	case isNumber(ch):
		tok = Number
		ch = s.scanNumber()
	}

	s.ch = ch
	s.rd.UnreadRune()

	if ch == Error {
		return ch
	}

	return tok
}

// Value returns the value of the Token returned by Next.
func (s *Scanner) Value() string {
	return s.b.String()
}

func (s *Scanner) scanOperator(ch1 rune) rune {
	s.b.WriteRune(ch1)
	ch2 := s.next()
	if isCompositeOperator(string([]rune{ch1, ch2})) {
		s.b.WriteRune(ch2)
		return s.next()
	}
	return ch2
}

func (s *Scanner) scanAlpha() rune {
	ch := s.next()
	for isAlpha(ch) {
		s.b.WriteRune(ch)
		ch = s.next()
	}
	return ch
}

func (s *Scanner) scanNumeric() rune {
	ch := s.next()
	for isDigit(ch) {
		ch = s.next()
	}
	return ch
}

func (s *Scanner) scanQuotedIdentifier(quote rune) rune {
	ch := s.next()
	for {
		if ch == quote {
			ch = s.next()
			if ch != quote {
				return ch
			}
		}
		s.b.WriteRune(ch)
		ch = s.next()
		if ch == EOF || ch == Error {
			return Error
		}
	}
}

func (s *Scanner) scanNumber() rune {
	ch := s.scanNumeric()
	if isDecimalSeparator(ch) {
		ch = s.scanNumeric()
	}
	if isExp(ch) {
		ch = s.next()
		if isNumber(ch) {
			ch = s.scanNumeric()
		}
	}
	return ch
}
