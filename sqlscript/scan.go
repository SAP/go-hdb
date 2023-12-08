package sqlscript

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"unicode"
	"unicode/utf8"
)

// DefaultSeparator is the default script statement separator.
const DefaultSeparator = ';'

const (
	nl          = '\n'
	cr          = '\r'
	minus       = '-'
	singleQuote = '\''
	doubleQuote = '"'
)

type scanner struct {
	separator rune
	comments  bool
	data      []byte
	atEOF     bool
	token     []byte
}

func (s *scanner) init(data []byte, atEOF bool) {
	s.data, s.atEOF, s.token = data, atEOF, nil
}

func (s *scanner) nextRune() (rune, int, error) {
	if len(s.data) < 1 {
		return -1, 0, io.EOF
	}

	// ASCII
	if s.data[0] < utf8.RuneSelf {
		return rune(s.data[0]), 1, nil
	}

	// correct UTF-8 decode without error
	if r, width := utf8.DecodeRune(s.data); width > 1 {
		return r, width, nil
	}
	return -1, 0, io.EOF
}

func (s *scanner) peekRune() (rune, error) {
	r, _, err := s.nextRune()
	return r, err
}

func (s *scanner) readRune() (rune, error) {
	r, width, err := s.nextRune()
	s.data = s.data[width:]
	return r, err
}

func (s *scanner) appendRune(r rune) {
	s.token = utf8.AppendRune(s.token, r)
}

func (s *scanner) appendLine(data []byte) {
	l := len(data)
	if l == 0 {
		return
	}
	if data[l-1] == cr { // cut off trailing \r
		l--
	}
	s.token = append(s.token, s.data[:l]...)
}

func (s *scanner) scanWhitespace() error {
	for {
		r, err := s.peekRune()
		if err != nil {
			return err
		}
		if !unicode.IsSpace(r) {
			return nil
		}
		if _, err := s.readRune(); err != nil {
			return err
		}
	}
}

func (s *scanner) scanComment() (bool, error) {
	if len(s.data) < 2 {
		if s.atEOF {
			return false, nil
		}
		return false, io.EOF
	}

	if s.data[0] != minus || s.data[1] != minus {
		return false, nil
	}

	if i := bytes.IndexByte(s.data, nl); i >= 0 {
		// terminated line
		if s.comments {
			s.appendLine(s.data[:i])
		}
		s.data = s.data[i+1:]
		return true, nil
	}

	if s.atEOF {
		// non-terminated final line
		if s.comments {
			s.appendLine(s.data)
		}
		s.data = s.data[len(s.data):]
		return true, nil
	}

	// need more data
	return false, io.EOF
}

func (s *scanner) scanString(quote rune) error {
	s.appendRune(quote)
	for {
		r, err := s.readRune()
		if err != nil {
			return err
		}
		switch r {
		case quote:
			s.appendRune(r)
			r2, err := s.peekRune()
			if err != nil {
				return err
			}
			if r2 != quote {
				return nil
			}
		case nl, cr:
			// skip line endings
		default:
			s.appendRune(r)
		}
	}
}

func (s *scanner) scanStatement() (bool, error) {
	for {
		r, err := s.readRune()
		if err != nil {
			return false, err
		}

		switch r {
		case singleQuote, doubleQuote:
			if err := s.scanString(r); err != nil {
				return false, err
			}
		case s.separator:
			return true, nil
		default:
			if !(r == nl || r == cr) { // skip line endings
				s.appendRune(r)
			}
		}
	}
}

func (s *scanner) _scan() (bool, error) {
	for {
		if err := s.scanWhitespace(); err != nil {
			return false, err
		}
		ok, err := s.scanComment()
		if err != nil {
			return false, err
		}
		if !ok {
			break
		}
		if s.comments {
			s.appendRune(nl)
		}
	}
	return s.scanStatement()
}

func (s *scanner) scan(data []byte, atEOF bool) (int, []byte, error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	s.init(data, atEOF)

	ok, err := s._scan()
	if errors.Is(err, io.EOF) {
		return 0, nil, nil // need more data
	}
	if err != nil {
		return 0, nil, err
	}

	advance := len(data) - len(s.data)

	if !ok { // no statement found
		if atEOF {
			return advance, nil, nil // seems like the script does only consist of comments
		}
		return 0, nil, nil // need more data to find the first statement
	}

	return advance, s.token, nil
}

// Scan is a split function for a bufio.Scanner that returns each statement as a token.
// It uses the default separator ';'. Comments are discarded - for adding leading comments
// to each statement or specify a different separator please use SplitFunc.
func Scan(data []byte, atEOF bool) (advance int, token []byte, err error) {
	s := scanner{separator: DefaultSeparator, comments: false}
	return s.scan(data, atEOF)
}

// ScanFunc returns a split function for a bufio.Scanner that returns each command as a token.
// In contrast of using the Scan function directly, the command separator can be specified.
// If comments is true, leading comments are added to each statement and discarded otherwise.
func ScanFunc(separator rune, comments bool) bufio.SplitFunc {
	s := scanner{separator: separator, comments: true}
	return s.scan
}
