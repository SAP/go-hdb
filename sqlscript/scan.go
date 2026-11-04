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

// ErrInvalidUTF8 is returned when the script contains a byte sequence that
// cannot be decoded as valid UTF-8.
var ErrInvalidUTF8 = errors.New("invalid UTF-8 encoding")

const (
	nl          = '\n'
	cr          = '\r'
	space       = ' '
	minus       = '-'
	slash       = '/'
	asterisk    = '*'
	underscore  = '_'
	hash        = '#'
	dollar      = '$'
	singleQuote = '\''
	doubleQuote = '"'

	lineEndings = "\r\n"
)

var (
	blockCommentEnd = []byte("*/")
	beginKeyword    = []byte("BEGIN")
	caseKeyword     = []byte("CASE")
	endKeyword      = []byte("END")
)

type scanner struct {
	separator rune
	comments  bool
	data      []byte
	atEOF     bool
	token     []byte
	pending   bool // statement content seen, separator not found
}

func (s *scanner) init(data []byte, atEOF bool) {
	s.data, s.atEOF, s.token, s.pending = data, atEOF, nil, false
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

	// a multi-byte sequence that cannot be decoded: more data may still
	// complete it, only at end of input it is invalid
	if s.atEOF {
		return -1, 0, ErrInvalidUTF8
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

func (s *scanner) scanComment(keep bool) (bool, error) {
	if len(s.data) < 2 {
		if s.atEOF {
			return false, nil
		}
		return false, io.EOF
	}

	end := len(s.data)
	switch {
	case s.data[0] == minus && s.data[1] == minus:
		if i := bytes.IndexAny(s.data, lineEndings); i >= 0 {
			end = i + 1
			if s.data[i] == cr {
				if end == len(s.data) && !s.atEOF {
					return false, io.EOF // CRLF may span reads.
				}
				if end < len(s.data) && s.data[end] == nl {
					end++
				}
			}
		} else if !s.atEOF {
			return false, io.EOF
		}
	case s.data[0] == slash && s.data[1] == asterisk:
		if i := bytes.Index(s.data[2:], blockCommentEnd); i >= 0 {
			end = i + 4
		} else if !s.atEOF {
			return false, io.EOF
		}
	default:
		return false, nil
	}

	if !utf8.Valid(s.data[:end]) {
		return false, ErrInvalidUTF8
	}
	if keep {
		s.token = append(s.token, s.data[:end]...)
	}
	s.data = s.data[end:]
	return true, nil
}

func (s *scanner) scanString(quote rune) error {
	for {
		r, err := s.readRune()
		if err != nil {
			return err
		}
		if r != quote {
			continue
		}
		r2, err := s.peekRune()
		if err != nil {
			return err
		}
		if r2 != quote {
			return nil
		}
		// Consume both quotes of an escape without leaving the string.
		if _, err := s.readRune(); err != nil {
			return err
		}
	}
}

func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == underscore || r == hash || r == dollar
}

func (s *scanner) scanWord() ([]byte, error) {
	data := s.data
	for {
		r, width, err := s.nextRune()
		if err != nil {
			if errors.Is(err, io.EOF) && s.atEOF {
				break
			}
			return nil, err
		}
		if r == s.separator || !isWordRune(r) {
			break
		}
		s.data = s.data[width:]
	}
	word := data[:len(data)-len(s.data)]
	return word, nil
}

func (s *scanner) scanStatement() (ok bool, err error) {
	data := s.data
	defer func() {
		if ok || s.atEOF {
			n := len(data) - len(s.data)
			if ok {
				n -= utf8.RuneLen(s.separator)
			}
			s.token = append(s.token, data[:n]...)
		}
	}()

	begins, cases := 0, 0
	end := false // Resolve END after the next word or separator.
	for {
		before := s.data
		if ok, err := s.scanComment(false); err != nil {
			return false, err
		} else if ok {
			if !s.comments {
				s.token = append(s.token, data[:len(data)-len(before)]...)
				n := len(before) - len(s.data)
				if before[0] == minus { // Preserve the line comment's terminating CR/LF.
					if n > 0 && before[n-1] == nl {
						n--
					}
					if n > 0 && before[n-1] == cr {
						n--
					}
				}
				data = before[n:]
				if len(s.token) > 0 && len(data) > 0 {
					last, _ := utf8.DecodeLastRune(s.token)
					first, _ := utf8.DecodeRune(data)
					if !unicode.IsSpace(last) && !unicode.IsSpace(first) {
						s.token = append(s.token, space) // Keep adjoining SQL tokens separate.
					}
				}
			}
			continue
		}

		r, width, err := s.nextRune()
		if err != nil {
			return false, err
		}
		if r != s.separator && isWordRune(r) {
			word, err := s.scanWord()
			if err != nil {
				return false, err
			}
			if end {
				end = false
				if cases == 0 {
					continue // END followed by a word does not close BEGIN.
				}
				cases--
				if bytes.EqualFold(word, caseKeyword) {
					continue // END CASE closes the existing CASE.
				}
			}
			switch {
			case bytes.EqualFold(word, beginKeyword):
				begins++
			case bytes.EqualFold(word, caseKeyword):
				cases++
			case bytes.EqualFold(word, endKeyword):
				end = true
			}
			continue
		}
		if end && (r == s.separator || !unicode.IsSpace(r)) {
			if cases > 0 {
				cases--
			} else if r == s.separator && begins > 0 {
				begins--
			}
			end = false
		}
		s.data = s.data[width:]

		switch r {
		case singleQuote, doubleQuote:
			if err := s.scanString(r); err != nil {
				return false, err
			}
		case s.separator:
			if begins == 0 && cases == 0 {
				return true, nil
			}
		}
	}
}

func (s *scanner) _scan() (bool, error) {
	for {
		if err := s.scanWhitespace(); err != nil {
			return false, err
		}
		ok, err := s.scanComment(s.comments)
		if err != nil {
			return false, err
		}
		if !ok {
			break
		}
	}
	s.pending = len(s.data) > 0 // statement content present
	return s.scanStatement()
}

func (s *scanner) scan(data []byte, atEOF bool) (int, []byte, error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	s.init(data, atEOF)

	ok, err := s._scan()
	if errors.Is(err, io.EOF) {
		if atEOF && s.pending {
			return len(data), s.token, nil // unterminated final statement
		}
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

// Scan splits SQL statements for a bufio.Scanner using ';' as the separator.
// It removes comments and preserves quoted text and line endings.
// Separators inside quotes, comments, or BEGIN/CASE blocks do not split statements.
// Invalid UTF-8 returns ErrInvalidUTF8.
func Scan(data []byte, atEOF bool) (advance int, token []byte, err error) {
	s := scanner{separator: DefaultSeparator, comments: false}
	return s.scan(data, atEOF)
}

// ScanFunc returns a split function like Scan, with a custom separator.
// If comments is true, comments are retained.
func ScanFunc(separator rune, comments bool) bufio.SplitFunc {
	s := scanner{separator: separator, comments: comments}
	return s.scan
}
