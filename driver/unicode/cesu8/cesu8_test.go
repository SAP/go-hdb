package cesu8

import (
	"bytes"
	"testing"
	"unicode/utf8"
)

func TestCodeLen(t *testing.T) {
	b := make([]byte, CESUMax)
	for i := rune(0); i <= utf8.MaxRune; i++ {
		n := EncodeRune(b, i)
		if n != RuneLen(i) {
			t.Fatalf("rune length check error %d %d", n, RuneLen(i))
		}
	}
}

type testCP struct {
	cp   rune
	utf8 []byte
}

// see http://en.wikipedia.org/wiki/CESU-8
var testCPData = []*testCP{
	{0x45, []byte{0x45}},
	{0x205, []byte{0xc8, 0x85}},
	{0x10400, []byte{0xed, 0xa0, 0x81, 0xed, 0xb0, 0x80}},
}

func TestCP(t *testing.T) {
	b := make([]byte, CESUMax)
	for _, d := range testCPData {
		n1 := EncodeRune(b, d.cp)
		if !bytes.Equal(b[:n1], d.utf8) {
			t.Logf("encode code point %x char %c cesu-8 %x - expected %x", d.cp, d.cp, b[:n1], d.utf8)
		} else {
			t.Logf("encode code point %x char %c cesu-8 %x", d.cp, d.cp, b[:n1])
		}

		cp, n2 := DecodeRune(b[:n1])
		if cp != d.cp || n2 != n1 {
			t.Logf("decode code point %x size %d - expected %x size %d", cp, n2, d.cp, n1)
		} else {
			t.Logf("decode code point %x size %d", cp, n2)
		}
	}
}

// took from https://golang.org/src/unicode/utf8/utf8_test.go
var testStrings = []string{
	"",
	"abcd",
	"☺☻☹",
	"日a本b語ç日ð本Ê語þ日¥本¼語i日©",
	"日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©日a本b語ç日ð本Ê語þ日¥本¼語i日©",
	"\x80\x80\x80\x80",
}

func TestString(t *testing.T) {
	b := make([]byte, CESUMax)
	for i, s := range testStrings {
		n := 0
		for _, r := range s {
			n += utf8.EncodeRune(b, r)
			if r >= 0xFFFF { // CESU-8: 6 Bytes
				n += 2
			}
		}

		// 1. Test: cesu string size
		m := StringSize(s)
		if m != n {
			t.Fatalf("%d invalid string size %d - expected %d", i, m, n)
		}
		// 2. Test: cesu slice len
		m = Size([]byte(s))
		if m != n {
			t.Fatalf("%d invalid slice size %d - expected %d", i, m, n)
		}
		// 3. Test: convert len
		m = 0
		for _, r := range s {
			m += EncodeRune(b, r)
		}
		if m != n {
			t.Fatalf("%d invalid encoder size %d - expected %d", i, m, n)
		}
	}
}
