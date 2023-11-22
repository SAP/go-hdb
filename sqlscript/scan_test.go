package sqlscript

import (
	"bufio"
	"strings"
	"testing"
)

func testScan(t *testing.T, separator rune, comments bool, script string, result []string) {
	scanner := bufio.NewScanner(strings.NewReader(script))
	if separator == DefaultSeparator && !comments {
		// if default values use Scan function directly
		scanner.Split(Scan)
	} else {
		// else use SplitFunc 'getter'
		scanner.Split(ScanFunc(separator, comments))
	}

	l := len(result)
	i := 0
	for scanner.Scan() {
		//t.Logf("statement %d\n%s", i, scanner.Bytes())

		if l <= i {
			t.Fatalf("for scan line %d result line is missing", i)
		}

		text := scanner.Text()
		if text != result[i] {
			t.Fatalf("line %d got text\n%s\nexpected\n%s", i, text, result[i])
		}
		i++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if i != l {
		t.Fatalf("got number of lines: %d - expected %d", i, l)
	}
}

func TestScript(t *testing.T) {
	testScript := `
--Comment 1
--Comment 2
STATEMENT;

--Comment 3
STATEMENT WITH PARAMETERS;

--Comment 4
STATEMENT WITH QUOTED LIST 'a,b,c';
STATEMENT WITHOUT COMMENT;

--COMMENT 5
MULTI LINE STATEMENT WITH
 PARAMETERS A
 B AND C;

--COMMENT 6
MULTI LINE STATEMENT WITH SINGE QUOTED PARAMER '
--A
--B
--C' LOOKING LIKE COMMENTS;

--COMMENT 7
MULTI LINE STATEMENT WITH DOUBLE QUOTED PARAMER "
--A
--B
--C" LOOKING LIKE COMMENTS;
`
	noCommentsResult := []string{
		"STATEMENT",
		"STATEMENT WITH PARAMETERS",
		"STATEMENT WITH QUOTED LIST 'a,b,c'",
		"STATEMENT WITHOUT COMMENT",
		"MULTI LINE STATEMENT WITH PARAMETERS A B AND C",
		"MULTI LINE STATEMENT WITH SINGE QUOTED PARAMER '--A--B--C' LOOKING LIKE COMMENTS",
		"MULTI LINE STATEMENT WITH DOUBLE QUOTED PARAMER \"--A--B--C\" LOOKING LIKE COMMENTS",
	}

	commentsResult := []string{
		"--Comment 1\n--Comment 2\nSTATEMENT",
		"--Comment 3\nSTATEMENT WITH PARAMETERS",
		"--Comment 4\nSTATEMENT WITH QUOTED LIST 'a,b,c'",
		"STATEMENT WITHOUT COMMENT",
		"--COMMENT 5\nMULTI LINE STATEMENT WITH PARAMETERS A B AND C",
		"--COMMENT 6\nMULTI LINE STATEMENT WITH SINGE QUOTED PARAMER '--A--B--C' LOOKING LIKE COMMENTS",
		"--COMMENT 7\nMULTI LINE STATEMENT WITH DOUBLE QUOTED PARAMER \"--A--B--C\" LOOKING LIKE COMMENTS",
	}

	testScan(t, DefaultSeparator, false, testScript, noCommentsResult)
	testScan(t, DefaultSeparator, true, testScript, commentsResult)
}
