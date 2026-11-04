package sqlscript

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func testScan(t *testing.T, separator rune, comments bool, reader io.Reader, result []string, wantErr error) {
	t.Helper()
	scanner := bufio.NewScanner(reader)
	if separator == DefaultSeparator && !comments {
		// if default values use Scan function directly
		scanner.Split(Scan)
	} else {
		// else use the SplitFunc getter function
		scanner.Split(ScanFunc(separator, comments))
	}

	l := len(result)
	i := 0
	for scanner.Scan() {
		// t.Logf("statement %d\n%s", i, scanner.Bytes())
		if l <= i {
			t.Fatalf("result line is missing for scan line %d", i)
		}
		text := scanner.Text()
		if text != result[i] {
			t.Fatalf("line %d got text\n%s\nexpected\n%s", i, text, result[i])
		}
		i++
	}
	if err := scanner.Err(); !errors.Is(err, wantErr) {
		t.Fatalf("got error %v, expected %v", err, wantErr)
	}
	if i != l {
		t.Fatalf("got number of lines: %d - expected %d", i, l)
	}
}

func TestScript(t *testing.T) {
	t.Parallel()

	const testScript = `
--Comment 0 followed by a newline

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
MULTI LINE STATEMENT WITH SINGLE QUOTED PARAMETER '
--A
--B
--C' LOOKING LIKE COMMENTS;

--COMMENT 7
MULTI LINE STATEMENT WITH DOUBLE QUOTED PARAMETER "
--A
--B
--C" LOOKING LIKE COMMENTS;
`
	noCommentsResult := []string{
		"STATEMENT",
		"STATEMENT WITH PARAMETERS",
		"STATEMENT WITH QUOTED LIST 'a,b,c'",
		"STATEMENT WITHOUT COMMENT",
		"MULTI LINE STATEMENT WITH\n PARAMETERS A\n B AND C",
		"MULTI LINE STATEMENT WITH SINGLE QUOTED PARAMETER '\n--A\n--B\n--C' LOOKING LIKE COMMENTS",
		"MULTI LINE STATEMENT WITH DOUBLE QUOTED PARAMETER \"\n--A\n--B\n--C\" LOOKING LIKE COMMENTS",
	}

	commentsResult := []string{
		"--Comment 0 followed by a newline\n--Comment 1\n--Comment 2\nSTATEMENT",
		"--Comment 3\nSTATEMENT WITH PARAMETERS",
		"--Comment 4\nSTATEMENT WITH QUOTED LIST 'a,b,c'",
		"STATEMENT WITHOUT COMMENT",
		"--COMMENT 5\nMULTI LINE STATEMENT WITH\n PARAMETERS A\n B AND C",
		"--COMMENT 6\nMULTI LINE STATEMENT WITH SINGLE QUOTED PARAMETER '\n--A\n--B\n--C' LOOKING LIKE COMMENTS",
		"--COMMENT 7\nMULTI LINE STATEMENT WITH DOUBLE QUOTED PARAMETER \"\n--A\n--B\n--C\" LOOKING LIKE COMMENTS",
	}

	const controlFlow = `DO BEGIN
  IF 1 = 1 THEN SELECT 1; END /* suffix */ IF;
  FOR i IN 1..2 DO SELECT 2; END FOR;
  WHILE 1 = 0 DO SELECT 3; END WHILE;
  LOOP SELECT 4; END LOOP;
  CASE WHEN 1 = 1 THEN SELECT 5; END CASE;
  SELECT 6;
END`

	const caseAssignment = `DO BEGIN
  DECLARE x INTEGER;
  x := CASE WHEN 1 = 1 THEN CASE WHEN 2 = 2 THEN 10 ELSE 20 END ELSE 30 END;
  SELECT :x FROM DUMMY;
END`

	const commentStatement = `SELECT/* ; END */1, '-- /* literal */', "/* identifier */", -/* first *//* second */-1
-- ; END '
FROM DUMMY`

	testData := []struct {
		separator rune
		script    string
		comments  bool
		result    []string
		wantErr   error
	}{
		{';', testScript, false, noCommentsResult, nil},
		{';', testScript, true, commentsResult, nil},
		{';', "STATEMENT", false, []string{"STATEMENT"}, nil},
		{';', "STATEMENT ONE;\nSTATEMENT TWO", false, []string{"STATEMENT ONE", "STATEMENT TWO"}, nil},
		{';', "-- COMMENT\nSTATEMENT", true, []string{"-- COMMENT\nSTATEMENT"}, nil},
		{';', "-- COMMENT\n-- ANOTHER", true, nil, nil},
		{';', "STATEMENT;\n-- trailing comment", true, []string{"STATEMENT"}, nil},
		{';', "SELECT\n1\nFROM DUMMY", false, []string{"SELECT\n1\nFROM DUMMY"}, nil},
		{';', "SELECT\r\n'first\r\nsecond', \"a\r\nb\" FROM DUMMY;", false, []string{"SELECT\r\n'first\r\nsecond', \"a\r\nb\" FROM DUMMY"}, nil},
		{';', "SELECT 'it''s; fine', '' FROM DUMMY; SELECT 'done'", false, []string{"SELECT 'it''s; fine', '' FROM DUMMY", "SELECT 'done'"}, nil},
		{';', "SELECT \"a\"\";b\" FROM DUMMY; SELECT 2;", false, []string{"SELECT \"a\"\";b\" FROM DUMMY", "SELECT 2"}, nil},
		{';', "SELECT 1 -- ; END '\r\nFROM DUMMY; SELECT 2;", false, []string{"SELECT 1 \r\nFROM DUMMY", "SELECT 2"}, nil},
		{';', "SELECT 1 -- comment\rFROM DUMMY;", false, []string{"SELECT 1 \rFROM DUMMY"}, nil},
		{';', "SELECT /* ; END '\n */ '😀' FROM DUMMY; SELECT 2;", false, []string{"SELECT  '😀' FROM DUMMY", "SELECT 2"}, nil},
		{';', commentStatement + ";", false, []string{"SELECT 1, '-- /* literal */', \"/* identifier */\", - -1\n\nFROM DUMMY"}, nil},
		{';', commentStatement + ";", true, []string{commentStatement}, nil},
		{';', "SELECT 1 FROM DUMMY -- trailing", false, []string{"SELECT 1 FROM DUMMY "}, nil},
		{';', "SELECT 1 FROM DUMMY -- trailing", true, []string{"SELECT 1 FROM DUMMY -- trailing"}, nil},
		{';', "/* leading ; */ SELECT 1; /* trailing */", false, []string{"SELECT 1"}, nil},
		{';', "/* leading ; */ SELECT 1; /* trailing */", true, []string{"/* leading ; */SELECT 1"}, nil},
		{';', "-- leading\r\nSELECT 1;", true, []string{"-- leading\r\nSELECT 1"}, nil},
		{';', "/* comments only */ -- final", true, nil, nil},
		{';', "DO BEGIN BEGIN SELECT 1; END; SELECT 2; END; SELECT 3;", false, []string{"DO BEGIN BEGIN SELECT 1; END; SELECT 2; END", "SELECT 3"}, nil},
		{';', "SELECT CASE WHEN 1 = 1 THEN CASE WHEN 2 = 2 THEN 3 END END FROM DUMMY; SELECT 4;", false, []string{"SELECT CASE WHEN 1 = 1 THEN CASE WHEN 2 = 2 THEN 3 END END FROM DUMMY", "SELECT 4"}, nil},
		{';', controlFlow + "; SELECT 7;", false, []string{strings.Replace(controlFlow, "/* suffix */", "", 1), "SELECT 7"}, nil},
		{';', caseAssignment + "; SELECT 8 FROM DUMMY;", false, []string{caseAssignment, "SELECT 8 FROM DUMMY"}, nil},
		{';', "create procedure p() language sqlscript as begin select 'END;' from dummy; end; select begin_value, end_value, case_value from t;", false, []string{"create procedure p() language sqlscript as begin select 'END;' from dummy; end", "select begin_value, end_value, case_value from t"}, nil},
		{'§', "DO BEGIN SELECT '§'; END§SELECT 2§", false, []string{"DO BEGIN SELECT '§'; END", "SELECT 2"}, nil},
		{';', "SELECT /* \xff */ 1;", false, nil, ErrInvalidUTF8},
		{';', "SELECT 1 -- \xff\nFROM DUMMY;", false, nil, ErrInvalidUTF8},
	}

	for _, d := range testData {
		for _, reader := range []io.Reader{strings.NewReader(d.script), iotest.OneByteReader(strings.NewReader(d.script))} {
			testScan(t, d.separator, d.comments, reader, d.result, d.wantErr)
		}
	}
}
