//go:build !unit && go1.22

package driver

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/big"
	"slices"
	"testing"
)

// TestNull tests go1.22 using generic Null type with go-hdb types.
func TestNullDataType(t *testing.T) { //nolint:gocyclo
	t.Parallel()

	type nullRow struct {
		No         int                 `sql:"no"` // record number
		Int        sql.Null[int]       `sql:"int"`
		Bytes      sql.Null[[]byte]    `sql:"bytes"`
		Decimal    sql.Null[Decimal]   `sql:"decimal"`
		Lob        sql.Null[Lob]       `sql:"lob"`
		IntRef     *sql.Null[*int]     `sql:"intref"`
		BytesRef   *sql.Null[*[]byte]  `sql:"byteref"`
		DecimalRef *sql.Null[*Decimal] `sql:"decimalref"`
		LobRef     *sql.Null[*Lob]     `sql:"lobref"`
	}

	lobTestValue := func() *bytes.Reader {
		return bytes.NewReader([]byte("hello from lob"))
	}

	cmpInt := func(no int, in sql.Null[int], out sql.Null[int]) error {
		if in.Valid != out.Valid || (in.Valid && in.V != out.V) {
			return fmt.Errorf("no %d int: got valid %t value %d - expected valid %t value %d", no, out.Valid, out.V, in.Valid, in.V)
		}
		return nil
	}

	cmpIntRef := func(no int, in *sql.Null[*int], out *sql.Null[*int]) error {
		if in == nil && out == nil {
			return nil
		}
		if in.Valid != out.Valid || (in.Valid && *in.V != *out.V) {
			return fmt.Errorf("no %d int: got valid %t value %d - expected valid %t value %d", no, out.Valid, *out.V, in.Valid, *in.V)
		}
		return nil
	}

	cmpBytes := func(no int, in sql.Null[[]byte], out sql.Null[[]byte]) error {
		if in.Valid != out.Valid || (in.Valid && slices.Compare(in.V, out.V) != 0) {
			return fmt.Errorf("no %d int: got valid %t value %s - expected valid %t value %s", no, out.Valid, out.V, in.Valid, in.V)
		}
		return nil
	}

	cmpBytesRef := func(no int, in *sql.Null[*[]byte], out *sql.Null[*[]byte]) error {
		if in == nil && out == nil {
			return nil
		}
		if in.Valid != out.Valid || (in.Valid && slices.Compare(*in.V, *out.V) != 0) {
			return fmt.Errorf("no %d int: got valid %t value %s - expected valid %t value %s", no, out.Valid, *out.V, in.Valid, *in.V)
		}
		return nil
	}

	cmpDecimal := func(no int, in sql.Null[Decimal], out sql.Null[Decimal]) error {
		if in.Valid != out.Valid || (in.Valid && (*big.Rat)(&in.V).Cmp((*big.Rat)(&out.V)) != 0) {
			return fmt.Errorf("no %d int: got valid %t value %v - expected valid %t value %v", no, out.Valid, out.V, in.Valid, in.V)
		}
		return nil
	}

	cmpDecimalRef := func(no int, in *sql.Null[*Decimal], out *sql.Null[*Decimal]) error {
		if in == nil && out == nil {
			return nil
		}
		if in.Valid != out.Valid || (in.Valid && (*big.Rat)(in.V).Cmp((*big.Rat)(out.V)) != 0) {
			return fmt.Errorf("no %d int: got valid %t value %v - expected valid %t value %v", no, out.Valid, *out.V, in.Valid, *in.V)
		}
		return nil
	}

	cmpLob := func(no int, in sql.Null[Lob], out sql.Null[Lob]) error {
		if in.Valid != out.Valid {
			return fmt.Errorf("no %d int: got valid %t value %v - expected valid %t value %v", no, out.Valid, out.V, in.Valid, in.V)
		}
		if !in.Valid {
			return nil
		}
		ok, err := compareLob(in.V, out.V)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("no %d int: got valid %t value %v - expected valid %t value %v", no, out.Valid, out.V, in.Valid, in.V)
		}
		return nil
	}

	cmpLobRef := func(no int, in *sql.Null[*Lob], out *sql.Null[*Lob]) error {
		if in == nil && out == nil {
			return nil
		}
		if in.Valid != out.Valid {
			return fmt.Errorf("no %d int: got valid %t value %v - expected valid %t value %v", no, out.Valid, *out.V, in.Valid, *in.V)
		}
		if !in.Valid {
			return nil
		}
		ok, err := compareLob(*in.V, *out.V)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("no %d int: got valid %t value %v - expected valid %t value %v", no, out.Valid, *out.V, in.Valid, *in.V)
		}
		return nil
	}

	cmp := func(in, out *nullRow) error {
		if err := cmpInt(out.No, in.Int, out.Int); err != nil {
			return err
		}
		if err := cmpIntRef(out.No, in.IntRef, out.IntRef); err != nil {
			return err
		}
		if err := cmpBytes(out.No, in.Bytes, out.Bytes); err != nil {
			return err
		}
		if err := cmpBytesRef(out.No, in.BytesRef, out.BytesRef); err != nil {
			return err
		}
		if err := cmpDecimal(out.No, in.Decimal, out.Decimal); err != nil {
			return err
		}
		if err := cmpDecimalRef(out.No, in.DecimalRef, out.DecimalRef); err != nil {
			return err
		}
		if err := cmpLob(out.No, in.Lob, out.Lob); err != nil {
			return err
		}
		if err := cmpLobRef(out.No, in.LobRef, out.LobRef); err != nil {
			return err
		}
		return nil
	}

	db := MT.DB()

	var (
		intValue     = 42
		bytesValue   = []byte("hello go-hdb")
		decimalValue = Decimal(*big.NewRat(1, 2))
	)

	testRows := []*nullRow{
		{}, // try with initial Null[T]
		{
			Int:     sql.Null[int]{V: intValue, Valid: true},
			Bytes:   sql.Null[[]byte]{V: bytesValue, Valid: true},
			Decimal: sql.Null[Decimal]{V: decimalValue, Valid: true},
			Lob:     sql.Null[Lob]{V: *NewLob(lobTestValue(), nil), Valid: true},

			IntRef:     &sql.Null[*int]{V: &intValue, Valid: true},
			BytesRef:   &sql.Null[*[]byte]{V: &bytesValue, Valid: true},
			DecimalRef: &sql.Null[*Decimal]{V: &decimalValue, Valid: true},
			LobRef:     &sql.Null[*Lob]{V: NewLob(lobTestValue(), nil), Valid: true},
		},
	}

	scanner, err := NewStructScanner[nullRow]()
	if err != nil {
		t.Fatal(err)
	}

	tableName := RandomIdentifier("null_")
	columnDefs, err := scanner.columnDefs()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("create table %s %s", tableName, columnDefs)); err != nil {
		t.Fatal(err)
	}

	// insert test rows
	stmt, err := db.Prepare(fmt.Sprintf("insert into %s values %s", tableName, scanner.queryPlaceholders()))
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for i, row := range testRows {
		_, err := stmt.Exec(i, row.Int, row.Bytes, row.Decimal, row.Lob, row.IntRef, row.BytesRef, row.DecimalRef, row.LobRef)
		if err != nil {
			t.Fatal(err)
		}
	}

	row := new(nullRow)

	rows, err := db.Query(fmt.Sprintf("select * from %s", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := scanner.Scan(rows, row); err != nil {
			t.Fatal(err)
		}
		if err := cmp(testRows[row.No], row); err != nil {
			t.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
