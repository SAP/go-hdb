//go:build !unit

package driver_test

import (
	"database/sql"
	"fmt"
	"log"
	"math/big"

	"github.com/SAP/go-hdb/driver"
)

// customDecimal is a minimal custom decimal type implementing the database/sql
// decimalDecompose and decimalCompose interfaces (unexported, defined in the Go
// standard library in database/sql/convert.go and database/sql/driver/types.go):
//
//	Decompose(buf []byte) (form byte, negative bool, coefficient []byte, exponent int32)
//	Compose(form byte, negative bool, coefficient []byte, exponent int32) error
//
// with value = (-1)^negative × coefficient × 10^exponent   (form 0 = finite).
//
// It demonstrates that go-hdb can write and scan decimal / fixed database
// attributes via any type implementing these interfaces - without using
// driver.Decimal.
type customDecimal struct {
	form     byte     // 0 finite, 1 infinite, 2 NaN
	negative bool     // sign
	coeff    *big.Int // coefficient magnitude (>= 0)
	exp      int32    // exponent
}

// String renders the decompose fields as coefficient and exponent.
func (d customDecimal) String() string {
	sign := ""
	if d.negative {
		sign = "-"
	}
	return fmt.Sprintf("%s%sE%d", sign, d.coeff, d.exp)
}

// Decompose implements the database/sql decimalDecompose interface (write side).
func (d customDecimal) Decompose(buf []byte) (form byte, negative bool, coefficient []byte, exponent int32) {
	if d.form != 0 {
		return d.form, d.negative, nil, 0
	}
	n := (d.coeff.BitLen() + 7) / 8
	if cap(buf) >= n {
		buf = buf[:n]
		coefficient = d.coeff.FillBytes(buf)
	} else {
		coefficient = d.coeff.Bytes()
	}
	return 0, d.negative, coefficient, d.exp
}

// Compose implements the database/sql decimalCompose interface (scan side).
func (d *customDecimal) Compose(form byte, negative bool, coefficient []byte, exponent int32) error {
	d.form = form
	d.negative = negative
	if d.coeff == nil {
		d.coeff = new(big.Int)
	}
	d.coeff.SetBytes(coefficient)
	d.exp = exponent
	return nil
}

/*
Example_customDecimal creates a table with a single decimal attribute, inserts a
record using a custom decimal type implementing the database/sql decimalDecompose
and decimalCompose interfaces, and selects the entry afterwards scanning into the
same custom type.
*/
func Example_customDecimal() {
	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	tableName := driver.RandomIdentifier("table_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (x decimal)", tableName)); err != nil {
		log.Fatal(err)
	}

	// value 1 = 1 × 10^0
	in := customDecimal{form: 0, negative: false, coeff: big.NewInt(1), exp: 0}

	if _, err := db.Exec(fmt.Sprintf("insert into %s values(?)", tableName), in); err != nil {
		log.Fatal(err)
	}

	var out customDecimal

	if err := db.QueryRow(fmt.Sprintf("select * from %s", tableName)).Scan(&out); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Decimal value: %s", out)

	// output: Decimal value: 1E0
}

/*
Example_customFixed is like Example_customDecimal but uses a fixed decimal
column (decimal(precision, scale)), exercising the fixed encode/decode path with a
custom decimal type implementing the database/sql decimalDecompose and
decimalCompose interfaces.
*/
func Example_customFixed() {
	db := sql.OpenDB(driver.MT.Connector())
	defer db.Close()

	tableName := driver.RandomIdentifier("table_")

	if _, err := db.Exec(fmt.Sprintf("create table %s (x decimal(18,2))", tableName)); err != nil {
		log.Fatal(err)
	}

	// value -12.34 = -1234 × 10^-2
	in := customDecimal{form: 0, negative: true, coeff: big.NewInt(1234), exp: -2}

	if _, err := db.Exec(fmt.Sprintf("insert into %s values(?)", tableName), in); err != nil {
		log.Fatal(err)
	}

	var out customDecimal

	if err := db.QueryRow(fmt.Sprintf("select * from %s", tableName)).Scan(&out); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Fixed value: %s", out)

	// output: Fixed value: -1234E-2
}
