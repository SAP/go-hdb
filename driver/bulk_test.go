//go:build !unit
// +build !unit

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

// TestBulkFrame
func testBulkFrame(conn *sql.Conn, samples int, cmd string, insertFct func(stmt *sql.Stmt), t *testing.T) {
	// 1. prepare
	tmpTableName := driver.RandomIdentifier("#tmpTable")

	//keep connection / hdb session for using local temporary tables
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //cleanup

	if _, err := tx.Exec(fmt.Sprintf("create local temporary table %s (i integer)", tmpTableName)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("%s %s values (?)", cmd, tmpTableName))
	if err != nil {
		t.Fatalf("prepare bulk insert failed: %s", err)
	}
	defer stmt.Close()

	// 2. call insert function
	insertFct(stmt)

	// 3. check
	i := 0
	err = tx.QueryRow(fmt.Sprintf("select count(*) from %s", tmpTableName)).Scan(&i)
	if err != nil {
		t.Fatalf("select count failed: %s", err)
	}

	if i != samples {
		t.Fatalf("invalid number of records %d - %d expected", i, samples)
	}

	rows, err := tx.Query(fmt.Sprintf("select * from %s order by i", tmpTableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i = 0
	for rows.Next() {

		var j int

		if err := rows.Scan(&j); err != nil {
			t.Fatal(err)
		}

		if j != i {
			t.Fatalf("value %d - expected %d", j, i)
		}

		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

// TestBulkInsertDuplicates
func testBulkInsertDuplicates(conn *sql.Conn, t *testing.T) {
	ctx := context.Background()

	table := driver.RandomIdentifier("bulkInsertDuplicates")

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("create table %s (k integer primary key, v integer)", table)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := conn.PrepareContext(ctx, fmt.Sprintf("bulk insert into %s values (?,?)", table))
	if err != nil {
		t.Fatalf("prepare bulk insert failed: %s", err)
	}
	defer stmt.Close()

	for i := 1; i < 4; i++ {
		if _, err := stmt.Exec(i, i); err != nil {
			t.Fatalf("insert failed: %s", err)
		}
	}
	if _, err := stmt.Exec(); err != nil {
		t.Fatalf("final insert (flush) failed: %s", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := stmt.Exec(i, i); err != nil {
			t.Fatalf("insert failed: %s", err)
		}
	}
	_, err = stmt.Exec()
	if err == nil {
		t.Fatal("error duplicate key expected")
	}

	dbError, ok := err.(driver.Error)
	if !ok {
		t.Fatal("driver.Error expected")
	}

	// expect 3 errors for statement 1,2 and 3
	if dbError.NumError() != 3 {
		t.Fatalf("number of errors: %d - %d expected", dbError.NumError(), 3)
	}

	stmtNo := []int{1, 2, 3}

	for i := 0; i < dbError.NumError(); i++ {
		dbError.SetIdx(i)
		if dbError.StmtNo() != stmtNo[i] {
			t.Fatalf("statement number: %d - %d expected", dbError.StmtNo(), stmtNo[i])
		}
	}
}

func testBulk(conn *sql.Conn, t *testing.T) {
	const samples = 1000

	tests := []struct {
		name      string
		cmd       string
		insertFct func(stmt *sql.Stmt)
	}{
		{
			"bulkInsertViaCommand",
			"bulk insert into",
			func(stmt *sql.Stmt) {
				for i := 0; i < samples; i++ {
					if _, err := stmt.Exec(i); err != nil {
						t.Fatalf("insert failed: %s", err)
					}
				}
				// final flush
				if _, err := stmt.Exec(); err != nil {
					t.Fatalf("final insert (flush) failed: %s", err)
				}
			},
		},
		{
			"bulkInsertViaParameter",
			"insert into",
			func(stmt *sql.Stmt) {
				prm := driver.NoFlush
				for i := 0; i < samples; i++ {
					if i == (samples - 1) {
						prm = driver.Flush
					}
					if _, err := stmt.Exec(i, prm); err != nil {
						t.Fatalf("insert failed: %s", err)
					}
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testBulkFrame(conn, samples, test.cmd, test.insertFct, t)
		})
	}
}

// TestBulkBlob
func testBulkBlob(conn *sql.Conn, t *testing.T) {

	samples := 100
	lobData := func(i int) string {
		return fmt.Sprintf("%s-%d", "Go rocks", i)
	}

	// 1. prepare
	tmpTableName := driver.RandomIdentifier("#tmpTable")

	//keep connection / hdb session for using local temporary tables
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback() //cleanup

	if _, err := tx.Exec(fmt.Sprintf("create local temporary table %s (i integer, b blob)", tmpTableName)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := tx.Prepare(fmt.Sprintf("bulk insert into %s values (?, ?)", tmpTableName))
	if err != nil {
		t.Fatalf("prepare bulk insert failed: %s", err)
	}
	defer stmt.Close()

	// 2. call insert function
	for i := 0; i < samples; i++ {
		lob := new(driver.Lob).SetReader(strings.NewReader(lobData(i)))

		if _, err := stmt.Exec(i, lob); err != nil {
			t.Fatalf("insert failed: %s", err)
		}
	}
	// final flush
	if _, err := stmt.Exec(); err != nil {
		t.Fatalf("final insert (flush) failed: %s", err)
	}

	// 3. check
	i := 0
	err = tx.QueryRow(fmt.Sprintf("select count(*) from %s", tmpTableName)).Scan(&i)
	if err != nil {
		t.Fatalf("select count failed: %s", err)
	}

	if i != samples {
		t.Fatalf("invalid number of records %d - %d expected", i, samples)
	}

	rows, err := tx.Query(fmt.Sprintf("select * from %s order by i", tmpTableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i = 0
	for rows.Next() {

		var j int
		builder := new(strings.Builder)

		lob := new(driver.Lob).SetWriter(builder)

		if err := rows.Scan(&j, lob); err != nil {
			t.Fatal(err)
		}

		if j != i {
			t.Fatalf("value %d - expected %d", j, i)
		}
		if builder.String() != lobData(i) {
			t.Fatalf("value %s - expected %s", builder.String(), lobData(i))
		}

		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func testBulkGeo(conn *sql.Conn, t *testing.T) {
	const (
		ewkb = "0106000020110F0000020000000103000000010000005700000037FE4552661A72C1F2951D71F0C05D419BCFC15E0D1A72C1E5DD15A2EFC35D419A062960831972C1B9A0E93765C75D4169070572FA1872C1CC185C4CB7C95D419DAB1CA0B31872C1DBF61D340CCA5D410E7E914EA51872C1255D2F7CABCA5D41293E49A7691872C1C245A0E82CCC5D413DEB833F1A1872C137AE8F484ACD5D41D13AE4EEB01772C150A7CC73D1CD5D410A5E83C72E1772C1A1447CFFF9CD5D41B7DE591BF01672C162A3364CE8CD5D410512F6AAB71672C1ABF1C133D3CE5D414B2EC384161672C13533535213D05D4167A16190481572C1B3E1F651CED05D416BF5CB51E71472C1D85F4A1EDCD05D41F01526ED181472C10586140273D05D417C04A0564E1372C10BA5C78526CF5D419A51D2CC141372C12684FFB09ECF5D41DBBF901DBD1272C12B12717EA3D05D416929408C861272C1533B119A18D15D4126DA849C401272C13EC78C405ED15D417490270E391272C1CDFA640612D25D411E6756C0141272C1CCCFD8C008D35D4170FC403ED11172C1E2E9A2FE7DD45D412F6837A2621172C1BF7145C3D4D55D413AC3E60F831072C1D41979733ED75D41E5A6A03CE40F72C13BCC9F1044D75D41E4C9FE4C2B0F72C1E79890A96CD75D4195088FED900E72C150E26D48F3D65D4166DF85BE870E72C10051C1C6E3D65D41D5D14D76F80D72C1D6A29B61F2D55D41FE92FBF2AA0D72C14981BAC8A6D45D410FE1E35D470D72C1601C20F21ED35D41E0DCEA53020D72C1120B6678A4D15D41D322EC76A40C72C1CD0C8875A4CF5D410ECB3490870C72C17FCC15CB21CD5D416E9B7536B20C72C10AD8F693A4C95D416020A3BF000D72C1FA73019574C65D41E173EEEE750D72C1706E53B5E1C35D4170A944E1ED0D72C1F9E1318DCAC15D4177EB3FC6630E72C194DF43E4B7C05D41BC9B147A550E72C12AC85BD99EBF5D4184B3280C4B0E72C1E715D37020BE5D415D240D75520E72C13D86716424BC5D419B6508C0C90E72C15758B5A6F6B85D416B70554F3B0F72C13BE8BB1194B55D41733CC04AEB0F72C1DF43844EA8B25D4122769011351072C1DAFB5E5E86B25D41E6EEE1EF5D1072C176BF1C4CAEB15D41D5CE2175AF1072C15848870FEBB05D4185C9BF99191172C1A327D4262FAF5D4170291986A21172C1D5F9199E45AC5D41CD4523D1421272C176E6F1968BA95D4192CDAB023B1372C1375A36C5A6A65D41293552163B1472C1D30A5BF375A55D4157CA4D2F7E1472C1A7B5D63893A55D4139BF58625F1472C1BD2CDAE3F6A35D41555A00965F1472C13BC45C7494A25D41A12E413E831472C13AFEBB6A22A05D4147B5635C011572C1D60AB510D49D5D41F2E4C573D11572C1C955E95EB49A5D41D7553E54EC1672C1EB8E511F88995D417A1894D50C1772C1C3469BA265995D4181576CB3431872C108339E2178995D41E1B6EB4F311972C16BB1AA59AD9B5D41DAD357DA6F1972C1EEB8AA6ABD9C5D4157A358B7831972C1653BAC897E9D5D418BF0DC20A61972C14663ED720C9F5D414B068122D61972C193E131E6DDA05D41FD391A61031A72C1CEDB944A82A35D41018FE68E021A72C14EA73284B3A65D415DA639FA901972C1A0A81BF542AA5D41DFF4CD49F41872C1F973F6D4C7AC5D414D8DFD71391872C17D6D3147AEAE5D419DC7FFB7AD1772C125486CF7F3AE5D4149EAD386771772C14D4192E52CAF5D41EB3558BC9C1772C17D9F13A660AF5D4170DB6194EC1772C1BB7DE2D1B5AF5D418635584F601872C1074C82ADEDAF5D41C19CC52BFC1872C149FA04EEF5B05D41435BA456A71972C1274EA442E5B35D4195B62B120D1A72C1425266B300B75D41D2B4A2C54F1A72C1BF7095FDAEBA5D416F5E5F7C131A72C1A369B8011BBD5D41AC7903B4391A72C12556661B73BD5D41A6621CFE591A72C17B7EDF30B3BE5D4137FE4552661A72C1F2951D71F0C05D410103000000010000002500000092B732F1900D72C15D66A1B3A2BF5D41CB3BE551870D72C170B7E61867C15D417C0A1059570D72C1FBAAA61E48C35D41C8B2DDE3E00C72C11A1A37F4A5C55D4155182F25690C72C1267D04FB58C75D413C01D87FFD0B72C132EA59504AC85D4164F72E211E0B72C1861A4D2AC8C95D4196090F16900A72C1615EC46316CA5D4106597DE2500A72C178EF691AFAC95D41B17CB09F1E0A72C1FB46C748FBC95D4189364DAA1A0A72C13B4F4363FBC95D41E90B51A8BD0972C1EF8DD05D8EC95D411F7C1596580972C148E327F8CEC85D4180FA8251080972C1F6605075F8C75D41CC1C30ADB10872C1EA24B18997C65D412DB61ED4800872C1F4FA5750BCC45D411CBD1A975A0872C188E6F169B8C25D419E01202D190872C13ADA1ED043C05D41696E00ED0D0872C176A28E123DBF5D4129D1B73F170872C1215233B57CBD5D41F632B99D4A0872C13C3039A435BC5D41871BEA7AA50872C18B190794D8B95D4190B3107C1E0972C1781210CD53B75D419B1CA158BE0972C14190ABBDCCB55D41753CFC42340A72C1F3089D44DAB45D415A42E7634C0A72C1436FE8BAA8B45D41EF8DC89BFC0A72C1A054D01BE1B35D414A3777C8590B72C177513EA6D4B35D41F0F852DDA20B72C1D748201406B45D4104B2C589E50B72C1A406952551B45D413BD103774B0C72C17F7389863EB55D41478E10D9CD0C72C180DB91B51AB75D41D8E7FC83320D72C166F39D36A3B95D4111A152CD630D72C1187C753C42BB5D41D528F080760D72C1906B44BEB1BD5D41CBB4747A8A0D72C1C5EE9D7AEFBE5D4192B732F1900D72C15D66A1B3A2BF5D41"
		cnt  = 10
	)

	tableName := driver.RandomIdentifier("bulkGeo")
	ctx := context.Background()

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("create table %s (id int, geo st_geometry(3857))", tableName)); err != nil {
		t.Fatalf("create table failed: %s", err)
	}

	stmt, err := conn.PrepareContext(ctx, fmt.Sprintf("bulk insert into %s values (?, st_geomfromewkb(?))", tableName)) // Prepare bulk query.
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	for i := 0; i < cnt; i++ {
		if _, err := stmt.Exec(i, new(driver.Lob).SetReader(strings.NewReader(ewkb))); err != nil {
			t.Fatal(err)
		}
	}
	// Call final stmt.Exec().
	if _, err := stmt.Exec(); err != nil {
		t.Fatal(err)
	}

	/*
		see https://github.com/SAP/go-hdb/issues/71
		- read rows to double check that geometry field attributes can be read
		- protocol return type is tcStGeometry (not tcLocator)
	*/
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("select * from %s order by id", tableName))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var j int
		b := []byte{}

		if err := rows.Scan(&j, &b); err != nil {
			t.Fatal(err)
		}
		if j != i {
			t.Fatalf("value %d - expected %d", j, i)
		}
		//t.Logf("value: %[1]v%[1]s", b)
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestBulk(t *testing.T) {
	tests := []struct {
		name string
		fct  func(conn *sql.Conn, t *testing.T)
	}{
		{"testBulk", testBulk},
		{"testBulkInsertDuplicates", testBulkInsertDuplicates},
		{"testBulkBlob", testBulkBlob},
		{"testBulkGeo", testBulkGeo},
	}

	db := driver.DefaultTestDB()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.fct(conn, t) // run bulk tests on conn
		})
	}
}
