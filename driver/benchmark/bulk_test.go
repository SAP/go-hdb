// +build !unit

// SPDX-FileCopyrightText: 2019-2020 Stefan Miller
//
// SPDX-License-Identifier: Apache-2.0

package benchmark

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

type bulkTabler interface {
	createTable(db *sql.DB, b *testing.B)
	bulkInsert(conn *sql.Conn, samples int, b *testing.B)
}

type bulkTable1 struct {
	tableName driver.Identifier
}

func newBulkTable1() *bulkTable1 {
	return &bulkTable1{tableName: driver.RandomIdentifier("bulkTable1")}
}

func (t *bulkTable1) createTable(db *sql.DB, b *testing.B) {
	if _, err := db.Exec(fmt.Sprintf("create table %s (i integer)", t.tableName)); err != nil {
		b.Fatalf("create table failed: %s", err)
	}
}

func (t *bulkTable1) bulkInsert(conn *sql.Conn, samples int, b *testing.B) {
	ctx := context.Background()

	stmt, err := conn.PrepareContext(ctx, fmt.Sprintf("bulk insert into %s values (?)", t.tableName))
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	b.ResetTimer()

	for i := 0; i < samples; i++ {
		if _, err := stmt.ExecContext(ctx, i); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := stmt.ExecContext(ctx); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkBulk(b *testing.B) {
	tests := []struct {
		name  string
		table bulkTabler
	}{
		{"table1", newBulkTable1()},
	}

	const samples = 1000000

	bulkSizes := []int{1000, 10000, 100000, 1000000}

	connector := NewTestConnector()
	db := sql.OpenDB(connector)
	defer db.Close()

	// create tables
	for _, test := range tests {
		test.table.createTable(db, b)
	}

	// execute sequential tests
	for _, test := range tests {
		for _, s := range bulkSizes {
			connector.SetBulkSize(s)
			conn, err := db.Conn(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			name := fmt.Sprintf("Bulk sequentially %s batchSize %d", test.name, s)
			b.Run(name, func(b *testing.B) {
				test.table.bulkInsert(conn, samples, b)
			})
		}
	}
}
