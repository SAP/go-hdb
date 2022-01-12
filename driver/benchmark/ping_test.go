//go:build !unit
// +build !unit

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package benchmark

import (
	"database/sql"
	"testing"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/drivertest"
)

func benchmarkPing(c *driver.Connector, b *testing.B) {
	db := sql.OpenDB(c)
	defer db.Close()
	if err := db.Ping(); err != nil {
		b.Fatal(err)
	}
}

func benchmarkPingSeq(c *driver.Connector, b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkPing(c, b)
	}
}

func benchmarkPingPar(c *driver.Connector, pb *testing.PB, b *testing.B) {
	for pb.Next() {
		benchmarkPing(c, b)
	}
}

func BenchmarkPing(b *testing.B) {
	connector, err := driver.NewDSNConnector(drivertest.DSN())
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Ping sequentially", func(b *testing.B) {
		benchmarkPingSeq(connector, b)
	})
	b.Run("Ping parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) { benchmarkPingPar(connector, pb, b) })
	})
}
