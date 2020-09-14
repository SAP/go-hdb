// SPDX-FileCopyrightText: 2019-2020 Stefan Miller
//
// SPDX-License-Identifier: Apache-2.0

package benchmark

import (
	"database/sql"
	"os"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

const envDSN = "GOHDBDSN"

func benchmarkPing(c *driver.Connector, b *testing.B) {
	for i := 0; i < b.N; i++ {
		db := sql.OpenDB(c)
		defer db.Close()
		if err := db.Ping(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPing(b *testing.B) {
	c, err := driver.NewDSNConnector(os.Getenv(envDSN))
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Ping", func(b *testing.B) {
		benchmarkPing(c, b)
	})
}
