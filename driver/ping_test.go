//go:build !unit
// +build !unit

package driver_test

import (
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func benchmarkPing(b *testing.B) {
	db := driver.DefaultTestDB()
	if err := db.Ping(); err != nil {
		b.Fatal(err)
	}
}

func benchmarkPingSeq(b *testing.B) {
	for i := 0; i < b.N; i++ {
		benchmarkPing(b)
	}
}

func benchmarkPingPar(pb *testing.PB, b *testing.B) {
	for pb.Next() {
		benchmarkPing(b)
	}
}

func BenchmarkPing(b *testing.B) {
	b.Run("Ping sequentially", func(b *testing.B) {
		benchmarkPingSeq(b)
	})
	b.Run("Ping parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) { benchmarkPingPar(pb, b) })
	})
}
