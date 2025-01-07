//go:build !unit

package driver_test

import (
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func benchmarkPing(b *testing.B) {
	db := driver.MT.DB()
	if err := db.Ping(); err != nil {
		b.Fatal(err)
	}
}

func benchmarkPingSeq(b *testing.B) {
	for range b.N {
		benchmarkPing(b)
	}
}

func benchmarkPingPar(b *testing.B, pb *testing.PB) {
	for pb.Next() {
		benchmarkPing(b)
	}
}

func BenchmarkPing(b *testing.B) {
	b.Run("Ping sequentially", func(b *testing.B) {
		benchmarkPingSeq(b)
	})
	b.Run("Ping parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) { benchmarkPingPar(b, pb) })
	})
}
