package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/SAP/go-hdb/driver"
)

func Benchmark(b *testing.B) {

	dba, err := newDBA(dsn)
	if err != nil {
		b.Fatal(err)
	}
	ts := newTests(dba)

	const maxDuration time.Duration = 1<<63 - 1

	f := func(b *testing.B, sequential bool, batchCount, batchSize int) {
		ds := make([]time.Duration, b.N)
		var avg, max time.Duration //nolint: predeclared
		min := maxDuration         //nolint: predeclared

		for i := 0; i < b.N; i++ {
			tr := ts.execute(sequential, batchCount, batchSize, drop)
			if tr.Err != nil {
				b.Fatal(tr.Err)
			}

			avg += tr.Duration
			if tr.Duration < min {
				min = tr.Duration
			}
			if tr.Duration > max {
				max = tr.Duration
			}
			ds[i] = tr.Duration
		}

		// Median.
		var med time.Duration
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		l := len(ds)
		switch {
		case l == 0: // keep med == 0
		case l%2 != 0: // odd number
			med = ds[l/2] //  mid value
		default:
			med = (ds[l/2] + ds[l/2-1]) / 2 // even number - return avg of the two mid numbers
		}

		// Add metrics.
		b.ReportMetric((avg / time.Duration(b.N)).Seconds(), "avgsec/op")
		b.ReportMetric(min.Seconds(), "minsec/op")
		b.ReportMetric(max.Seconds(), "maxsec/op")
		b.ReportMetric(med.Seconds(), "medsec/op")
	}

	// Additional info.
	log.SetOutput(os.Stdout)
	log.Printf("Runtime Info - GOMAXPROCS: %d NumCPU: %d DriverVersion %s HDBVersion: %s",
		runtime.GOMAXPROCS(0),
		runtime.NumCPU(),
		driver.DriverVersion,
		dba.hdbVersion(),
	)

	b.Cleanup(func() {
		dba.close()
	})

	for _, prm := range parameters {
		// Use batchCount and batchCount flags.
		b.Run(fmt.Sprintf("sequential-%dx%d", prm.BatchCount, prm.BatchSize), func(b *testing.B) {
			f(b, true, prm.BatchCount, prm.BatchSize)
		})
	}
	for _, prm := range parameters {
		// Use batchCount and batchCount flags.
		b.Run(fmt.Sprintf("concurrent-%dx%d", prm.BatchCount, prm.BatchSize), func(b *testing.B) {
			f(b, false, prm.BatchCount, prm.BatchSize)
		})
	}
}
