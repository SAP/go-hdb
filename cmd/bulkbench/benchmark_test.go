package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"
)

func Benchmark(b *testing.B) {

	checkErr := func(err error) {
		if err != nil {
			b.Fatal(err)
		}
	}

	// Create handler.
	dbHandler, err := newDBHandler(b.Logf)
	checkErr(err)
	testHandler, err := newTestHandler(b.Logf)
	checkErr(err)

	// Register handlers.
	mux := http.NewServeMux()
	mux.Handle("/test/", testHandler)
	mux.Handle("/db/", dbHandler)

	// Start http test server.
	ts := httptest.NewServer(mux)
	client := ts.Client()

	execTest := func(test string, batchCount, batchSize int) (*testResult, error) {
		r, err := client.Get(fmt.Sprintf("%s%s?batchcount=%d&batchsize=%d", ts.URL, test, batchCount, batchSize))
		if err != nil {
			return nil, err
		}
		defer r.Body.Close()
		return newTestResult(r)
	}

	execDropSchema := func() (*testResult, error) {
		r, err := client.Get(fmt.Sprintf("%s/db/dropSchema", ts.URL))
		if err != nil {
			return nil, err
		}
		defer r.Body.Close()
		return newTestResult(r)
	}

	const maxDuration time.Duration = 1<<63 - 1

	f := func(test string, batchCount, batchSize int, b *testing.B) {
		ds := make([]time.Duration, b.N)
		var avg, max time.Duration
		min := maxDuration

		for i := 0; i < b.N; i++ {
			r, err := execTest(test, batchCount, batchSize)
			if err != nil {
				b.Fatal(err)
			}
			if r.Error != "" {
				b.Fatal(r.Error)
			}

			avg += r.Duration
			if r.Duration < min {
				min = r.Duration
			}
			if r.Duration > max {
				max = r.Duration
			}
			ds[i] = r.Duration
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

	format := `
GOMAXPROCS: %d
NumCPU: %d
Driver Version: %s
HANA Version: %s
`
	log.Printf(format, runtime.GOMAXPROCS(0), runtime.NumCPU(), dbHandler.driverVersion(), dbHandler.hdbVersion())

	b.Cleanup(func() {
		// close test server
		defer ts.Close()

		r, err := execDropSchema()
		if err != nil {
			b.Fatal(err)
		}
		if r.Error != "" {
			b.Fatal(r.Error)
		}
	})

	// Start benchmarks.
	names := []string{"seq", "par"}
	tests := []string{testSeq, testPar}

	for i, test := range tests {
		for _, prm := range parameters.prms {
			// Use batchCount and batchCount flags.
			b.Run(fmt.Sprintf("%s-%dx%d", names[i], prm.BatchCount, prm.BatchSize), func(b *testing.B) {
				f(test, prm.BatchCount, prm.BatchSize, b)
			})
		}
	}
}
