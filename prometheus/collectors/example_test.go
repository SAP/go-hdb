//go:build !unit

package collectors_test

import (
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/SAP/go-hdb/driver"
	drivercollectors "github.com/SAP/go-hdb/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func formatHTTPAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "80"
	}
	return net.JoinHostPort(host, port)
}

// Example demonstrates the usage of go-hdb prometheus metrics.
func Example() {
	const (
		envDSN  = "GOHDBDSN"
		envHTTP = "GOHDBHTTP"
	)

	dsn := os.Getenv(envDSN)
	addr := os.Getenv(envHTTP)

	// exit if dsn or http address is missing.
	if dsn == "" || addr == "" {
		return
	}

	connector, err := driver.NewDSNConnector(dsn)
	if err != nil {
		log.Fatal(err)
	}
	// use driver.OpenDB instead of sql.OpenDB to collect driver.DB specific statistics.
	db := driver.OpenDB(connector)
	defer db.Close()

	// use dbName as label.
	const dbName = "myDatabase"

	// register collector for sql.DB stats.
	sqlDBStatsCollector := collectors.NewDBStatsCollector(db.DB, dbName)
	if err := prometheus.Register(sqlDBStatsCollector); err != nil {
		log.Fatal(err)
	}

	// register collector for go-hdb driver stats.
	driverCollector := drivercollectors.NewDriverStatsCollector(connector.NativeDriver(), dbName)
	if err := prometheus.Register(driverCollector); err != nil {
		log.Fatal(err)
	}

	// register collector for extended go-hdb db stats.
	driverDBExStatsCollector := drivercollectors.NewDBExStatsCollector(db, dbName)
	if err := prometheus.Register(driverDBExStatsCollector); err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	done := make(chan struct{})

	// do some database stuff...
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				if err := db.Ping(); err != nil {
					log.Fatal(err)
				}
			}
		}
	}()

	// register prometheus HTTP handler and start HTTP server.
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(addr, nil)

	log.Printf("access the metrics at http://%s/metrics", formatHTTPAddr(addr))

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt)
	<-sigint

	close(done)
	wg.Wait()

	// output:
}
