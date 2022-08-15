//go:build !unit
// +build !unit

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package prometheus_test

import (
	"database/sql"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/SAP/go-hdb/driver"
	drivercollectors "github.com/SAP/go-hdb/driver/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func completeAddr(addr string) string {
	const (
		defaultHost = "localhost"
		defaultPort = "50000"
	)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Fatal(err)
	}
	if host == "" {
		host = defaultHost
	}
	if port == "" {
		port = defaultPort
	}
	return net.JoinHostPort(host, port)
}

// Example demonstrates the usage of go-hdb prometheus metrics.
func Example() {
	const (
		envDSN  = "GOHDBDSN"
		envAddr = "GOHDBADDR"
	)

	dsn, ok := os.LookupEnv(envDSN)
	if !ok {
		return
	}
	addr, ok := os.LookupEnv(envAddr)
	if ok {
		addr = completeAddr(addr)
	}

	connector, err := driver.NewDSNConnector(dsn)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	// dbName: use as label.
	// as alternative connector.Host() could be used.
	dbName := "myDatabase"

	// register collector for sql db stats.
	dbStatsCollector := collectors.NewDBStatsCollector(db, dbName)
	if err := prometheus.Register(dbStatsCollector); err != nil {
		log.Fatal(err)
	}

	// register collector for go-hdb driver metrics.
	driverCollector := drivercollectors.NewDriverCollector(connector.NativeDriver(), dbName)
	if err := prometheus.Register(driverCollector); err != nil {
		log.Fatal(err)
	}

	// register collector for go-hdb connector metrics.
	connectorCollector := drivercollectors.NewConnectorCollector(connector, dbName)
	if err := prometheus.Register(connectorCollector); err != nil {
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

	if addr == "" {
		// if no HTTP server address then store metrics in "metrics.prom" file.

		const filename = "metrics.prom"

		time.Sleep(100 * time.Millisecond) // wait for some pings

		if err := prometheus.WriteToTextfile(filename, prometheus.DefaultRegisterer.(*prometheus.Registry)); err != nil {
			log.Fatal(err)
		}

	} else {
		// else register prometheus HTTP handler and start HTTP server.

		http.Handle("/metrics", promhttp.Handler())
		go http.ListenAndServe(addr, nil)

		log.Printf("access the metrics at http://%s/metrics", addr)

		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		<-sigint
	}

	close(done)
	wg.Wait()

	// output:
}
