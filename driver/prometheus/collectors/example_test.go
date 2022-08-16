//go:build !unit
// +build !unit

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package collectors_test

import (
	"database/sql"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/SAP/go-hdb/driver"
	drivercollectors "github.com/SAP/go-hdb/driver/prometheus/collectors"
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
	db := sql.OpenDB(connector)
	defer db.Close()

	// dbName: use as label.
	// as alternative connector.Host() could be used.
	const dbName = "myDatabase"

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
