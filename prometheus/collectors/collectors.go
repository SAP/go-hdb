// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Package collectors provides prometheus collectors for driver and extended database statistics.
package collectors

import (
	"fmt"
	"strings"

	"github.com/SAP/go-hdb/driver"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "go_hdb"

type collector struct {
	fn func() *driver.Stats

	openConnections  *prometheus.Desc
	openTransactions *prometheus.Desc
	openStatements   *prometheus.Desc
	readBytes        *prometheus.Desc
	writtenBytes     *prometheus.Desc
	readTime         *prometheus.Desc
	writeTime        *prometheus.Desc
	authTime         *prometheus.Desc
	sqlTimes         *prometheus.Desc
}

func newCollector(fn func() *driver.Stats, subsystem string, labels prometheus.Labels) prometheus.Collector {
	// fqName: namespace, subsystem, name
	fqName := func(name string) string { return strings.Join([]string{namespace, subsystem, name}, "_") }
	return &collector{
		fn: fn,
		openConnections: prometheus.NewDesc(
			fqName("open_connections"),
			fmt.Sprintf("The number of established %s connections.", subsystem),
			nil,
			labels,
		),
		openTransactions: prometheus.NewDesc(
			fqName("open_transactions"),
			fmt.Sprintf("The number of open %s transactions.", subsystem),
			nil,
			labels,
		),
		openStatements: prometheus.NewDesc(
			fqName("open_statements"),
			fmt.Sprintf("The number of open %s statements.", subsystem),
			nil,
			labels,
		),
		readBytes: prometheus.NewDesc(
			fqName("bytes_read"),
			fmt.Sprintf("The total bytes read from the database connection of %s statements.", subsystem),
			nil,
			labels,
		),
		writtenBytes: prometheus.NewDesc(
			fqName("bytes_written"),
			fmt.Sprintf("The total bytes written to the database connection of %s statements.", subsystem),
			nil,
			labels,
		),
		readTime: prometheus.NewDesc(
			fqName("read_time"),
			fmt.Sprintf("The time spent measured in milliseconds for reading from the database connection of %s.", subsystem),
			nil,
			labels,
		),
		writeTime: prometheus.NewDesc(
			fqName("write_time"),
			fmt.Sprintf("The time spent measured in milliseconds for writing to the database connection of %s.", subsystem),
			nil,
			labels,
		),
		authTime: prometheus.NewDesc(
			fqName("auth_time"),
			fmt.Sprintf("The time spent measured in milliseconds for client authentication of %s.", subsystem),
			nil,
			labels,
		),
		sqlTimes: prometheus.NewDesc(
			fqName("sql_time"),
			fmt.Sprintf("The spent time measured in milliseconds for the different sql statements of %s.", subsystem),
			[]string{"sql"},
			labels,
		),
	}
}

// Describe implements Collector.
func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.openConnections
	ch <- c.openTransactions
	ch <- c.openStatements
	ch <- c.readBytes
	ch <- c.writtenBytes
	ch <- c.readTime
	ch <- c.writeTime
	ch <- c.authTime
	ch <- c.sqlTimes
}

// Collect implements Collector.
func (c *collector) Collect(ch chan<- prometheus.Metric) {
	stats := c.fn()
	ch <- prometheus.MustNewConstMetric(c.openConnections, prometheus.GaugeValue, float64(stats.OpenConnections))
	ch <- prometheus.MustNewConstMetric(c.openTransactions, prometheus.GaugeValue, float64(stats.OpenTransactions))
	ch <- prometheus.MustNewConstMetric(c.openStatements, prometheus.GaugeValue, float64(stats.OpenStatements))
	ch <- prometheus.MustNewConstMetric(c.readBytes, prometheus.CounterValue, float64(stats.ReadBytes))
	ch <- prometheus.MustNewConstMetric(c.writtenBytes, prometheus.CounterValue, float64(stats.WrittenBytes))
	ch <- prometheus.MustNewConstHistogram(c.readTime, stats.ReadTime.Count, stats.ReadTime.Sum, stats.ReadTime.Buckets)
	ch <- prometheus.MustNewConstHistogram(c.writeTime, stats.WriteTime.Count, stats.WriteTime.Sum, stats.WriteTime.Buckets)
	ch <- prometheus.MustNewConstHistogram(c.authTime, stats.AuthTime.Count, stats.AuthTime.Sum, stats.AuthTime.Buckets)
	for k, v := range stats.SQLTimes {
		ch <- prometheus.MustNewConstHistogram(c.sqlTimes, v.Count, v.Sum, v.Buckets, k)
	}
}

// NewDriverStatsCollector returns a collector that exports *driver.Driver statistics.
func NewDriverStatsCollector(d driver.Driver, dbName string) prometheus.Collector {
	return newCollector(d.Stats, "driver", prometheus.Labels{"db_name": dbName})
}

// NewDBExStatsCollector returns a collector that exports extended *driver.DB statistics.
func NewDBExStatsCollector(db *driver.DB, dbName string) prometheus.Collector {
	return newCollector(db.ExStats, "db", prometheus.Labels{"db_name": dbName})
}
