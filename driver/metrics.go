// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"sync"
	"sync/atomic"
)

// Constants for time statistics.
const (
	timeRead = iota
	timeWrite
	timeAuth
	timeQuery
	timePrepare
	timeExec
	timeCall
	timeFetch
	timeFetchLob
	timeRollback
	timeCommit
	numTime
)

const (
	counterBytesRead = iota
	counterBytesWritten
	numCounter
)

const (
	gaugeConn = iota
	gaugeTx
	gaugeStmt
	numGauge
)

type counter struct {
	n uint64 // atomic access.
}

func (c *counter) add(n uint64)  { atomic.AddUint64(&c.n, uint64(n)) }
func (c *counter) value() uint64 { return atomic.LoadUint64(&c.n) }

type gauge struct {
	v int64 // atomic access.
}

func (g *gauge) add(n int64)  { atomic.AddInt64(&g.v, int64(n)) }
func (g *gauge) value() int64 { return atomic.LoadInt64(&g.v) }

type timeHistogram struct {
	mu          sync.Mutex
	count       uint64
	sum         uint64
	timeBuckets []uint64
	buckets     []uint64
	underflow   uint64 // in case of negative duration (will add to zero bucket)
}

func newTimeHistogram(timeBuckets []uint64) *timeHistogram {
	return &timeHistogram{timeBuckets: timeBuckets, buckets: make([]uint64, len(timeBuckets))}
}

func (h *timeHistogram) stats() *TimeStat {
	h.mu.Lock()
	rv := &TimeStat{
		Count:   h.count,
		Sum:     h.sum / 1e6, // time in milliseconds
		Buckets: make(map[uint64]uint64, len(h.buckets)),
	}
	for i, timeBucket := range h.timeBuckets {
		rv.Buckets[timeBucket] = h.buckets[i]
	}
	h.mu.Unlock()
	return rv
}

func (h *timeHistogram) add(ns int64) { // time in nanoseconds
	h.mu.Lock()
	h.count++
	if ns < 0 {
		h.underflow++
		h.mu.Unlock()
		return
	}
	h.sum += uint64(ns)
	// determine index
	i := binarySearchSliceUint64(h.timeBuckets, uint64(ns)/1e6) // bucket in milliseconds
	if i < len(h.timeBuckets) {
		h.buckets[i]++
	}
	h.mu.Unlock()
}

type metrics struct {
	parent         *metrics
	counters       []*counter
	gauges         []*gauge
	timeHistograms []*timeHistogram
}

func newMetrics(parent *metrics, timeBuckets []uint64) *metrics {
	rv := &metrics{
		parent:         parent,
		counters:       make([]*counter, numCounter),
		gauges:         make([]*gauge, numGauge),
		timeHistograms: make([]*timeHistogram, numTime),
	}
	for i := 0; i < numCounter; i++ {
		rv.counters[i] = &counter{}
	}
	for i := 0; i < numGauge; i++ {
		rv.gauges[i] = &gauge{}
	}
	for i := 0; i < int(numTime); i++ {
		rv.timeHistograms[i] = newTimeHistogram(timeBuckets)
	}
	return rv
}

func (m *metrics) addCounterValue(kind int, v uint64) {
	m.counters[kind].add(v)
	if m.parent != nil {
		m.parent.addCounterValue(kind, v)
	}
}

func (m *metrics) addGaugeValue(kind int, v int64) {
	m.gauges[kind].add(v)
	if m.parent != nil {
		m.parent.addGaugeValue(kind, v)
	}
}

func (m *metrics) addTimeValue(kind int, v int64) {
	m.timeHistograms[kind].add(v)
	if m.parent != nil {
		m.parent.addTimeValue(kind, v)
	}
}

func (m *metrics) stats() Stats {
	timeStats := make([]*TimeStat, numTime)
	for i, th := range m.timeHistograms {
		timeStats[i] = th.stats()
	}
	return Stats{
		OpenConnections:  int(m.gauges[gaugeConn].value()),
		OpenTransactions: int(m.gauges[gaugeTx].value()),
		OpenStatements:   int(m.gauges[gaugeStmt].value()),
		BytesRead:        m.counters[counterBytesRead].value(),
		BytesWritten:     m.counters[counterBytesWritten].value(),
		TimeStats:        timeStats,
	}
}
