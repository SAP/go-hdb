// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// StatsNumSQL is the number of SQL command categories.
const StatsNumSQL = 6

// StatsSQLTexts are the text used for the SQL command categories.
var StatsSQLTexts = [StatsNumSQL]string{"ping", "query", "prepare", "exec", "rollback", "commit"}

// StatsDurationBuckets are the used duration buckets in millisecongs.
var StatsDurationBuckets = []uint64{1, 10, 100, 1000, 10000, 100000}

// DurationStat represents a duration statistic.
type DurationStat struct {
	Count   uint64
	Sum     uint64            // Values in milliseconds.
	Buckets map[uint64]uint64 // map[<duration in ms>]<counter>.
}

func (s *DurationStat) String() string {
	return fmt.Sprintf("count %d sum %d values %v", s.Count, s.Sum, s.Buckets)
}

// Stats contains driver statistics.
type Stats struct {
	// Gauges
	OpenConnections  int // The number of established driver connections.
	OpenTransactions int // The number of open driver transactions.
	OpenStatements   int // The number of open driver database statements.
	// Counter
	BytesRead    uint64 // Total bytes read by client connection.
	BytesWritten uint64 // Total bytes written by client connection.
	//
	SQLDurations []*DurationStat // SQL execution duration statistics.
}

func (s Stats) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("\nopenConnections  %d", s.OpenConnections))
	sb.WriteString(fmt.Sprintf("\nopenTransactions %d", s.OpenTransactions))
	sb.WriteString(fmt.Sprintf("\nopenStatements   %d", s.OpenStatements))
	sb.WriteString(fmt.Sprintf("\nbytesRead        %d", s.BytesRead))
	sb.WriteString(fmt.Sprintf("\nbytesWritten     %d", s.BytesWritten))
	sb.WriteString("\nSQLDurations")
	for i, durationStat := range s.SQLDurations {
		sb.WriteString(fmt.Sprintf("\n  %-8s %s", StatsSQLTexts[i], durationStat.String()))
	}
	return sb.String()
}

// Constants for duration statistics.
const (
	sqlPing = iota
	sqlQuery
	sqlPrepare
	sqlExec
	sqlRollback
	sqlCommit
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

type durationHistogram struct {
	mu              sync.Mutex
	count           uint64
	sum             uint64
	durationBuckets []uint64
	buckets         []uint64
	underflow       uint64 // in case of negative duration (will add to zero bucket)
}

func newDurationHistogram(durationBuckets []uint64) *durationHistogram {
	durationBucketsClone := make([]uint64, len(durationBuckets))
	copy(durationBucketsClone, durationBuckets)
	numBuckets := len(durationBucketsClone)
	if numBuckets == 0 {
		panic("number of duration buckets cannot be zero")
	}
	return &durationHistogram{durationBuckets: durationBucketsClone, buckets: make([]uint64, numBuckets)}
}

func (h *durationHistogram) stats() *DurationStat {
	h.mu.Lock()
	rv := &DurationStat{
		Count:   h.count,
		Sum:     h.sum,
		Buckets: make(map[uint64]uint64, len(h.buckets)),
	}
	for i, durationBucket := range h.durationBuckets {
		rv.Buckets[durationBucket] = h.buckets[i]
	}
	h.mu.Unlock()
	return rv
}

func (h *durationHistogram) add(ms int64) {
	h.mu.Lock()
	h.count++
	if ms < 0 {
		h.underflow++
		h.mu.Unlock()
		return
	}
	h.sum += uint64(ms)
	// determine index
	i := sort.Search(len(h.durationBuckets), func(i int) bool { return h.durationBuckets[i] >= uint64(ms) })
	if i < len(h.durationBuckets) {
		h.buckets[i]++
	}
	h.mu.Unlock()
}

type metrics struct {
	parent             *metrics
	counters           []*counter
	gauges             []*gauge
	durationHistograms []*durationHistogram
}

func newMetrics(parent *metrics) *metrics {
	rv := &metrics{
		parent:             parent,
		counters:           make([]*counter, numCounter),
		gauges:             make([]*gauge, numGauge),
		durationHistograms: make([]*durationHistogram, StatsNumSQL),
	}
	for i := 0; i < numCounter; i++ {
		rv.counters[i] = &counter{}
	}
	for i := 0; i < numGauge; i++ {
		rv.gauges[i] = &gauge{}
	}
	for i := 0; i < int(StatsNumSQL); i++ {
		rv.durationHistograms[i] = newDurationHistogram(StatsDurationBuckets)
	}
	return rv
}

func (m *metrics) addCounterValue(kind int, v uint64) {
	m.counters[kind].add(v)
	if m.parent != nil {
		m.parent.counters[kind].add(v)
	}
}

func (m *metrics) addGaugeValue(kind int, v int64) {
	m.gauges[kind].add(v)
	if m.parent != nil {
		m.parent.gauges[kind].add(v)
	}
}

func (m *metrics) addDurationHistogramValue(kind int, v int64) {
	m.durationHistograms[kind].add(v)
	if m.parent != nil {
		m.parent.durationHistograms[kind].add(v)
	}
}

func (m *metrics) stats() Stats {
	sqlDurations := make([]*DurationStat, StatsNumSQL)
	for i := 0; i < int(StatsNumSQL); i++ {
		sqlDurations[i] = m.durationHistograms[i].stats()
	}
	return Stats{
		OpenConnections:  int(m.gauges[gaugeConn].value()),
		OpenTransactions: int(m.gauges[gaugeTx].value()),
		OpenStatements:   int(m.gauges[gaugeStmt].value()),
		BytesRead:        m.counters[counterBytesRead].value(),
		BytesWritten:     m.counters[counterBytesWritten].value(),
		SQLDurations:     sqlDurations,
	}
}
