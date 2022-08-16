// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

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

type histogram struct {
	count     uint64
	sum       uint64
	keys      []uint64
	values    []uint64
	underflow uint64 // in case of negative duration (will add to zero bucket)
}

func newHistogram(keys []uint64) *histogram {
	return &histogram{keys: keys, values: make([]uint64, len(keys))}
}

func (h *histogram) stats() *StatsHistogram {
	rv := &StatsHistogram{
		Count:   h.count,
		Sum:     h.sum / 1e6, // time in milliseconds
		Buckets: make(map[uint64]uint64, len(h.keys)),
	}
	for i, key := range h.keys {
		rv.Buckets[key] = h.values[i]
	}
	return rv
}

func (h *histogram) add(ns int64) { // time in nanoseconds
	h.count++
	if ns < 0 {
		h.underflow++
		return
	}
	h.sum += uint64(ns)
	// determine index
	i := binarySearchSliceUint64(h.keys, uint64(ns)/1e6) // bucket in milliseconds
	if i < len(h.keys) {
		h.values[i]++
	}
}

type counterMsg struct {
	v   uint64
	idx int
}

type gaugeMsg struct {
	v   int64
	idx int
}

type metrics struct {
	parent   *metrics
	counters []uint64
	gauges   []int64
	times    []*histogram

	chCounters   chan counterMsg
	chGauges     chan gaugeMsg
	chHistograms chan gaugeMsg
	chReqStats   chan chan *Stats
}

const (
	numChMetrics = 100
	numChStats   = 10
)

func newMetrics(parent *metrics, timeKeys []uint64) *metrics {
	rv := &metrics{
		parent:       parent,
		counters:     make([]uint64, numCounter),
		gauges:       make([]int64, numGauge),
		times:        make([]*histogram, NumStatsTime),
		chCounters:   make(chan counterMsg, numChMetrics),
		chGauges:     make(chan gaugeMsg, numChMetrics),
		chHistograms: make(chan gaugeMsg, numChMetrics),
		chReqStats:   make(chan chan *Stats, numChStats),
	}
	for i := 0; i < int(NumStatsTime); i++ {
		rv.times[i] = newHistogram(timeKeys)
	}

	go rv.collect(rv.chCounters, rv.chGauges, rv.chHistograms, rv.chReqStats)
	return rv
}

func (m *metrics) buildStats() *Stats {
	times := make([]*StatsHistogram, NumStatsTime)
	for i, th := range m.times {
		times[i] = th.stats()
	}
	return &Stats{
		OpenConnections:  int(m.gauges[gaugeConn]),
		OpenTransactions: int(m.gauges[gaugeTx]),
		OpenStatements:   int(m.gauges[gaugeStmt]),
		BytesRead:        m.counters[counterBytesRead],
		BytesWritten:     m.counters[counterBytesWritten],
		Times:            times,
	}
}

func (m *metrics) collect(chCounters <-chan counterMsg, chGauges <-chan gaugeMsg, chHistograms <-chan gaugeMsg, chReqStats <-chan chan *Stats) {
	for {
		select {
		case msg := <-chCounters:
			m.counters[msg.idx] += msg.v
			if m.parent != nil {
				m.parent.chCounters <- msg
			}
		case msg := <-chGauges:
			m.gauges[msg.idx] += msg.v
			if m.parent != nil {
				m.parent.chGauges <- msg
			}
		case msg := <-chHistograms:
			m.times[msg.idx].add(msg.v)
			if m.parent != nil {
				m.parent.chHistograms <- msg
			}
		case chStats := <-chReqStats:
			chStats <- m.buildStats()
		}
	}
}

func (m *metrics) stats() Stats {
	chStats := make(chan *Stats)
	m.chReqStats <- chStats
	return *<-chStats
}
