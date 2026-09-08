package driver

import (
	"sync"
	"testing"
	"time"
)

func TestMetricsConnLifecycle(t *testing.T) {
	m := newMetrics(nil, "µs", []float64{1, 2})

	// first wave
	m.addConn()
	m.msgCh <- timeMsg{idx: timeRead, d: time.Microsecond}
	m.removeConn() // closes channel, collector exits; must not block on itself

	// second wave (channel recreated)
	m.addConn()
	m.msgCh <- counterMsg{v: 1, idx: counterBytesRead}
	m.removeConn()
}

func TestMetricsSendBeforeRemoveAll(t *testing.T) {
	m := newMetrics(nil, "µs", []float64{1, 2})
	m.addConn()
	// sends during conn lifetime are delivered
	for range 10 {
		m.msgCh <- gaugeMsg{idx: gaugeConn, v: 1}
		m.msgCh <- sqlTimeMsg{idx: sqlTimeQuery, d: time.Microsecond}
	}
	m.removeConn()
}

func TestMetricsSendCloseConcurrent(t *testing.T) {
	m := newMetrics(nil, "µs", []float64{1, 2})

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			m.addConn() // a "conn" holds the metrics alive
			for range 1000 {
				m.msgCh <- gaugeMsg{idx: gaugeConn, v: 1}
			}
			m.removeConn() // sends strictly precede removeConn
		})
	}
	wg.Wait()
}

func TestMetricsUnpairedRemoveConn(t *testing.T) {
	m := newMetrics(nil, "µs", []float64{1, 2})
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	m.removeConn()
}
