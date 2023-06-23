package driver

import (
	"sync"
	"testing"
)

// test if refresh in parallel would dead lock
func testParallelRefresh(t *testing.T) {
	const numParallelRefresh = 100

	attrs := &authAttrs{}
	attrs.SetRefreshPassword(func() (string, bool) { return "", true })
	attrs.SetRefreshToken(func() (string, bool) { return "", true })
	attrs.SetRefreshClientCert(func() ([]byte, []byte, bool) { return nil, nil, true })

	wg := new(sync.WaitGroup)
	wg.Add(numParallelRefresh)
	start := make(chan struct{})
	for i := 0; i < numParallelRefresh; i++ { // start 10 refresh go routines
		go func(start <-chan struct{}, wg *sync.WaitGroup) {
			defer wg.Done()
			<-start
			attrs.refresh()
		}(start, wg)
	}
	// start refresh in parallel
	close(start)
	// wait for all go routines to end
	wg.Wait()
}

func TestAuthAttrs(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"testParallelRefresh", testParallelRefresh},
	}

	for _, test := range tests {
		func(name string, fct func(t *testing.T)) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				fct(t)
			})
		}(test.name, test.fct)
	}
}
