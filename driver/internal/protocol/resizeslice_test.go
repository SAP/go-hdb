package protocol

import (
	"testing"
)

func TestResizeSlice(t *testing.T) {
	testSlices := [][]bool{
		nil,
		make([]bool, 0, 2),
	}
	n := 3

	for _, s := range testSlices {
		s = resizeSlice(s, n)
		if cap(s) < n || len(s) < n {
			t.Fatalf("resize error: cap %d len %d n %d", cap(s), len(s), n)
		}
	}
}
