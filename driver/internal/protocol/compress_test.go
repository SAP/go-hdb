package protocol

import "testing"

func TestCompressionBeneficial(t *testing.T) {
	tests := []struct {
		name            string
		uncompressed    int
		compressed      int
		wantCompression bool
	}{
		{"compression saves well over 5 percent", 100000, 4000, true},
		{"compression saves exactly 5 percent (boundary)", 100, 95, true},
		{"compression saves 20 percent", 100000, 80000, true},
		{"compression saves 50 percent", 100000, 50000, true},
		{"compression saves just under 5 percent", 100, 96, false},
		{"compression saves nothing (same size)", 1000, 1000, false},
		{"compressed larger than input", 1000, 1200, false},
		{"incompressible input", 100000, 99999, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compressionBeneficial(tt.uncompressed, tt.compressed); got != tt.wantCompression {
				t.Fatalf("compressionBeneficial(%d, %d) = %t, want %t",
					tt.uncompressed, tt.compressed, got, tt.wantCompression)
			}
		})
	}
}
