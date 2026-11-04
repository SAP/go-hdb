package driver

import "testing"

func TestParseDSNConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{"valid without query", "hdb://user:pass@localhost:30015", false},
		{"valid with query", "hdb://user:pass@localhost:30015?databaseName=TENANT&timeout=5", false},
		{"valid with escaped value", "hdb://user:pass@localhost:30015?TLSServerName=db%2Eexample.com&TLSInsecureSkipVerify=false", false},
		{"invalid escape", "hdb://user:pass@localhost:30015?TLSRootCAFile=%ZZ", true},
		{"invalid separator", "hdb://user:pass@localhost:30015?TLSServerName=localhost;ignored=x", true},
		{"invalid query with timeout", "hdb://user:pass@localhost:30015?timeout=5&TLSRootCAFile=%ZZ", true},
		{"invalid query with server name", "hdb://user:pass@localhost:30015?TLSServerName=localhost&TLSRootCAFile=%ZZ", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseDSNConfig(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Fatalf("got error %v, want error: %t", err, tt.wantErr)
			}
		})
	}
}
