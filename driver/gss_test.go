package driver

import "testing"

type testGSS struct{}

func (testGSS) Principal() string { return "user@EXAMPLE.TEST" }

func (testGSS) InitSecContext(string) ([]byte, func([]byte) error, error) { return nil, nil, nil }

func TestGSSConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		set   func(cfg *ConnectorConfig)
		valid bool
	}{
		{"GSS alone", func(*ConnectorConfig) {}, true},
		{"GSS with username", func(cfg *ConnectorConfig) { cfg.Username = "SYSTEM" }, false},
		{"GSS with password", func(cfg *ConnectorConfig) { cfg.Password = "secret" }, false},
		{"GSS with token", func(cfg *ConnectorConfig) { cfg.Token = "eyJ" }, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := NewConnectorConfig()
			cfg.Host = "localhost:39041"
			cfg.GSS = testGSS{}
			test.set(cfg)
			if err := cfg.validate(); (err == nil) != test.valid {
				t.Fatalf("valid %t, got error %v", test.valid, err)
			}
		})
	}
}
