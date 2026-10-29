//go:build !unit

package driver_test

import (
	"bytes"
	"database/sql"
	"log/slog"
	"strings"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func TestRedaction(t *testing.T) {
	t.Parallel()

	// switchUserConnect verifies the switch-user connect statement password is
	// redacted in the trace. A switch to a bogus user is rejected by the server,
	// but the connect statement is still built and written (and therefore
	// traced) first: the statement must appear with the username but without the
	// password. Distinct, non-existent credentials are used so the assertions
	// never touch a live secret.
	switchUserConnect := func(t *testing.T) {
		const user = "redactionTestUser"
		const password = "redactionTestPassword"

		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

		cfg := driver.MT.Connector().Config()
		cfg.Logger = logger
		cfg.SQLTrace = driver.SQLTraceConfig{Enabled: true}
		cfg.ProtTrace = driver.ProtTraceConfig{Enabled: true}

		ctr, err := driver.NewConfigConnector(&cfg)
		if err != nil {
			t.Fatal(err)
		}

		db := sql.OpenDB(ctr)
		defer db.Close()

		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		// The query triggers the switch user; the connect to the bogus user is
		// written (and traced) before the server rejects it.
		ctx := driver.WithUserSwitch(t.Context(), &driver.SessionUser{Username: user, Password: password})
		var n int
		if err := conn.QueryRowContext(ctx, "select 1 from dummy").Scan(&n); err == nil {
			t.Fatal("expected switch user to fail for a non-existent user")
		}

		trace := buf.String()
		if !strings.Contains(trace, user) {
			t.Fatalf("connect statement not traced: user %q absent", user)
		}
		if strings.Contains(trace, password) {
			t.Fatalf("password %q leaked in trace", password)
		}
	}

	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{"switch user connect", switchUserConnect},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.fn(t)
		})
	}
}
