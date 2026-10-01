package driver

import (
	"context"
	"database/sql/driver"
	"flag"
	"fmt"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	protTrace atomic.Bool
	sqlTrace  atomic.Bool
)

func setTrace(b *atomic.Bool, s string) error {
	v, err := strconv.ParseBool(s)
	if err == nil {
		b.Store(v)
	}
	return err
}

func init() {
	flag.BoolFunc("hdb.protTrace", "enabling hdb protocol trace", func(s string) error { return setTrace(&protTrace, s) })
	flag.BoolFunc("hdb.sqlTrace", "enabling hdb sql trace", func(s string) error { return setTrace(&sqlTrace, s) })
}

// ProtTrace returns true if protocol tracing output is active, false otherwise.
func ProtTrace() bool { return protTrace.Load() }

// SetProtTrace sets protocol tracing output active or inactive.
// Credential fields in protocol trace output are masked by default and only
// shown in full if redaction is explicitly disabled. If the application cannot
// fully control command-line flags (e.g. in shared or managed environments),
// call SetProtTrace(false) at startup to override the -hdb.protTrace flag.
func SetProtTrace(on bool) { protTrace.Store(on) }

// SQLTrace returns true if sql tracing output is active, false otherwise.
func SQLTrace() bool { return sqlTrace.Load() }

// SetSQLTrace sets sql tracing output active or inactive.
func SetSQLTrace(on bool) { sqlTrace.Store(on) }

const (
	tracePing     = "ping"
	tracePrepare  = "prepare"
	traceQuery    = "query"
	traceExec     = "exec"
	traceExecCall = "call"
)

type sqlTracer struct {
	logger *slog.Logger
	maxArg int
	cfg    SQLTraceConfig
}

const defSQLTracerMaxArg = 5 // default limit of number of arguments

// truncMs renders milliseconds truncated to two decimals via integer math.
func truncMs(d time.Duration) float64 {
	return float64(d.Microseconds()/10) / 100
}

// newSQLTracer returns nil unless cfg enables every-statement logging
// or at least one trip leg.
func newSQLTracer(logger *slog.Logger, maxArg int, cfg SQLTraceConfig) *sqlTracer {
	if !cfg.Enabled && cfg.ServerThreshold <= 0 && cfg.TotalThreshold <= 0 {
		return nil
	}
	if maxArg <= 0 {
		maxArg = defSQLTracerMaxArg
	}
	return &sqlTracer{logger: logger, maxArg: maxArg, cfg: cfg}
}

func (t *sqlTracer) log(ctx context.Context, startTime time.Time, traceKind string, query string, serverTime time.Duration, nvargs ...driver.NamedValue) {
	elapsed := time.Since(startTime)
	l := len(nvargs)

	slow := (t.cfg.ServerThreshold > 0 && serverTime >= t.cfg.ServerThreshold) ||
		(t.cfg.TotalThreshold > 0 && elapsed >= t.cfg.TotalThreshold)
	if !t.cfg.Enabled && !slow {
		return
	}
	level := slog.LevelInfo
	if slow {
		level = slog.LevelWarn
	}

	attrs := []slog.Attr{
		slog.String(traceKind, query),
		slog.Float64("ms", truncMs(elapsed)),
	}
	if serverTime > 0 {
		attrs = append(attrs, slog.Float64("serverMs", truncMs(serverTime)))
	}

	if l == 0 {
		t.logger.LogAttrs(ctx, level, "SQL", attrs...)
		return
	}

	numArg := min(l, t.maxArg)
	argAttrs := make([]slog.Attr, 0, numArg)
	for i := range numArg {
		name := nvargs[i].Name
		if name == "" {
			name = strconv.Itoa(nvargs[i].Ordinal)
		}
		argAttrs = append(argAttrs, slog.String(name, fmt.Sprintf("%v", nvargs[i].Value)))
	}
	if l > t.maxArg {
		argAttrs = append(argAttrs, slog.Int("numArgSkip", l-t.maxArg))
	}
	attrs = append(attrs, slog.Any("arg", slog.GroupValue(argAttrs...)))

	t.logger.LogAttrs(ctx, level, "SQL", attrs...)
}
