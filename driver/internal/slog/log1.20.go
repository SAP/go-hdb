//go:build go1.20

// Delete after go1.19 is out of maintenance.

// Package slog provides a compatibility layer for slog until go1.19 is out of maintenance.
package slog

import (
	sl "golang.org/x/exp/slog"
)

// Level aliases.
const (
	LevelDebug = sl.LevelDebug
	LevelInfo  = sl.LevelInfo
	LevelWarn  = sl.LevelWarn
	LevelError = sl.LevelError
)

var defLogger *Logger = &Logger{Logger: sl.Default()}

// Default is an alias for slog.Default
func Default() *Logger { return defLogger }

// Logger is an alias for slog.Logger
type Logger struct {
	*sl.Logger
}

// With is an alias for slog.Logger.With
func (l *Logger) With(args ...any) *Logger {
	return &Logger{Logger: l.Logger.With(args...)}
}

// Uint64 is an alias for slog.Uint64
func Uint64(key string, value uint64) sl.Attr { return sl.Uint64(key, value) }

// Int64 is an alias for slog.Int64
func Int64(key string, value int64) sl.Attr { return sl.Int64(key, value) }

// String is an alias for slog.String
func String(key, value string) sl.Attr { return sl.String(key, value) }

// Any is an alias for slog.Any
func Any(key string, value any) sl.Attr { return sl.Any(key, value) }

// Group is an alias for slog.Group
func Group(key string, args ...any) sl.Attr { return sl.Group(key, args...) }
