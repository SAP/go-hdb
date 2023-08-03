//go:build !go1.21

// Delete after go1.20 is out of maintenance.

// Package slog provides a compatibility layer until g1.20 is out of maintenance.
package slog

import expslog "golang.org/x/exp/slog"

type (
	// Logger is a type aliases for exp/slog/Logger.
	Logger = expslog.Logger
	// Value is a type aliases for exp/slog/Value.
	Value = expslog.Value
	// Attr is a type aliases for exp/slog/Attr.
	Attr = expslog.Attr
)

// Alias definitions for exp/slog.
var (
	LevelInfo  = expslog.LevelInfo
	LevelWarn  = expslog.LevelWarn
	LevelError = expslog.LevelError

	Default    = expslog.Default
	String     = expslog.String
	Uint64     = expslog.Uint64
	Int64      = expslog.Int64
	Any        = expslog.Any
	GroupValue = expslog.GroupValue
)
