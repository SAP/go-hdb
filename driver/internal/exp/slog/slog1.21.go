//go:build go1.21

// Delete after go1.20 is out of maintenance.

// Package slog provides a compatibility layer until g1.20 is out of maintenance.
package slog

import goslog "log/slog"

type (
	// Logger is a type aliases for exp/slog/Logger.
	Logger = goslog.Logger
	// Value is a type aliases for exp/slog/Value.
	Value = goslog.Value
	// Attr is a type aliases for exp/slog/Attr.
	Attr = goslog.Attr
)

// Alias definitions for exp/slog.
var (
	LevelInfo  = goslog.LevelInfo
	LevelWarn  = goslog.LevelWarn
	LevelError = goslog.LevelError

	Default    = goslog.Default
	String     = goslog.String
	Uint64     = goslog.Uint64
	Int64      = goslog.Int64
	Any        = goslog.Any
	GroupValue = goslog.GroupValue
)
