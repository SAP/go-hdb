//go:build !go1.20

// Delete after go1.19 is out of maintenance.

package slog

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type Level int

const (
	LevelDebug Level = -4
	LevelInfo  Level = 0
	LevelWarn  Level = 4
	LevelError Level = 8
)

var defLogger *Logger = &Logger{Logger: log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile)}

func Default() *Logger { return defLogger }

type Logger struct {
	args string
	*log.Logger
}

func (l *Logger) With(args ...any) *Logger {
	return &Logger{Logger: l.Logger, args: formatArgs("", args...)}
}

func (l *Logger) output(msg string) { l.Logger.Output(3, msg) }

func (l *Logger) LogAttrs(ctx context.Context, level Level, msg string, attrs ...any) {
	switch level {
	case LevelInfo:
		l.output(fmt.Sprintf("%s %s %s %s", "INFO", msg, l.args, formatArgs("", attrs...)))
	case LevelWarn:
		l.output(fmt.Sprintf("%s %s %s %s", "WARN", msg, l.args, formatArgs("", attrs...)))
	case LevelError:
		l.output(fmt.Sprintf("%s %s %s %s", "ERROR", msg, l.args, formatArgs("", attrs...)))
	}
}

func formatArgs(prefix string, attrs ...any) string {
	l := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		if attr, ok := attr.(fmt.Stringer); ok {
			if prefix != "" {
				l = append(l, fmt.Sprintf("%s.%s", prefix, attr.String()))
			} else {
				l = append(l, attr.String())
			}
		}
	}
	return strings.Join(l, " ")
}

type StringAttr struct {
	Key   string
	Value string
}
type AnyAttr struct {
	Key   string
	Value any
}
type Uint64Attr struct {
	Key   string
	Value uint64
}
type Int64Attr struct {
	Key   string
	Value int64
}
type GroupAttr struct {
	Key  string
	Args []any
}

func (a StringAttr) String() string { return fmt.Sprintf("%s=%s", a.Key, strconv.Quote(a.Value)) } // simplify: quote all
func (a AnyAttr) String() string    { return fmt.Sprintf("%s=%v", a.Key, a.Value) }
func (a Uint64Attr) String() string { return fmt.Sprintf("%s=%d", a.Key, a.Value) }
func (a Int64Attr) String() string  { return fmt.Sprintf("%s=%d", a.Key, a.Value) }
func (a GroupAttr) String() string  { return formatArgs(a.Key, a.Args...) }

func String(key, value string) StringAttr        { return StringAttr{key, value} }
func Any(key string, value any) AnyAttr          { return AnyAttr{key, value} }
func Uint64(key string, value uint64) Uint64Attr { return Uint64Attr{key, value} }
func Int64(key string, value int64) Int64Attr    { return Int64Attr{key, value} }
func Group(key string, args ...any) GroupAttr    { return GroupAttr{Key: key, Args: args} }
