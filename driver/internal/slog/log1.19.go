//go:build !go1.20

// Delete after go1.19 is out of maintenance.

package slog

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
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

func (l *Logger) Info(msg string, args ...any) {
	l.output(fmt.Sprintf("%s %s %s %s", "INFO", msg, l.args, formatArgs("", args...)))
}

func (l *Logger) Error(msg string, args ...any) {
	l.output(fmt.Sprintf("%s %s %s %s", "ERROR", msg, l.args, formatArgs("", args...)))
}

func (l *Logger) Warn(msg string, args ...any) {
	l.output(fmt.Sprintf("%s %s %s %s", "WARN", msg, l.args, formatArgs("", args...)))
}

func formatArgs(prefix string, args ...any) string {
	l := make([]string, 0, len(args))
	for _, arg := range args {
		if arg, ok := arg.(fmt.Stringer); ok {
			if prefix != "" {
				l = append(l, fmt.Sprintf("%s.%s", prefix, arg.String()))
			} else {
				l = append(l, arg.String())
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
