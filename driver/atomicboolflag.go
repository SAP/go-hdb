package driver

import (
	"strconv"
	"sync/atomic"
)

type atomicBoolFlag struct {
	atomic.Bool
}

func (f *atomicBoolFlag) Set(s string) error {
	v, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	f.Bool.Store(v)
	return nil
}

func (f *atomicBoolFlag) Get() any { return f.Load() }
func (f *atomicBoolFlag) String() string {
	if f == nil {
		return ""
	}
	return strconv.FormatBool(f.Load())
}
func (f *atomicBoolFlag) IsBoolFlag() bool { return true }
