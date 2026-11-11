//go:build !unit && !go1.27

package driver_test

// extraScanTargetTests is empty on go1.26: native lob scan targets (string, []byte, io.Writer and
// their nullable forms) are a go1.27 feature. On go1.26 a lob reaches a destination only through
// sql.Scanner, so those destinations are not scannable and the same-type lob lines cover what works.
func extraScanTargetTests() []tester { return nil }
