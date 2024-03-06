package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// prm represents a test parameter consisting of BatchCount and BatchSize.
type prm struct {
	BatchCount, BatchSize int
}

// prmsValue represents a flag value for parameters.
type prmsValue []prm

// String implements the flag.Value interface.
func (v prmsValue) String() string {
	b := new(bytes.Buffer)
	last := len(v) - 1
	for i, prm := range v {
		b.WriteString(strconv.Itoa(prm.BatchCount))
		b.WriteString("x")
		b.WriteString(strconv.Itoa(prm.BatchSize))
		if i != last {
			b.WriteString(" ")
		}
	}
	return b.String()
}

// Set implements the flag.Value interface.
func (v *prmsValue) Set(s string) error {
	*v = nil // clear slice
	for _, ts := range strings.Split(s, " ") {
		t := strings.Split(ts, "x")
		if len(t) != 2 {
			return fmt.Errorf("invalid value: %s", s)
		}
		var err error
		var prm prm
		prm.BatchCount, err = strconv.Atoi(t[0])
		if err != nil {
			return err
		}
		prm.BatchSize, err = strconv.Atoi(t[1])
		if err != nil {
			return err
		}
		*v = append(*v, prm)
	}
	return nil
}
