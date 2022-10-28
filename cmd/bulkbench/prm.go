package main

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// prm represents a test parameter consisting of BatchCount and BatchSize
type prm struct {
	BatchCount, BatchSize int
}

// prmValue represents a flag Value fpr parameters.
type prmValue struct {
	prms []prm
}

// String implements the flag.Value interface.
func (v *prmValue) String() string {
	b := new(bytes.Buffer)
	last := len(v.prms) - 1
	for i, prm := range v.prms {
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
func (v *prmValue) Set(s string) error {
	if v.prms == nil {
		v.prms = []prm{}
	} else {
		v.prms = v.prms[:0]
	}

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
		v.prms = append(v.prms, prm)
	}
	return nil
}

// toNumRecordList returns a list of lists of prms with equal number of records.
func (v *prmValue) toNumRecordList() [][]prm {

	// create categories by number of records
	m := make(map[int][]prm)

	for _, prm := range v.prms {
		numRecord := prm.BatchCount * prm.BatchSize
		m[numRecord] = append(m[numRecord], prm)
	}
	s := []int{}
	for numRecord := range m {
		s = append(s, numRecord)
	}
	// sort by number of records
	sort.Ints(s)

	r := make([][]prm, len(s))
	for i, numRecord := range s {
		r[i] = m[numRecord]
	}
	return r
}
