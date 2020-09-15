// SPDX-FileCopyrightText: 2019-2020 Stefan Miller
//
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	versionMajor = iota
	versionMinor
	versionRevision
	versionPatch
	versionBuildID
	versionCount
)

// Version holds the information of a hdb semantic version.
//
// u.vv.wwx.yy.zzzzzzzzzz
//
// u.vv:       hdb version (major.minor)
// ww:         SPS number
// wwx:        revision number
// yy:         patch number
// zzzzzzzzzz: build id
//
// Example: 2.00.045.00.1575639312
//
// hdb version:     2.00
// SPS number:      4
// revision number: 45
// patch number:    0
// build id:        1575639312
type hdbVersion [versionCount]uint64 // assumption: all fields are numeric

// parseVersion parses a semantic hdb version string field.
func parseHDBVersion(s string) hdbVersion {
	v := hdbVersion{}
	parts := strings.SplitN(s, ".", 5)
	for i := 0; i < len(parts); i++ {
		v[i], _ = strconv.ParseUint(parts[i], 10, 64)
	}
	return v
}

func formatUint64(i uint64, digits int) string {
	s := strings.Repeat("0", digits) + strconv.FormatUint(i, 10)
	return s[len(s)-digits:]
}

func (v hdbVersion) String() string {
	s := fmt.Sprintf("%d.%s.%s.%s", v[versionMajor], formatUint64(v[versionMinor], 2), formatUint64(v[versionRevision], 3), formatUint64(v[versionPatch], 2))
	if v[versionBuildID] != 0 {
		return fmt.Sprintf("%s.%d", s, v[versionBuildID])
	}
	return s
}

func (v hdbVersion) isEmpty() bool {
	for _, e := range v {
		if e != 0 {
			return false
		}
	}
	return true
}

func (v hdbVersion) major() uint64    { return v[versionMajor] }
func (v hdbVersion) minor() uint64    { return v[versionMinor] }
func (v hdbVersion) sps() uint64      { return v[versionRevision] / 10 }
func (v hdbVersion) revision() uint64 { return v[versionRevision] }
func (v hdbVersion) patch() uint64    { return v[versionPatch] }
func (v hdbVersion) buildID() uint64  { return v[versionBuildID] }

func compareUint64(u1, u2 uint64) int {
	switch {
	case u1 == u2:
		return 0
	case u1 > u2:
		return 1
	default:
		return -1
	}
}

// Compare compares the version with a second version v2. The result will be
//  0 in case the two versions are equal,
// -1 in case version v has lower precedence than c2,
//  1 in case version v has higher precedence than c2.
func (v hdbVersion) compare(v2 hdbVersion) int {
	for i := 0; i < (versionCount - 1); i++ { // ignore buildID - might not be ordered}
		if r := compareUint64(v[i], v2[i]); r != 0 {
			return r
		}
	}
	return 0
}
