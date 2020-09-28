// SPDX-FileCopyrightText: 2019-2020 Stefan Miller
//
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	hdbVersionMajor = iota
	hdbVersionMinor
	hdbVersionRevision
	hdbVersionPatch
	hdbVersionBuildID
	hdbVersionCount
)

// HDBVersion holds the information of a hdb semantic version.
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
type HDBVersion [hdbVersionCount]uint64 // assumption: all fields are numeric

// ParseHDBVersion parses a semantic hdb version string field.
func ParseHDBVersion(s string) HDBVersion {
	v := HDBVersion{}
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

func (v HDBVersion) String() string {
	s := fmt.Sprintf("%d.%s.%s.%s", v[hdbVersionMajor], formatUint64(v[hdbVersionMinor], 2), formatUint64(v[hdbVersionRevision], 3), formatUint64(v[hdbVersionPatch], 2))
	if v[hdbVersionBuildID] != 0 {
		return fmt.Sprintf("%s.%d", s, v[hdbVersionBuildID])
	}
	return s
}

// IsEmpty returns true if all version fields are zero.
func (v HDBVersion) IsEmpty() bool {
	for _, e := range v {
		if e != 0 {
			return false
		}
	}
	return true
}

// Major returns the major field of a HDBVersion.
func (v HDBVersion) Major() uint64 { return v[hdbVersionMajor] } // Major returns the major field of a HDBVersion.

// Minor returns the minor field of a HDBVersion.
func (v HDBVersion) Minor() uint64 { return v[hdbVersionMinor] }

// SPS returns the sps field of a HDBVersion.
func (v HDBVersion) SPS() uint64 { return v[hdbVersionRevision] / 10 }

// Revision returns the revision field of a HDBVersion.
func (v HDBVersion) Revision() uint64 { return v[hdbVersionRevision] }

// Patch returns the patch field of a HDBVersion.
func (v HDBVersion) Patch() uint64 { return v[hdbVersionPatch] }

// BuildID returns the build id field of a HDBVersion.
func (v HDBVersion) BuildID() uint64 { return v[hdbVersionBuildID] }

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
func (v HDBVersion) Compare(v2 HDBVersion) int {
	for i := 0; i < (hdbVersionCount - 1); i++ { // ignore buildID - might not be ordered}
		if r := compareUint64(v[i], v2[i]); r != 0 {
			return r
		}
	}
	return 0
}
