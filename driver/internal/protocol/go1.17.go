//go:build !go1.18
// +build !go1.18

// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.17 is out of maintenance.

package protocol

import "database/sql/driver"

func resizeHdbErrorSlice(v []*hdbError, n int) []*hdbError {
	switch {
	case v == nil:
		v = make([]*hdbError, n)
	case n > cap(v):
		v = append(v, make([]*hdbError, n-cap(v))...)
	}
	return v[:n]
}

func resizeTopologyInformationSlice(v []map[topologyOption]interface{}, n int) []map[topologyOption]interface{} {
	switch {
	case v == nil:
		v = make([]map[topologyOption]interface{}, n)
	case n > cap(v):
		v = append(v, make([]map[topologyOption]interface{}, n-cap(v))...)
	}
	return v[:n]
}

func resizeByteSlice(v []byte, n int) []byte {
	switch {
	case v == nil:
		v = make([]byte, n)
	case n > cap(v):
		v = append(v, make([]byte, n-cap(v))...)
	}
	return v[:n]
}

func resizeFieldValues(v []driver.Value, n int) []driver.Value {
	switch {
	case v == nil:
		v = make([]driver.Value, n)
	case n > cap(v):
		v = append(v, make([]driver.Value, n-cap(v))...)
	}
	return v[:n]
}
