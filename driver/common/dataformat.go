// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package common

// Data format version values.
const (
	DfvLevel0 int = 0 // base data format
	DfvLevel1 int = 1 // eval types support all data types
	DfvLevel2 int = 2 // reserved, broken, do not use
	DfvLevel3 int = 3 // additional types Longdate, Secondate, Daydate, Secondtime supported for NGAP
	DfvLevel4 int = 4 // generic support for new date/time types
	DfvLevel5 int = 5 // spatial types in ODBC on request
	DfvLevel6 int = 6 // BINTEXT
	DfvLevel7 int = 7 // with boolean support
	DfvLevel8 int = 8 // with FIXED8/12/16 support
)

// IsSupportedDfv returns true if the data format version dfv is supported by the driver, false otherwise.
func IsSupportedDfv(dfv int) bool {
	return dfv == DfvLevel1 || dfv == DfvLevel4 || dfv == DfvLevel6 || dfv == DfvLevel8
}

// SupportedDfvs returns a slice of data format versions supported by the driver.
var SupportedDfvs = []int{DfvLevel1, DfvLevel4, DfvLevel6, DfvLevel8}
