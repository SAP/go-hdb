// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	_ "embed" // embed stats configuration
	"fmt"
	"strings"
)

// TimeStat represents a statistic of spent time.
type TimeStat struct {
	// Count holds the number of measurements
	Count uint64
	// Sum holds the sum of the spent time in milliseconds.
	Sum uint64
	// The bucket key is the upper time limit in milliseconds for a time measurement falling in this category with
	// time measurement <= time limt.
	// The bucket value is the number of measurements falling in the time limit category.
	Buckets map[uint64]uint64 // Count bucketsmap[<duration in ms>]<counter>.
}

func (s *TimeStat) String() string {
	return fmt.Sprintf("count %d sum %d values %v", s.Count, s.Sum, s.Buckets)
}

// Stats contains driver statistics.
type Stats struct {
	// Gauges
	OpenConnections  int // The number of established driver connections.
	OpenTransactions int // The number of open driver transactions.
	OpenStatements   int // The number of open driver database statements.
	// Counter
	BytesRead    uint64 // Total bytes read by client connection.
	BytesWritten uint64 // Total bytes written by client connection.
	//
	ReadTime  *TimeStat
	WriteTime *TimeStat

	TimeStats []*TimeStat // Spent time statistics.
}

func (s Stats) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("\nopenConnections  %d", s.OpenConnections))
	sb.WriteString(fmt.Sprintf("\nopenTransactions %d", s.OpenTransactions))
	sb.WriteString(fmt.Sprintf("\nopenStatements   %d", s.OpenStatements))
	sb.WriteString(fmt.Sprintf("\nbytesRead        %d", s.BytesRead))
	sb.WriteString(fmt.Sprintf("\nbytesWritten     %d", s.BytesWritten))
	sb.WriteString("\nTimes")
	for i, timeStat := range s.TimeStats {
		sb.WriteString(fmt.Sprintf("\n  %-12s %s", statsCfg.TimeTexts[i], timeStat.String()))
	}
	return sb.String()
}
