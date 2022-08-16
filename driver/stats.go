// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	_ "embed" // embed stats configuration
	"fmt"
	"strings"
)

// StatsHistogram represents statistic data in a histogram structure.
type StatsHistogram struct {
	// Count holds the number of measurements
	Count uint64
	// Sum holds the sum of the measurements.
	Sum uint64
	// Buckets contains the count of measurements belonging to a bucket where the
	// value of the measurement is less or equal the bucket map key.
	Buckets map[uint64]uint64
}

func (s *StatsHistogram) String() string {
	return fmt.Sprintf("count %d sum %d values %v", s.Count, s.Sum, s.Buckets)
}

// Constants for time statistics.
const (
	StatsTimeRead     = iota // Time spent on reading from connection.
	StatsTimeWrite           // Time spent on writing to connection.
	StatsTimeAuth            // Time spent on authentication.
	StatsTimeQuery           // Time spent on executing queries.
	StatsTimePrepare         // Time spent on preparing queries.
	StatsTimeExec            // Time spent on execution queries which do not return rows, like INSERT or UPDATE.
	StatsTimeCall            // Time spent on call statements.
	StatsTimeFetch           // Time spent on fetching rows.
	StatsTimeFetchLob        // Time spent on fetching large objects.
	StatsTimeRollback        // Time spent on rollbacks.
	StatsTimeCommit          // Time spent on commits.
	NumStatsTime
)

// Stats contains driver statistics.
type Stats struct {
	OpenConnections  int             // The number of current established driver connections.
	OpenTransactions int             // The number of current open driver transactions.
	OpenStatements   int             // The number of current open driver database statements.
	BytesRead        uint64          // Total bytes read by client connection.
	BytesWritten     uint64          // Total bytes written by client connection.
	ReadTime         *StatsHistogram // Total time spent reading data from connection.
	WriteTime        *StatsHistogram // Total time spent writing data to connection.

	Times []*StatsHistogram // Spent time statistics (see StatsTime* constants for details).
}

func (s Stats) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("\nopenConnections  %d", s.OpenConnections))
	sb.WriteString(fmt.Sprintf("\nopenTransactions %d", s.OpenTransactions))
	sb.WriteString(fmt.Sprintf("\nopenStatements   %d", s.OpenStatements))
	sb.WriteString(fmt.Sprintf("\nbytesRead        %d", s.BytesRead))
	sb.WriteString(fmt.Sprintf("\nbytesWritten     %d", s.BytesWritten))
	sb.WriteString("\nTimes")
	for i, timeStat := range s.Times {
		sb.WriteString(fmt.Sprintf("\n  %-12s %s", statsCfg.TimeTexts[i], timeStat.String()))
	}
	return sb.String()
}
