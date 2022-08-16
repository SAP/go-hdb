// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	_ "embed" // embed stats configuration
	"encoding/json"
	"fmt"
)

// StatsTimeTexts returns the texts of time measurement categories.
func StatsTimeTexts() []string { return cloneStringSlice(statsCfg.TimeTexts) }

//go:embed statscfg.json
var statsCfgRaw []byte

var statsCfg struct {
	TimeTexts   []string `json:"timeTexts"`
	TimeBuckets []uint64 `json:"timeBuckets"`
}

func loadStatsCfg() error {

	if err := json.Unmarshal(statsCfgRaw, &statsCfg); err != nil {
		return fmt.Errorf("invalid statscfg.json file: %s", err)
	}

	if len(statsCfg.TimeTexts) != int(NumStatsTime) {
		return fmt.Errorf("invalid number of statscfg.json timeTexts %d - expected %d", len(statsCfg.TimeTexts), NumStatsTime)
	}
	if len(statsCfg.TimeBuckets) == 0 {
		return fmt.Errorf("number of statscfg.json timeBuckets needs to be greater than %d", 0)
	}

	// sort and dedup timeBuckets
	sortSliceUint64(statsCfg.TimeBuckets)
	statsCfg.TimeBuckets = compactSliceUint64(statsCfg.TimeBuckets)

	return nil
}
