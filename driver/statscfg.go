// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	_ "embed" // embed stats configuration
	"encoding/json"
	"fmt"
)

//go:embed statscfg.json
var statsCfgRaw []byte

var statsCfg struct {
	SQLTimeTexts    []string  `json:"sqlTimeTexts"`
	TimeUpperBounds []float64 `json:"timeUpperBounds"`
}

func loadStatsCfg() error {

	if err := json.Unmarshal(statsCfgRaw, &statsCfg); err != nil {
		return fmt.Errorf("invalid statscfg.json file: %s", err)
	}

	if len(statsCfg.SQLTimeTexts) != int(numSQLTime) {
		return fmt.Errorf("invalid number of statscfg.json sqlTimeTexts %d - expected %d", len(statsCfg.SQLTimeTexts), numSQLTime)
	}
	if len(statsCfg.TimeUpperBounds) == 0 {
		return fmt.Errorf("number of statscfg.json timeUpperBounds needs to be greater than %d", 0)
	}

	// sort and dedup timeBuckets
	sortSliceFloat64(statsCfg.TimeUpperBounds)
	statsCfg.TimeUpperBounds = compactSliceFloat64(statsCfg.TimeUpperBounds)

	return nil
}
