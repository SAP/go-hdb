// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"fmt"
	"log"
	"os"
)

const (
	logPrefix = "hdb.driver"
)

var dlog = log.New(os.Stderr, fmt.Sprintf("%s ", logPrefix), log.Ldate|log.Ltime|log.Lshortfile)
