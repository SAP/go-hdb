// +build !go1.16

// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete after go1.15 is out of maintenance.

package driver

import (
	"io"
	"io/ioutil"
)

func _readFile(filename string) ([]byte, error) { return ioutil.ReadFile(filename) }
func _readAll(r io.Reader) ([]byte, error)      { return ioutil.ReadAll(r) }
