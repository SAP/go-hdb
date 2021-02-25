// +build go1.16

// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Delete and re-itegrate after go1.15 is out of maintenance.

package driver

import (
	"io"
	"os"
)

func _readFile(filename string) ([]byte, error) { return os.ReadFile(filename) }
func _readAll(r io.Reader) ([]byte, error)      { return io.ReadAll(r) }
