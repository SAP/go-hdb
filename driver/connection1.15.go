// +build go1.15

// SPDX-FileCopyrightText: 2014-2021 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"database/sql/driver"
)

//  check if conn implements all required interfaces
var (
	_ driver.Validator = (*conn)(nil)
)

// IsValid implements the driver.Validator interface.
func (c *conn) IsValid() bool {
	c.lock()
	defer c.unlock()

	return !c.dbConn.isBad()
}
