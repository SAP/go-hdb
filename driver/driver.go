// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
)

// DriverVersion is the version number of the hdb driver.
const DriverVersion = "0.101.1"

// DriverName is the driver name to use with sql.Open for hdb databases.
const DriverName = "hdb"

// default application name.
var defaultApplicationName string

var drv = &hdbDrv{}

func init() {
	defaultApplicationName, _ = os.Executable()
	sql.Register(DriverName, drv)
}

// driver

//  check if driver implements all required interfaces
var (
	_ driver.Driver        = (*hdbDrv)(nil)
	_ driver.DriverContext = (*hdbDrv)(nil)
)

type hdbDrv struct{}

func (d *hdbDrv) Open(dsn string) (driver.Conn, error) {
	connector, err := NewDSNConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d *hdbDrv) OpenConnector(dsn string) (driver.Connector, error) {
	return NewDSNConnector(dsn)
}
