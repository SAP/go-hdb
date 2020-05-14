/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// DriverVersion is the version number of the hdb driver.
const DriverVersion = "0.100.6"

// DriverName is the driver name to use with sql.Open for hdb databases.
const DriverName = "hdb"

var drv = &hdbDrv{}

//nolint:gochecknoinits
func init() {
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
