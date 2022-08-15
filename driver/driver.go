// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"strconv"
	"strings"
)

// DriverVersion is the version number of the hdb driver.
const DriverVersion = "0.107.3"

// DriverName is the driver name to use with sql.Open for hdb databases.
const DriverName = "hdb"

var clientID = func() string {
	if hostname, err := os.Hostname(); err == nil {
		return strings.Join([]string{strconv.Itoa(os.Getpid()), hostname}, "@")
	}
	return strconv.Itoa(os.Getpid())
}()

// clientType is the information provided to HDB identifying the driver.
// Previously the driver.DriverName "hdb" was used but we should be more specific in providing a unique client type to HANA backend.
const clientType = "go-hdb"

var defaultApplicationName, _ = os.Executable()

// driver singleton instance (do not use directly - use getDriver() instead)
var hdbDriver *Driver

func init() {
	// load stats configuration
	if err := loadStatsCfg(); err != nil {
		panic(err) // invalid configuration file
	}

	// instantiate hdbDriver
	hdbDriver = &Driver{newMetrics(nil, statsCfg.TimeBuckets)}
	// register driver
	sql.Register(DriverName, hdbDriver)
}

// driver

// check if driver implements all required interfaces
var (
	_ driver.Driver        = (*Driver)(nil)
	_ driver.DriverContext = (*Driver)(nil)
)

// Driver represents the go sql driver implementation for hdb.
type Driver struct {
	metrics *metrics
}

// Open implements the driver.Driver interface.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := NewDSNConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector implements the driver.DriverContext interface.
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) { return NewDSNConnector(dsn) }

// Name returns the driver name.
func (d *Driver) Name() string { return DriverName }

// Version returns the driver version.
func (d *Driver) Version() string { return DriverVersion }

// Stats returns driver statistics.
func (d *Driver) Stats() Stats { return d.metrics.stats() }
