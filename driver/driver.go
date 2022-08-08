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

	p "github.com/SAP/go-hdb/driver/internal/protocol"
)

// DriverVersion is the version number of the hdb driver.
const DriverVersion = "0.107.2"

// DriverName is the driver name to use with sql.Open for hdb databases.
const DriverName = "hdb"

var clientID string

func init() {
	p.DriverVersion = DriverVersion // set driver version in session
	// clientID
	if hostname, err := os.Hostname(); err == nil {
		clientID = strings.Join([]string{strconv.Itoa(os.Getpid()), hostname}, "@")
	} else {
		clientID = strconv.Itoa(os.Getpid())
	}
	p.ClientID = clientID // set client id in session
	sql.Register(DriverName, hdbDriver)
}

var hdbDriver = newDriver()

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

func newDriver() *Driver {
	return &Driver{metrics: newMetrics(nil)}
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
