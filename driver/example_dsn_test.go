package driver_test

import (
	"database/sql"
	"log"
	"net/url"

	"github.com/SAP/go-hdb/driver"
)

// dsn creates data source name with the help of the net/url package.
func dsn() string {
	dsn := &url.URL{
		Scheme: driver.DriverName,
		User:   url.UserPassword("user", "password"),
		Host:   "host:port",
	}
	return dsn.String()
}

// ExampleDSN shows how to construct a DSN (data source name) as url.
func ExampleDSN() {
	db, err := sql.Open(driver.DriverName, dsn())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
}
