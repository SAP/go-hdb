//go:build !unit

package driver_test

import (
	"database/sql"
	"log"
	"os"
	"strconv"

	"github.com/SAP/go-hdb/driver"
)

// ExampleNewDSNConnector shows how to open a database with the help of a connector using DSN.
func ExampleNewDSNConnector() {
	const (
		envDSN = "GOHDBDSN"
	)

	dsn, ok := os.LookupEnv(envDSN)
	if !ok {
		return
	}

	connector, err := driver.NewDSNConnector(dsn)
	if err != nil {
		log.Panic(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Panic(err)
	}
	// output:
}

func lookupTLS() (string, bool, string, bool) {
	const (
		envServerName         = "GOHDBTLSSERVERNAME"
		envInsecureSkipVerify = "GOHDBINSECURESKIPVERIFY"
		envRootCAFile         = "GOHDBROOTCAFILE"
	)

	set := false

	serverName, ok := os.LookupEnv(envServerName)
	if ok {
		set = true
	}
	insecureSkipVerify := false
	if b, ok := os.LookupEnv(envInsecureSkipVerify); ok {
		var err error
		if insecureSkipVerify, err = strconv.ParseBool(b); err != nil {
			log.Fatal(err)
		}
		set = true
	}
	rootCAFile, ok := os.LookupEnv(envRootCAFile)
	if ok {
		set = true
	}
	return serverName, insecureSkipVerify, rootCAFile, set
}

// ExampleNewBasicAuthConnector shows how to open a database with the help of a connector using basic authentication.
func ExampleNewBasicAuthConnector() {
	const (
		envHost     = "GOHDBHOST"
		envUsername = "GOHDBUSERNAME"
		envPassword = "GOHDBPASSWORD"
		envDatabase = "GOHDBDATABASE"
	)

	host, ok := os.LookupEnv(envHost)
	if !ok {
		return
	}
	username, ok := os.LookupEnv(envUsername)
	if !ok {
		return
	}
	password, ok := os.LookupEnv(envPassword)
	if !ok {
		return
	}
	database, ok := os.LookupEnv(envDatabase)
	if !ok {
		return
	}

	connector := driver.NewBasicAuthConnector(host, username, password)
	if serverName, insecureSkipVerify, rootCAFile, ok := lookupTLS(); ok {
		if err := connector.SetTLS(serverName, insecureSkipVerify, rootCAFile); err != nil {
			log.Fatal(err)
		}
	}
	if database != "" {
		connector = connector.WithDatabase(database)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Panic(err)
	}
	// output:
}

// ExampleNewX509AuthConnectorByFiles shows how to open a database with the help of a connector
// using x509 (client certificate) authentication and providing client certificate and client key by file.
func ExampleNewX509AuthConnectorByFiles() {
	const (
		envHost           = "GOHDBHOST"
		envClientCertFile = "GOHDBCLIENTCERTFILE"
		envClientKeyFile  = "GOHDBCLIENTKEYFILE"
	)

	host, ok := os.LookupEnv(envHost)
	if !ok {
		return
	}
	clientCertFile, ok := os.LookupEnv(envClientCertFile)
	if !ok {
		return
	}
	clientKeyFile, ok := os.LookupEnv(envClientKeyFile)
	if !ok {
		return
	}

	connector, err := driver.NewX509AuthConnectorByFiles(host, clientCertFile, clientKeyFile)
	if err != nil {
		log.Panic(err)
	}
	if serverName, insecureSkipVerify, rootCAFile, ok := lookupTLS(); ok {
		if err := connector.SetTLS(serverName, insecureSkipVerify, rootCAFile); err != nil {
			log.Panic(err)
		}
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Panic(err)
	}
	// output:
}

// ExampleNewJWTAuthConnector shows how to open a database with the help of a connector using JWT authentication.
func ExampleNewJWTAuthConnector() {
	const (
		envHost  = "GOHDBHOST"
		envToken = "GOHDBTOKEN"
	)

	host, ok := os.LookupEnv(envHost)
	if !ok {
		return
	}
	token, ok := os.LookupEnv(envToken)
	if !ok {
		return
	}

	const invalidToken = "ey"

	connector := driver.NewJWTAuthConnector(host, invalidToken)
	if serverName, insecureSkipVerify, rootCAFile, ok := lookupTLS(); ok {
		if err := connector.SetTLS(serverName, insecureSkipVerify, rootCAFile); err != nil {
			log.Panic(err)
		}
	}
	// in case JWT authentication fails provide a (new) valid token.
	connector.SetRefreshToken(func() (string, bool) { return token, true })

	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Panic(err)
	}
	// output:
}

// ExampleConnector_WithDatabase shows how to open a tenant database with the help of a connector using basic authentication.
func ExampleConnector_WithDatabase() {
	const (
		envHost     = "GOHDBHOST"
		envUsername = "GOHDBUSERNAME"
		envPassword = "GOHDBPASSWORD"
		envDatabase = "GOHDBDATABASE"
	)

	host, ok := os.LookupEnv(envHost)
	if !ok {
		return
	}
	username, ok := os.LookupEnv(envUsername)
	if !ok {
		return
	}
	password, ok := os.LookupEnv(envPassword)
	if !ok {
		return
	}
	database, ok := os.LookupEnv(envDatabase)
	if !ok {
		return
	}

	connector := driver.NewBasicAuthConnector(host, username, password).WithDatabase(database)
	if serverName, insecureSkipVerify, rootCAFile, ok := lookupTLS(); ok {
		if err := connector.SetTLS(serverName, insecureSkipVerify, rootCAFile); err != nil {
			log.Panic(err)
		}
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Panic(err)
	}
	// output:
}
