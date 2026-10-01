//go:build !unit

package driver_test

import (
	"context"
	"database/sql"
	"log"
	"os"
	"strconv"

	"github.com/SAP/go-hdb/driver"
)

// ExampleParseDSNConfig shows how to open a database with the help of a connector configured by DSN.
func ExampleParseDSNConfig() {
	const (
		envDSN = "GOHDBDSN"
	)

	dsn, ok := os.LookupEnv(envDSN)
	if !ok {
		return
	}

	cfg, err := driver.ParseDSNConfig(dsn)
	if err != nil {
		log.Fatal(err)
	}
	connector, err := driver.NewConfigConnector(cfg)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		log.Fatal(err)
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

// ExampleNewConfigConnector shows how to open a database with the help of a connector using basic authentication.
func ExampleNewConfigConnector() {
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

	cfg := driver.NewConnectorConfig()
	cfg.Host = host
	cfg.Username = username
	cfg.Password = password
	cfg.DatabaseName = database
	if serverName, insecureSkipVerify, rootCAFile, ok := lookupTLS(); ok {
		tlsConfig, err := driver.NewTLSConfig(serverName, insecureSkipVerify, rootCAFile)
		if err != nil {
			log.Fatal(err)
		}
		cfg.TLSConfig = tlsConfig
	}
	connector, err := driver.NewConfigConnector(cfg)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		log.Fatal(err)
	}
	// output:
}

// ExampleNewConfigConnector_x509 shows how to open a database with the help of a connector
// using x509 (client certificate) authentication and providing client certificate and client key by file.
func ExampleNewConfigConnector_x509() {
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

	cfg := driver.NewConnectorConfig()
	cfg.Host = host
	cfg.ClientCertFile = clientCertFile
	cfg.ClientKeyFile = clientKeyFile
	if serverName, insecureSkipVerify, rootCAFile, ok := lookupTLS(); ok {
		tlsConfig, err := driver.NewTLSConfig(serverName, insecureSkipVerify, rootCAFile)
		if err != nil {
			log.Fatal(err)
		}
		cfg.TLSConfig = tlsConfig
	}
	connector, err := driver.NewConfigConnector(cfg)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		log.Fatal(err)
	}
	// output:
}

// ExampleNewConfigConnector_jwt shows how to open a database with the help of a connector using JWT authentication.
func ExampleNewConfigConnector_jwt() {
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

	cfg := driver.NewConnectorConfig()
	cfg.Host = host
	cfg.Token = invalidToken
	// in case JWT authentication fails provide a (new) valid token.
	cfg.RefreshToken = func() (string, bool) { return token, true }
	if serverName, insecureSkipVerify, rootCAFile, ok := lookupTLS(); ok {
		tlsConfig, err := driver.NewTLSConfig(serverName, insecureSkipVerify, rootCAFile)
		if err != nil {
			log.Fatal(err)
		}
		cfg.TLSConfig = tlsConfig
	}
	connector, err := driver.NewConfigConnector(cfg)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		log.Fatal(err)
	}
	// output:
}
