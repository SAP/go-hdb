// SPDX-FileCopyrightText: 2014-2022 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

package driver_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/SAP/go-hdb/driver"
)

const (
	envHost                  = "GOHDBHOST"
	envUsername              = "GOHDBUSERNAME"
	envPassword              = "GOHDBPASSWORD"
	envClientCertFile        = "GOHDBCLIENTCERTFILE"
	envClientKeyFile         = "GOHDBCLIENTKEYFILE"
	envToken                 = "GOHDBTOKEN"
	envTLSServerName         = "GOHDBTLSSERVERNAME"
	envTLSInsecureSkipVerify = "GOHDBTLSINSECURESKIPVERIFY"
	envTLSRootCAFile         = "GOHDBTLSROOTCAFILE"
)

func basicAuthPrms() (host, username, password string, ok bool) {
	if host, ok = os.LookupEnv(envHost); !ok {
		return
	}
	if username, ok = os.LookupEnv(envUsername); !ok {
		return
	}
	password, ok = os.LookupEnv(envPassword)
	return
}

func x509AuthPrms() (host, username, clientCertFile, clientKeyFile string, ok bool) {
	if host, ok = os.LookupEnv(envHost); !ok {
		return
	}
	if username, ok = os.LookupEnv(envUsername); !ok {
		return
	}
	if clientCertFile, ok = os.LookupEnv(envClientCertFile); !ok {
		return
	}
	clientKeyFile, ok = os.LookupEnv(envClientKeyFile)
	return
}

func jwtAuthPrms() (host, username, token string, ok bool) {
	if host, ok = os.LookupEnv(envHost); !ok {
		return
	}
	if username, ok = os.LookupEnv(envUsername); !ok {
		return
	}
	token, ok = os.LookupEnv(envToken)
	return
}

func tlsPrms() (serverName string, insecureSkipVerify bool, rootCAFile string, prmExist bool) {
	prmExist = false // returns true in case any of the TLS parameter is set.
	var ok bool

	if serverName, ok = os.LookupEnv(envTLSServerName); ok {
		prmExist = true
	}

	if insecureSkipVerifyValue, ok := os.LookupEnv(envTLSInsecureSkipVerify); ok {
		var err error
		if insecureSkipVerify, err = strconv.ParseBool(insecureSkipVerifyValue); err == nil {
			prmExist = true
		}
	}

	if rootCAFile, ok = os.LookupEnv(envTLSRootCAFile); ok {
		prmExist = true
	}
	return
}

// ExampleConnector_basicAuth shows how to open a database with the help of a connector using basic authentication.
func ExampleConnector_basicAuth() {
	fmt.Print("ok") // execute example as test

	host, username, password, ok := basicAuthPrms()
	if !ok {
		log.Print("not all basic authorization parameters set")
		return
	}

	connector := driver.NewBasicAuthConnector(host, username, password)
	if serverName, insecureSkipVerify, rootCAFile, ok := tlsPrms(); ok {
		connector.SetTLS(serverName, insecureSkipVerify, rootCAFile)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	// output: ok
}

// ExampleConnector_x509Auth shows how to open a database with the help of a connector using x509 (client certificate) authentication.
func ExampleConnector_x509Auth() {
	fmt.Print("ok") // execute example as test

	host, username, clientCertFile, clientKeyFile, ok := x509AuthPrms()
	if !ok {
		log.Print("not all client certificate authorization parameters set")
		return
	}

	connector := driver.NewX509AuthConnector(host, username, clientCertFile, clientKeyFile)
	if serverName, insecureSkipVerify, rootCAFile, ok := tlsPrms(); ok {
		connector.SetTLS(serverName, insecureSkipVerify, rootCAFile)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	// output: ok
}

// ExampleConnector_jwtAuth shows how to open a database with the help of a connector using JWT authentication.
func ExampleConnector_jwtAuth() {
	fmt.Print("ok") // execute example as test

	host, username, token, ok := jwtAuthPrms()
	if !ok {
		log.Print("not all token authorization parameters set")
		return
	}

	connector := driver.NewJWTAuthConnector(host, username, token)
	if serverName, insecureSkipVerify, rootCAFile, ok := tlsPrms(); ok {
		connector.SetTLS(serverName, insecureSkipVerify, rootCAFile)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}

	// output: ok
}
