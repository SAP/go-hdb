# go-hdb
[![Go Reference](https://pkg.go.dev/badge/github.com/SAP/go-hdb/driver.svg)](https://pkg.go.dev/github.com/SAP/go-hdb/driver)
[![Go Report Card](https://goreportcard.com/badge/github.com/SAP/go-hdb)](https://goreportcard.com/report/github.com/SAP/go-hdb)
[![REUSE status](https://api.reuse.software/badge/github.com/SAP/go-hdb)](https://api.reuse.software/info/github.com/SAP/go-hdb)
![](https://github.com/SAP/go-hdb/workflows/build/badge.svg)

Go-hdb is a native Go (Golang) HANA database driver for Go's SQL package. It implements the "SAP HANA SQL Command Network Protocol".

For the official SAP HANA client Go support (not this database driver), please see [SAP Help Portal](https://help.sap.com/docs/SAP_HANA_CLIENT).

## Installation

```
go get -u github.com/SAP/go-hdb/driver
```

## Building

To build go-hdb, a working Go environment of the [latest or second latest Go version](https://golang.org/dl/) is required.

## Documentation

API documentation and documented examples can be found at <https://pkg.go.dev/github.com/SAP/go-hdb/driver>.

## HANA Cloud Connection

The HANA cloud connection proxy uses SNI, which requires a TLS connection.
By default, one can rely on the root certificate set provided by the host, which already comes with the necessary
DigiCert certificates (CA, G5).
For more information on [Go](https://go.dev/) TLS certificate handling, please see https://pkg.go.dev/crypto/tls#Config.

Assuming the HANA cloud 'endpoint' is "something.hanacloud.ondemand.com:443", the DSN should look as follows:

```
"hdb://<USER>:<PASSWORD>@something.hanacloud.ondemand.com:443?TLSServerName=something.hanacloud.ondemand.com"
```

where:
- TLSServerName: same as 'host'

## Specific Root Certificate
If a specific root certificate (e.g. self-signed) is needed, the TLSRootCAFile DSN parameter must point to the location in the file system where the root certificate file in PEM format is stored.

## Tests

To run the driver integration tests, a HANA Database server is required. The test user must have privileges to create database schemas.

Set the environment variable GOHDBDSN:

```
#linux example
export GOHDBDSN="hdb://user:password@host:port"
go test
```

Using the Go build tag 'unit', only the driver unit tests will be executed (no HANA Database server required):

```
go test --tags unit
```

## Features

* Native Go implementation — no C libraries, no CGO.
* Compliant with the Go [database/sql](https://golang.org/pkg/database/sql) package.
* UTF-8 to/from CESU-8 encoding for HANA Unicode types.
* HANA decimals as Go rational numbers via [math/big](http://golang.org/pkg/math/big).
* Large Object streaming.
* 'Bulk' query execution.
* Stored Procedures with table output parameters.
* Parameter free statements and queries via Execer and Queryer interfaces.
* TLS TCP connections.
* Little-endian (e.g. amd64) and big-endian (e.g. s390x) architecture support.
* [Driver connector](https://golang.org/pkg/database/sql/driver/#Connector) interface.
* [PBKDF2](https://tools.ietf.org/html/rfc2898) authentication as default, standard user/password as fallback.
* LDAP, client certificate (X509) and JWT (JSON Web Token) authentication.
* [Prometheus](https://prometheus.io) collectors for driver and extended database statistics.
* [Scanning database rows into Go structs](https://pkg.go.dev/github.com/SAP/go-hdb/driver#StructScanner).

## Dependencies

* Please see [go.mod](https://github.com/SAP/go-hdb/blob/main/go.mod).

## Licensing

SAP SE or an SAP affiliate company and go-hdb contributors. Please see our [LICENSE](LICENSE.md) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/SAP/go-hdb).

## AI Assistance

This project integrates AI into its development practices. Contributors are expected to review and take ownership of all AI-assisted changes.
