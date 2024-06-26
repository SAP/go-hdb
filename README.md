# go-hdb
[![Go Reference](https://pkg.go.dev/badge/github.com/SAP/go-hdb/driver.svg)](https://pkg.go.dev/github.com/SAP/go-hdb/driver)
[![Go Report Card](https://goreportcard.com/badge/github.com/SAP/go-hdb)](https://goreportcard.com/report/github.com/SAP/go-hdb)
[![REUSE status](https://api.reuse.software/badge/github.com/SAP/go-hdb)](https://api.reuse.software/info/github.com/SAP/go-hdb)
![](https://github.com/SAP/go-hdb/workflows/build/badge.svg)

Go-hdb is a native Go (golang) HANA database driver for Go's sql package. It implements the SAP HANA SQL command network protocol.

For the official SAP HANA client Go support (not this database driver) please see [SAP Help Portal](https://help.sap.com/docs/SAP_HANA_PLATFORM/0eec0d68141541d1b07893a39944924e/0ffbe86c9d9f44338441829c6bee15e6.html).

## Installation

```
go get -u github.com/SAP/go-hdb/driver
```

## Building

To build go-hdb you need to have a working Go environment of the [latest or second latest Go version](https://golang.org/dl/).

## Documentation

API documentation and documented examples can be found at <https://pkg.go.dev/github.com/SAP/go-hdb/driver>.

## HANA cloud connection

HANA cloud connection proxy is using SNI which does require a TLS connection.
As default one can rely on the root certificate set provided by the host, which already comes with the nessecary
DigiCert certificates (CA, G5).
For more information on [Go](https://go.dev/) tls certificate handling, please see https://pkg.go.dev/crypto/tls#Config.

Assuming the HANA cloud 'endpoint' is "something.hanacloud.ondemand.com:443". Then the dsn should look as follows:

```
"hdb://<USER>:<PASSWORD>@something.hanacloud.ondemand.com:443?TLSServerName=something.hanacloud.ondemand.com"
```

with:
- TLSServerName same as 'host'

## Specific root certificate
In case a specific root certificate (e.g. self-signed) would be needed, the TLSRootCAFile DSN parameter needs to
point to the location in the filesystem where a root certificate file in 'pem-format' is stored.

## Tests

To run the driver integration tests a HANA Database server is required. The test user must have privileges to create database schemas.

Set environment variable GOHDBDSN:

```
#linux example
export GOHDBDSN="hdb://user:password@host:port"
go test
```

Using the Go build tag 'unit' only the driver unit tests will be executed (no HANA Database server required):

```
go test --tags unit
```

## Features

* Native Go implementation (no C libraries, CGO).
* Go <http://golang.org/pkg/database/sql> package compliant.
* Support of database/sql/driver Execer and Queryer interface for parameter free statements and queries.
* Support of 'bulk' query execution.
* Support of UTF-8 to / from CESU-8 encodings for HANA Unicode types.
* Built-in support of HANA decimals as Go rational numbers <http://golang.org/pkg/math/big>.
* Support of Large Object streaming.
* Support of Stored Procedures with table output parameters.
* Support of TLS TCP connections.
* Support of little-endian (e.g. amd64) and big-endian architectures (e.g. s390x).
* Support of [driver connector](https://golang.org/pkg/database/sql/driver/#Connector).
* Support of [PBKDF2](https://tools.ietf.org/html/rfc2898) authentication as default and standard user / password as fallback.
* Support of client certificate (X509) and JWT (JSON Web Token) authentication.
* [Prometheus](https://prometheus.io) collectors for driver and extended database statistics.
* Support of [scanning database rows into go structs](https://pkg.go.dev/github.com/SAP/go-hdb/driver#StructScanner).

## Dependencies

* Please see [go.mod](https://github.com/SAP/go-hdb/blob/main/go.mod).

## Licensing

SAP SE or an SAP affiliate company and go-hdb contributors. Please see our [LICENSE](LICENSE.md) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/SAP/go-hdb).
