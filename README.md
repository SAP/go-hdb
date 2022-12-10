# go-hdb
[![Go Reference](https://pkg.go.dev/badge/github.com/SAP/go-hdb/driver.svg)](https://pkg.go.dev/github.com/SAP/go-hdb/driver)
[![Go Report Card](https://goreportcard.com/badge/github.com/SAP/go-hdb)](https://goreportcard.com/report/github.com/SAP/go-hdb)
[![REUSE status](https://api.reuse.software/badge/github.com/SAP/go-hdb)](https://api.reuse.software/info/github.com/SAP/go-hdb)
![](https://github.com/SAP/go-hdb/workflows/build/badge.svg)

Go-hdb is a native Go (golang) HANA database driver for Go's sql package. It implements the SAP HANA SQL command network protocol.

For the official SAP HANA client Go support (not this database driver) please see [SAP Help Portal](https://help.sap.com/viewer/0eec0d68141541d1b07893a39944924e/2.0.02/en-US/0ffbe86c9d9f44338441829c6bee15e6.html).

## Version 1.0

go-hdb version 1.0 is going to be released after Go 1.20 is available which is expected to be released in February 2023. For migration details please refer to [Version 1.0](https://github.com/SAP/go-hdb/blob/main/VERSION1.0.md).

When starting newly with this driver please use the latest 1.0 release candidate branch of this repository

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
To connect successfully you would need a valid root certificate in pem format (please see
[SAP Help](https://help.sap.com/viewer/cc53ad464a57404b8d453bbadbc81ceb/Cloud/en-US/5bd9bcec690346a8b36df9161b1343c2.html)).

The certificate (DigiCertGlobalRootCA.crt.pem) can be downloaded in 'pem-format' from
[digicert](https://www.digicert.com/kb/digicert-root-certificates.htm).

Assuming the HANA cloud 'endpoint' is "something.hanacloud.ondemand.com:443". Then the dsn should look as follows:

```
"hdb://<USER>:<PASSWORD>@something.hanacloud.ondemand.com:443?TLSServerName=something.hanacloud.ondemand.com&TLSRootCAFile=<PATH TO CERTIFICATE>/DigiCertGlobalRootCA.crt.pem"
```

or

```
"hdb://<USER>:<PASSWORD>@something.hanacloud.ondemand.com:443?TLSServerName=something.hanacloud.ondemand.com"
```

with:
- TLSServerName same as 'host'
- TLSRootCAFile needs to point to the location in your filesystem where the file DigiCertGlobalRootCA.crt.pem is stored

When omitting the TLSRootCAFile, TLS uses the host's root CA set (for more information please see
[Config RootCAs](https://pkg.go.dev/crypto/tls#Config).

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

## Dependencies

* Please see [go.mod](https://github.com/SAP/go-hdb/blob/main/go.mod).

## Licensing

Copyright 2014-2022 SAP SE or an SAP affiliate company and go-hdb contributors. Please see our [LICENSE](LICENSE.md) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/SAP/go-hdb).
