# go-hdb
[![Go Reference](https://pkg.go.dev/badge/github.com/SAP/go-hdb/driver.svg)](https://pkg.go.dev/github.com/SAP/go-hdb/driver)
[![REUSE status](https://api.reuse.software/badge/github.com/SAP/go-hdb)](https://api.reuse.software/info/github.com/SAP/go-hdb)
[![build](https://github.com/SAP/go-hdb/actions/workflows/build.yml/badge.svg)](https://github.com/SAP/go-hdb/actions/workflows/build.yml)
[![release](https://img.shields.io/github/v/release/SAP/go-hdb)](https://github.com/SAP/go-hdb/releases/latest)

Go-hdb is a native Go (Golang) HANA database driver for Go's [database/sql](https://pkg.go.dev/database/sql) package. It implements the "SAP HANA SQL Command Network Protocol" directly, so it is **pure Go with no cgo and no HANA client library dependency** — a single statically linked binary, cross-compilable, with driver upgrades decoupled from the database version.

For the official SAP HANA client Go support (not this database driver), please see [SAP Help Portal](https://help.sap.com/docs/SAP_HANA_CLIENT).

## Features

* Compliant with the Go [database/sql](https://golang.org/pkg/database/sql) package.
* UTF-8 to/from CESU-8 encoding for HANA Unicode types.
* HANA decimals as:
  * Go rational numbers via [math/big](http://golang.org/pkg/math/big) ([example](https://pkg.go.dev/github.com/SAP/go-hdb/driver#example-Decimal)).
  * custom decimal types via the [database/sql](https://golang.org/pkg/database/sql) decimal decompose / compose interfaces ([example](https://pkg.go.dev/github.com/SAP/go-hdb/driver#example-package-CustomDecimal)).
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
* [LZ4 compression](driver/compress/README.md) support.

## Installation

```
go get -u github.com/SAP/go-hdb/driver
```

## Quickstart

```go
package main

import (
	"database/sql"
	"fmt"

	"github.com/SAP/go-hdb/driver"
)

func main() {
	connector := driver.NewBasicAuthConnector("host:port", "user", "password")

	db := sql.OpenDB(connector)
	defer db.Close()

	var name string
	if err := db.QueryRow("select current_user from dummy").Scan(&name); err != nil {
		panic(err)
	}
	fmt.Println(name)
}
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

### CPU Profiling

Integration tests include network I/O which dominates wall time and obscures driver CPU cost. To profile only the driver code, collect a CPU profile and then filter out database-tagged samples with `tagignore=db`:

```
go test -v -test.cpuprofile cpu.out
go tool pprof cpu.out
```

```
(pprof) tagignore=db
(pprof) top 10
```

The `tagignore=db` filter excludes samples tagged with database activity (network, syscalls waiting on the server), leaving only driver-side CPU work visible. To further exclude CESU-8 encoding/decoding overhead, add `tagignore=cesu8`:

```
(pprof) tagignore=db,cesu8
(pprof) top 10
```

## Performance

* For diagnosing latency and tuning throughput, see the [performance guide](PERFORMANCE.md).

## Dependencies

* Please see [go.mod](https://github.com/SAP/go-hdb/blob/main/go.mod).

## Licensing

SAP SE or an SAP affiliate company and go-hdb contributors. Please see our [LICENSE](LICENSE.md) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/SAP/go-hdb).

## AI Assistance

This project integrates AI into its development practices; contributors are expected to review and take ownership of all AI-assisted changes. Its design philosophy and development guidelines are documented in [AGENTS.md](AGENTS.md).
