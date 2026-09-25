# go-hdb

[![Go Reference](https://pkg.go.dev/badge/github.com/SAP/go-hdb/driver.svg)](https://pkg.go.dev/github.com/SAP/go-hdb/driver)
[![go-version](https://img.shields.io/github/go-mod/go-version/SAP/go-hdb)](https://github.com/SAP/go-hdb/blob/main/go.mod)
[![build](https://github.com/SAP/go-hdb/actions/workflows/build.yml/badge.svg)](https://github.com/SAP/go-hdb/actions/workflows/build.yml)
[![release](https://img.shields.io/github/v/release/SAP/go-hdb)](https://github.com/SAP/go-hdb/releases/latest)
[![license](https://img.shields.io/github/license/SAP/go-hdb)](LICENSE.md)
[![REUSE status](https://api.reuse.software/badge/github.com/SAP/go-hdb)](https://api.reuse.software/info/github.com/SAP/go-hdb)

Pure-Go driver for SAP HANA. No cgo, no client library - one static binary.
Speaks the HANA SQL Command Network Protocol directly. Built for [database/sql](https://pkg.go.dev/database/sql), [minimal dependencies](https://github.com/SAP/go-hdb/blob/main/go.mod).
Requires Go [latest or second latest version](https://golang.org/dl/).

```go
connector := driver.NewBasicAuthConnector("host:port", "user", "password")
db := sql.OpenDB(connector)

var user string
if err := db.QueryRow("select current_user from dummy").Scan(&user); err != nil {
	// handle error
}
```

> Need the official SAP HANA client Go support (not this driver)? See [SAP Help Portal](https://help.sap.com/docs/SAP_HANA_CLIENT).

## Install

```sh
go get github.com/SAP/go-hdb/driver
```

## Quickstart

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/SAP/go-hdb/driver"
)

func main() {
	connector := driver.NewBasicAuthConnector("host:port", "user", "password")

	db := sql.OpenDB(connector)
	defer db.Close()

	var user string
	if err := db.QueryRow("select current_user from dummy").Scan(&user); err != nil {
		log.Fatal(err)
	}
	fmt.Println(user)
}
```

> For HANA Cloud (SNI/TLS), see the [cloud connection guide](docs/HANACLOUD.md).

## Features

**Core**

- `database/sql` compliant, `driver.Connector` support
- TLS, PBKDF2 (default) with user/password fallback
- Auth: LDAP, X.509 client cert, JWT - with refresh callbacks
- Bulk execution, stored procedures with table output parameters
- Little-endian (e.g. amd64) and big-endian (e.g. s390x) architecture support

**Data**

- UTF-8 to/from CESU-8 for HANA Unicode types
- HANA decimals as:
  - Go rational numbers via `math/big` ([example](https://pkg.go.dev/github.com/SAP/go-hdb/driver#example-Decimal)).
  - Custom decompose/compose types ([example](https://pkg.go.dev/github.com/SAP/go-hdb/driver#example-package-CustomDecimal)).
- LOB streaming, scanning rows into structs (`driver.StructScanner`)
- [LZ4 compression](driver/compress/README.md)

**Observability**

- Driver stats via `NativeDriver().Stats()`
- Per-DB `ExStats()` (requires `driver.OpenDB`)
- [Prometheus collectors](https://github.com/SAP/go-hdb/tree/main/prometheus)
- Per-statement SQL trace via `slog`

## Performance

For diagnosing latency and tuning throughput, see the [performance guide](docs/PERFORMANCE.md).

## Documentation

- API + examples: [pkg.go.dev](https://pkg.go.dev/github.com/SAP/go-hdb/driver)
- Compression: [driver/compress/README.md](driver/compress/README.md)
- Changes: [RELEASENOTES.md](RELEASENOTES.md)

## Testing

To run the driver integration tests, a HANA database server is required. The test user must have privileges to create database schemas.

```sh
export GOHDBDSN="hdb://user:password@host:port"
go test ./...
```

Unit-only (no server):

```sh
go test --tags unit ./...
```

## License

SAP SE or an SAP affiliate company and go-hdb contributors. Please see our [LICENSE](LICENSE.md) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/SAP/go-hdb).

## AI Assistance

This project integrates AI into its development practices; contributors are expected to review and take ownership of all AI-assisted changes. Its design philosophy and development guidelines are documented in [AGENTS.md](AGENTS.md).
