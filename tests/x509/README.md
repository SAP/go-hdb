# X.509 Authentication Tests

This package tests X.509 client certificate authentication against a SAP HANA database. It covers multiple key types and certificate formats.

## Prerequisites

- A running SAP HANA database instance
- A HANA user with privileges to:
  - Create/drop X509 providers, certificates, and PSEs
  - Create/drop database users
- `openssl` installed

## Step 1: Generate Certificates

Certificate and key files are not included in the repository and must be generated before running the tests. Run the provided script from within the `tests/x509/` directory:

```bash
cd tests/x509
bash x509Config.sh
```

This generates a self-signed root CA (`rootCA.crt` / `rootCA.key`) and signs client certificates for all supported key types.

## Step 2: Set the DSN Environment Variable

The test requires the `GOHDBDSN` environment variable pointing to a HANA instance with an admin user who can configure X.509 authentication:

```bash
export GOHDBDSN="hdb://user:password@host:port"
```

## Step 3: Run the Tests

The tests use the `x509` build tag.

```bash
go test --tags x509
```
