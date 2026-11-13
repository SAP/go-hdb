# LDAP Authentication Tests

This package tests LDAP authentication against a SAP HANA database. Both a HANA instance and an OpenLDAP server are required. The provided `docker-compose.yml` sets up both services automatically.

## Prerequisites

- Docker and Docker Compose installed

## Services

The `docker-compose.yml` starts two containers on a shared `ldap_network`:

- OpenLDAP (osixia/openldap:latest)
- HANA Express (saplabs/hanaexpress:latest)

## Network Configuration

A network called `ldap_network` is defined in the Docker Compose file to allow communication between the services.

## Step 1: Start the Containers

Run from within the `tests/ldap/` directory:

```bash
cd tests/ldap
docker compose up -d
```

Wait until both containers are healthy before continuing. HANA Express can take several minutes to initialize.

## Step 2: Configure the LDAP Server

Once the containers are running, execute the LDAP configuration script inside the `ldap` container:

```bash
docker compose exec ldap /ldapConfig.sh
```

## Step 3: Run the Tests

The tests use the `ldap` build tag. Run from the `tests/ldap/` directory:

```bash
go test --tags ldap
```

By default the driver offers LDAP authentication only over verified TLS: LDAP
encrypts the password with a server-supplied key but does not authenticate the
server, so verified TLS is required to trust that key. Because this test
connects to HANA without TLS, it opts in via
`ConnectorConfig.AllowLDAPWithoutVerifiedTLS`. Production deployments should
rely on verified TLS rather than this opt-in.

## Step 4: Shut Down the Containers

```bash
docker compose down
```
