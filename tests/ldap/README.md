# LDAP Authentication Test

Tests LDAP authentication against SAP HANA. See [../README.md](../README.md) for
the common prerequisites (a container engine only), the run model, Podman notes,
and the HANA startup caveat.

The test runs inside the `runner` container, so the host needs no Go toolchain.

## Services

`docker-compose.yml` defines, on a shared `ldap_network`:

- `ldap` — OpenLDAP (`osixia/openldap`), configured via the `OPENLDAP_BOOTSTRAP_*`
  environment: suffix `dc=example,dc=com`, admin `cn=admin,dc=example,dc=com` /
  `admin1234` (set as a pre-hashed password), the memberof overlay enabled, and
  listening on 3890 (the image runs unprivileged and cannot bind 389).
- `hana` — SAP HANA Express.
- `runner` — runs the test.

## Steps

```bash
cd tests/ldap

# 1. start OpenLDAP and HANA
docker compose up -d ldap hana
#    wait for "Startup finished!" in: docker compose logs hana

# 2. populate the LDAP directory (users, group; memberof populates memberOf)
docker compose exec ldap /ldapConfig.sh

# 3. run the test (creates the LDAP provider + user on HANA, then authenticates)
docker compose --profile runner run --rm runner

# 4. tear down
docker compose down -v
```

`ldapConfig.sh` administers OpenLDAP over the local `ldapi:///` socket with SASL
EXTERNAL (no TCP port or admin password needed), adding `ldapuser` and the
`groupOfNames` group `hanausers`. The memberof overlay then gives `ldapuser` a
`memberOf` attribute, which HANA uses to map the LDAP group to a role.

The test, as SYSTEM, creates an LDAP provider pointing at `openldap:3890`, a role
mapped to the LDAP group, and a HANA user identified externally as the LDAP
user; it then connects as that user over LDAP and asserts `current_user`.

Because the test connects to HANA without TLS, it opts into offering LDAP on an
unverified connection via `ConnectorConfig.AllowLDAPWithoutVerifiedTLS`.
Production deployments should rely on verified TLS instead.
