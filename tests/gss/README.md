# GSS (Kerberos) Authentication Test

Tests Kerberos authentication via GSS-API against SAP HANA. See
[../README.md](../README.md) for the common prerequisites (a container engine
only), the run model, Podman notes, and the HANA startup caveat.

The test builds the Linux `libgssapi_krb5` reference initiator (`driver/gss`,
tags `gss gssapi`) and runs inside the `runner` container, so the host needs no
Go or Kerberos tooling.

## Services

`docker-compose.yml` defines, on a shared `gss_network`:

- `kdc` — an MIT Kerberos KDC (realm `EXAMPLE.TEST`), set up by `kdcInit.sh`. It
  creates the client principal `gssuser@EXAMPLE.TEST` (password `GssUser1234`)
  and the service principal `hdb/hana@EXAMPLE.TEST`, and exports the service
  keytab to the shared `gss_keytab` volume as `krb5_hdb.keytab`.
- `hana` — SAP HANA Express, with the keytab volume mounted at `/keytab`.
- `runner` — obtains a ticket (`kinit`) and runs the test.

## Steps

```bash
cd tests/gss

# 1. start the KDC and HANA
docker compose up -d kdc hana
#    wait for "Startup finished!" in: docker compose logs hana

# 2. enable Kerberos on HANA: install the keytab and restart the instance
docker compose exec hana bash /hanaKrbSetup.sh

# 3. run the test (maps the principal to a HANA user, then authenticates by GSS)
docker compose --profile runner run --rm runner

# 4. tear down
docker compose down -v
```

Step 2 installs `krb5_hdb.keytab` + `krb5_hdb.conf` into the `<sid>adm` home
`etc` directory and restarts HANA so it reads them. The full HANA-side Kerberos
configuration is documented in **SAP Note 1837331** (*Single Sign-On with SAP
HANA Database using Kerberos*); it is Active-Directory-oriented but the
HANA-side steps apply to an MIT KDC. The realm must match exactly
(case-sensitive) across the keytab, `krb5_hdb.conf`, and the user's external
identity.

The user-to-principal mapping is created by the test as SYSTEM:
`CREATE USER GSSUSER WITH IDENTITY 'gssuser@EXAMPLE.TEST' FOR KERBEROS`; it then
connects with `ConnectorConfig.GSS = gss.NewInitiator` and
`AuthMethods = driver.AuthGSS` (no username or password) and asserts
`current_user` is `GSSUSER`.

## Scope

The reference initiator and this test exercise single-round raw Kerberos V5 on
Linux. SPNEGO / multi-round and non-Linux platforms are supported by the
`gss.Initiator` interface but are the application's responsibility (bring your
own implementation); they are not exercised here.
