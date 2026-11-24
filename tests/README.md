# Integration tests

These packages test go-hdb against a real SAP HANA database. They come in two
shapes:

- **Self-contained container rigs** — `ldap` and `gss` bring up HANA and the
  auxiliary service an authentication method needs (an OpenLDAP server, a
  Kerberos KDC) via a `docker-compose.yml`, and run the test in a `runner`
  container. The only host requirement is a container engine.
- **DSN-based host tests** — `x509` runs against an existing HANA instance given
  by `GOHDBDSN` and needs a host tool (`openssl`); it uses no containers. See
  `x509/README.md`.

Each package's README has the exact steps; this document covers what the two
container rigs (`ldap`, `gss`) share.

## Container rigs: prerequisite is a container engine only

For `ldap` and `gss` the only thing required on the host is **Docker (with the
Compose plugin) or Podman (with `podman-compose`)**. Go, the Kerberos/LDAP
client libraries, and everything else live inside the containers — each rig has
a `runner` service that builds the module and runs `go test`, so the host needs
no Go toolchain.

Commands below use `docker compose`; for Podman substitute `podman-compose`
(see [Podman notes](#podman-notes)).

## How a container rig runs

Each rig has three kinds of service:

- the **auxiliary service** (`ldap` / `kdc`) — configured on start,
- **`hana`** — SAP HANA Express,
- **`runner`** — builds and runs the test; behind the `runner` compose profile,
  so a plain `up` does not start it.

The common flow (exact per-test steps are in each README):

```bash
cd tests/<name>
docker compose up -d                 # start the auxiliary service + hana
# ... wait for HANA (see below), run the rig's one-time configuration ...
docker compose --profile runner run --rm runner   # build + run the test
docker compose down -v               # tear everything down
```

The `runner` connects to the other services by their network name (`hana`,
`openldap`, `kdc`), so it needs no published ports and no host configuration.

## Teardown and purge

`down -v` stops the containers and removes the rig's named volumes (the Kerberos
keytab volume, any LDAP data):

```bash
cd tests/<name>
docker compose --profile runner down -v
```

Include `--profile runner` so the profiled `runner` service is torn down too.
This leaves the built images behind (HANA Express is large); remove them
explicitly when you want the space back:

```bash
docker image rm <name>-runner        # the runner image built for this rig
docker image rm docker.io/saplabs/hanaexpress:latest   # ~optional, large
```

A full reset of all test state on a host (both engines):

```bash
docker compose --profile runner down -v     # in each tests/<name> you started
docker container prune -f
docker volume prune -f
# podman: same commands with `podman` / `podman-compose`
```

## HANA takes time to start

HANA Express needs several minutes to initialize on first start; wait for
`Startup finished!` in `docker compose logs hana`. HANA also reports its
processes up **before** the SQL port (39017) accepts connections — the `runner`
entrypoints wait for the port, so nothing is needed when using the runner.

## Podman notes

The container rigs run under Podman as well as Docker. Two Podman-specific
points:

- **Fully qualified image names** — the compose files use `docker.io/...`
  because Podman does not assume Docker Hub for short names. Docker accepts the
  qualified names too, so this is engine-neutral.
- **Container egress when Docker is also installed** — a host that has Docker
  installed alongside Podman can leave the iptables `FORWARD` chain policy at
  `DROP` (Docker manages forwarding via its own chains and can leave the policy
  dropped when its daemon is stopped), which starves Podman containers of
  outbound network — the `kdc`/`ldap` images then cannot `apt-get` and fail to
  start. If a Podman rig's auxiliary container has no internet, check
  `sudo iptables -S FORWARD`; allow forwarding with
  `sudo iptables -P FORWARD ACCEPT` (restarting the Docker daemon also restores
  its normal `ACCEPT` + chain setup). A Podman-only host does not hit this.

## Build tags

The tests are guarded by build tags so they are excluded from normal builds:
`x509`, `ldap`, and `gss gssapi` for GSS (the latter also builds the cgo
`libgssapi_krb5` reference initiator). The `ldap` and `gss` container runners
pass the tags; the host-run `x509` test needs them passed to `go test`.
