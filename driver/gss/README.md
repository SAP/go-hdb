# GSS (Kerberos) authentication

go-hdb can authenticate to a HANA database with Kerberos via the GSS-API. The
user authenticates once to a Key Distribution Center (KDC) at login (`kinit`, or
a Windows domain logon) and obtains a ticket; the driver presents a service
ticket for the database, and HANA maps the Kerberos principal to a database
user. No password travels on the wire.

## Design goals

go-hdb's default build is **pure Go, with no CGO and no third-party runtime
dependencies**. Producing a Kerberos ticket is in tension with this: it requires
a GSS library — the system `libgssapi_krb5` (Linux/macOS) or SSPI (Windows) — and
go-hdb does not want to pull such a dependency, or bless one particular
third-party Kerberos package, onto every consumer whether or not they use
Kerberos.

So go-hdb ships **no built-in GSS mechanism** in its default build. It defines a
small [`Initiator`](#the-initiator-interface) interface and lets the application
**bring its own implementation** wrapping whatever GSS library fits its platform
and dependency requirements. If no initiator is configured, the driver behaves
exactly as before — no Kerberos, no extra dependencies. A CGO
[reference implementation](#reference-implementation) for Linux is included but
gated behind a build tag, so it never reaches the default build.

The driver itself does **no** Kerberos cryptography. It speaks the HANA
authentication protocol and shuttles opaque GSS tokens between the server and the
application's `Initiator`.

## The `Initiator` interface

```go
type Initiator interface {
    SrcName() string
    InitSecContext(ctx context.Context, targetName string, inputToken []byte) (outputToken []byte, established bool, err error)
}
```

- The application sets `ConnectorConfig.GSS` to a `func() (Initiator, error)`; the
  driver calls it **once per connection**, so each connection owns its own GSS
  security context. There is no shared mutable state, hence no locking
  requirement on the application.
- `SrcName` returns the client principal (`user@REALM`).
- `InitSecContext` advances the context and is called **in a loop** until
  `established` is true: `inputToken` is `nil` on the first call and the
  server's latest token thereafter, `outputToken` is sent to the server. Raw
  Kerberos needs one round beyond the first; SPNEGO may need several.

### Mapping to the native GSS API

The interface uses GSS-API vocabulary so the mapping to the native call is
direct on every platform:

| `Initiator`        | Unix GSS (`gss_init_sec_context`) | Windows SSPI (`InitializeSecurityContext`) |
|--------------------|-----------------------------------|--------------------------------------------|
| `InitSecContext`   | `gss_init_sec_context`            | `InitializeSecurityContext`                |
| `targetName`       | `target_name`                     | `pszTargetName`                            |
| `inputToken` (in)  | `input_token` (empty first call)  | `pInput`                                   |
| `outputToken` (out)| `output_token`                    | `pOutput`                                  |
| `established`      | `GSS_S_COMPLETE` vs `GSS_S_CONTINUE_NEEDED` | `SEC_E_OK` vs `SEC_I_CONTINUE_NEEDED` |
| `SrcName`          | `gss_inquire_cred` / `gss_display_name` | `QueryCredentialsAttributes` (names) |

The credential (for delegation) and the request flags (mutual auth, integrity,
confidentiality, replay) are internal to the implementation — the driver has no
surface for them.

## Usage

Enable GSS on the connector and supply an `Initiator` factory:

```go
cfg := driver.NewConnectorConfig()
cfg.Host = "hana:39017"
cfg.AuthMethods = driver.AuthGSS // offer GSS (omit to offer all methods)
cfg.GSS = gss.NewInitiator       // or your own func() (gss.Initiator, error)
```

`gss.NewInitiator` is the tag-gated Linux reference (build with `gssapi`); for
other platforms supply your own factory. No username or password is set: the
server maps the Kerberos principal to a database user. On success HANA may
return a session cookie, which the driver stores and replays for fast reconnect
— handled automatically, no caller action.

## Security

- **Request mutual authentication.** An implementation MUST request mutual
  authentication (and integrity and replay detection) when it builds the
  context. With mutual auth the server proves its identity with an AP-REP that
  only the holder of the service keytab can produce; without it the server is
  not authenticated. Unlike LDAP, the driver cannot enforce this — the flags
  live in the application's implementation — so it is the implementation's
  responsibility.
- **GSS authenticates, it does not encrypt the SQL traffic.** Kerberos secures
  the login only. As with every other method, use TLS for confidentiality of the
  data on the wire.

## Delegation

A middle-tier service can connect to HANA as the end user (constrained
delegation) by having its `Initiator` use a forwarded credential — it passes
that credential's handle to the native call instead of the default ticket cache.
This needs no driver support: the application's implementation owns which
credential it uses.

## Reference implementation

The package includes a reference `Initiator` for Linux backed by the system
`libgssapi_krb5` (MIT Kerberos), guarded by the `gssapi` build tag
(`gss.NewInitiator`), so neither CGO nor `libgssapi` are dependencies of the
default build. It requests mutual authentication, integrity, confidentiality and
replay detection, and uses raw Kerberos V5 (not SPNEGO).

SPNEGO and other platforms (Windows SSPI, macOS) are **bring-your-own**: the
interface supports them — SPNEGO is just more `InitSecContext` rounds — but go-hdb
ships only the raw-Kerberos Linux reference. For production, provide an
`Initiator` that fits your platform, dependency and security requirements.
