# Deprecation and removal policy

This document explains how go-hdb deprecates and eventually removes API and
features, and how you can prepare your code for each removal ahead of time.

## Support window

go-hdb follows the Go release policy. Go issues two releases a year and
supports the current release and the one before it. go-hdb does the same: at
any time the driver supports the **current Go release and the previous one**.

Deprecations are therefore timed to the Go release cadence, **not** to go-hdb's
own version numbers. As the supported-Go window advances, deprecated code moves
through its lifecycle and is eventually dropped. Upgrading Go on the normal
N → N+1 cadence takes you through every stage — the opt-out self-check while a
symbol is still present, then one opt-in release before removal. Skipping a Go
release skips the opt-in window with it.

## How deprecated code is phased out

Deprecated code is isolated in its own file or files, each guarded by a build
tag of the form

    go_hdb_compat_go1_NN

where `go1_NN` names the Go release at which that bundle **flips** from
compiled-by-default to opt-in (the dot in the Go version is written as an
underscore, because build tags may not contain dots). Each bundle carries one
such tag, and its meaning is resolved by **combining it with Go's own release
build tags** (`go1.28`, `go1.29`, …), which the toolchain sets automatically.

A bundle that flips at Go 1.29 — tag `go_hdb_compat_go1_29` — passes through
three stages:

| Go release        | Tag not set (default) | Tag set (`-tags go_hdb_compat_go1_29`) |
| ----------------- | --------------------- | -------------------------------------- |
| **before 1.29**   | compiled              | **excluded**                           |
| **1.29**          | excluded              | **included**                           |
| **1.30 onward**   | removed from the codebase                                  |

### Default-on: opt-out self-check

On every release **before** the flip, the deprecated code is compiled by
default; you need not set anything and your build is unaffected. This spans the
full support window — both the current and the previous supported Go release —
so the code stays present throughout.

Setting the tag **excludes** the code, so a build with the tag set fails if —
and only if — your code still depends on something deprecated. This is a
deliberate self-check: run your build or tests with the tag set, in CI or
locally, to find out whether you are ready for the code to go away, while it is
still present by default.

### At the flip release: opt-in escape hatch

At the flip release the default reverses. The deprecated code is now
**excluded** by default, so code that still depends on it no longer builds.
Setting the tag **includes** the code again — an escape hatch for anyone who
has not yet migrated, buying one more release to do so.

> **Upgrading your Go toolchain can remove deprecated go-hdb API even if you do
> not change go-hdb.** Because the lifecycle is keyed to the Go release, a build
> that worked on Go 1.N may fail on Go 1.(N+1) with a bare `undefined: <symbol>`
> and no further hint. If that happens, build with
> `-tags go_hdb_compat_go1_<N+1>` to restore the symbol for that one release
> while you migrate off it (see [Checking your code](#checking-your-code) for
> the exact command), then remove the deprecated usage before the following
> release deletes it for good.

### After the flip release: removal

From the next release onward the bundle no longer compiles at all, and the
guarded files and their build tags are deleted from the repository. No tag
brings the code back.

Because the default-on stage covers the whole support window and the flip
release adds one more opt-in release on top, a deprecated symbol stays reachable
for at least one Go release after the self-check first reports it, and for one
further release behind the opt-in tag.

With two Go releases a year, that is roughly twelve months from the flip to
removal. The opt-in tag works for a single release only — on the removal release
the symbol is gone even with the tag set. Upgrading across both the flip and the
removal in one step (by skipping a Go release) leaves no opt-in window.

## The build constraint

A Go release tag is a **minimum**: `go1.29` is satisfied by Go 1.29 and every
later release, not by 1.29 alone. The lifecycle therefore pivots on a single
release — the flip release `go1.F` — and needs only that one number. A bundle
that flips at `go1.F` is compiled when

    (!go1.F && !go_hdb_compat_go1_F) || (go1.F && !go1.(F+1) && go_hdb_compat_go1_F)

holds:

- `!go1.F && !go_hdb_compat_go1_F` — compiled by default on every release before
  the flip. This single lower-unbounded term covers both supported releases
  during the default-on stage; there is no need to enumerate them. Setting the
  tag drops this term, which is the self-check.
- `go1.F && !go1.(F+1) && go_hdb_compat_go1_F` — on the flip release only, and
  only when the tag is set: the opt-in escape hatch.
- From `go1.(F+1)` onward no term matches, so the code is inert even before the
  file is deleted.

Concretely, a bundle flipping at Go 1.29:

    //go:build (!go1.29 && !go_hdb_compat_go1_29) || (go1.29 && !go1.30 && go_hdb_compat_go1_29)

## Checking your code

To find out whether your code still uses anything deprecated in a given cycle,
build or test with that bundle's tag set, for example:

    go build -tags go_hdb_compat_go1_29 ./...
    go test  -tags go_hdb_compat_go1_29 ./...

Before the flip release a clean build means you are ready for the removal. At
the flip release the same tag is instead what keeps your build working until you
have migrated.

## Multiple bundles

Each deprecation cycle introduces its own tag (`go_hdb_compat_go1_29`,
`go_hdb_compat_go1_30`, …), so several bundles can be in flight at once, each at
its own stage. Set the tags for the cycles you still depend on; each retires
independently as its code is removed.

The current deprecated API across all bundles — each symbol, its replacement,
and the release it is removed in — is listed in the generated
[deprecated surface](DEPRECATED.md).
