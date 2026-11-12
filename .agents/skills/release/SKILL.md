---
name: release
description: Release procedure for go-hdb. Use when cutting a release or upgrading the supported Go version.
---

This document is the standing procedure for cutting a go-hdb release. Read it
in full, then execute the steps for the release type at hand.

## Standard Release
- Update `DriverVersion` in `driver/driver.go`.
- Update `prometheus/go.mod` version reference.
- Update `RELEASENOTES.md`.
- Run `make`.
- Commit and tag.

## Go Version Upgrade (additional steps)
- Update `go` and `toolchain` directives in `go.mod` and `prometheus/go.mod`.
- Update minimum supported Go version in `Makefile` (`GOTOOLCHAIN` line and `go` install target) and `prometheus/Makefile` (`GOTOOLCHAIN` line).
- Update `.github/workflows/build.yml` Go version matrix.
- Drop version-gated compatibility files if the minimum version floor rises past them.
- Update `RELEASENOTES.md` to document the new Go version support.

## RELEASENOTES.md structure
- A minor release gets its own `## vMAJOR.MINOR.0` heading with sectioned entries (new features, incompatible changes, etc.).
- A patch release is not a new heading: it is a `#### vMAJOR.MINOR.PATCH` bullet-list entry under the `### Minor revisions` subsection of its `vMAJOR.MINOR.0` heading.

## Deprecation flip releases
- Check whether the release being cut coincides with the **flip release** of any `go_hdb_compat_go1_NN` bundle (i.e. a Go 1.NN is now current). At the flip, the bundle's deprecated API is excluded by default, so a user upgrading their Go toolchain can hit a bare `undefined: <symbol>` with no further hint (the compiler error cannot name the tag).
- When cutting such a release, add an `### Incompatible changes` entry stating that upgrading to Go 1.NN removes the deprecated API by default and that `-tags go_hdb_compat_go1_NN` restores it for that one release. See `docs/DEPRECATIONS.md`.
