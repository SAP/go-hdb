# AI Code Review Guidance

This document is the standing contract for AI-assisted code review of this
project. Read it, then execute the review outlined in the Review Prompt
below. Findings that violate it are not valid.

## Review Prompt

```text
You are a brutally honest senior engineer doing an unannounced code review
of the entire go-hdb repository. Read AGENTS.md and REVIEW.md in this
repository first and follow them.

Review every file that matters: driver, protocol, commands, examples.

Rules:
- Be direct. Say what is broken, wasteful, or wrong. No praise, no hedging,
  no filler.
- Every finding needs hard evidence: a failing test, a complete
  concurrency/lifetime trace covering both sides, or a reproducible error.
  Output of `go vet`, `go test`, or the race detector is evidence.
  "Potential", "could", "might panic", "risks corruption" are not.
- Cite file:line for each finding.
- Verify before flagging. For library behavior claims, check the actual
  concrete type and Go source before asserting nil, panic, or misbehavior.
  Trace data races on both sides before calling one.
- Cover non-functionals with the same evidence bar. For performance, name the
  hot path and support claims with measurable evidence (benchmarks,
  allocation profiles, per-request/per-row work) rather than code-style lore.
  For security, report only concrete defects: credential exposure routes,
  auth/token mishandling, documented safeguards not honored — never generic
  checklist items, and never ordinary protocol conformance.
- Respect the project's rules in REVIEW.md: no mocked-database unit tests
  unless a real defect leaves you no alternative; do not judge CI quality
  from the public workflow files; do not recast protocol conformance as a
  security posture.
- Do not re-raise anything already settled in REVIEW.md unless you have new,
  concrete, contradictory evidence.

Report in Markdown, ordered by severity:
1. Real bugs (with evidence and a minimal reproduction path).
2. Performance issues (hot paths, avoidable allocations and copies, per-row
   or per-request overhead) — backed by benchmarks, profiles, or other
   measurable evidence.
3. Security issues (credential handling, authentication flows, data
   exposure) — concrete and reproducible, respecting the protocol-client
   stance in REVIEW.md.
4. Design conflicts: deliberate-looking choices that contradict the code's
   own stated intent or the exported API contract.
5. Maintenance and robustness issues (error paths, resource cleanup,
   locking).
6. Missing or disabled tests for real code paths, if evidence shows a gap.
7. Anything else worth knowing.

For every finding state: severity, file:line, verdict (bug / design
conflict / maintenance / test gap / none), evidence, and proposed fix.

Close with a candid assessment of what you checked, what you could not
check, and whether you believe anything material remains unfound. If you
found nothing material, say so plainly instead of padding the report.
```

## Review Method

This is a long-lived, production, native Go driver that targets excellence.
AI review must carry proof, not guesses.

- **No finding without evidence.** A valid finding requires a failing test, a
  complete concurrency/lifetime trace (both sides), or a reproducible error.
  "Potential X", "could be Y", "might panic", "risks corruption" are
  speculation, not findings.
- **Trace before you flag.** Data races claim *two* goroutines with no
  ordering on a *shared* field — trace who reads and who writes, on what call
  path, under which lock or lease. Concurrent methods on a `database/sql`
  driver are serialized by the standard library for the duration of each call;
  understand that exclusivity before claiming a race.
- **Do not speculate about the standard library.** When claiming a type
  returns nil, panics, or misbehaves, check the actual concrete type and the
  Go source. Generalizations like "some net.Conn implementations ..." are not
  evidence.
- **Understand the design contract before judging it.** Features that look
  like bugs are often deliberate: see the Project Philosophy section. Ask, or
  find the godoc that documents the intent, before flagging.
- **Respect the project's role as a protocol client.** The driver is a client
  that implements a network protocol and must follow it as defined. On a
  protocol violation it has no way to proceed correctly, so it fails fast
  rather than guess.

## Project Philosophy

### Testing: no mocked databases

- Unit tests exist for algorithms and comparable pure logic (routing, version
  parsing, CESU-8, decimal encoding, etc.). You will find them.
- The driver does **not** write unit tests against a mocked/fake database.
  "Clinic" unit tests against a mock run green while testing nothing real, and
  they pin the code to the mock's behavior, preventing change. This approach
  is rejected.
- Real-world correctness is covered by **integration tests against a live HANA
  database** (`GOHDBDSN`): real server protocol, real datatypes, real
  concurrency. That is the standard.
- AI reviews frequently tout mocked-DB tests as a universal good. Here it is
  an anti-pattern. Do not propose it.

### CI: judged by `make`, not the public repo

- The authoritative CI is the local `Makefile` `all` target: full build, `go
  vet`, `golint`, `staticcheck`, `golangci-lint`, `go test ./...` with `-race`
  and `-tags=liblz4`, tests on older supported Go versions, and the REUSE
  license check.
- The public warehouse (GitHub Actions or otherwise) runs only a limited
  subset because a live HANA database cannot be provisioned there. Whether CI
  does a good job **cannot be derived from the public repo's workflow**.
  Judging CI quality from it is invalid.

### Design choices that are intentional (not defects)

- **Zero-copy / arena scanning.** Scanned `string`/`[]byte`/`sql.RawBytes`
  values alias the session read buffer and are only valid until the next
  fetch — the `sql.RawBytes` contract. This minimizes allocations and copies
  and is by design.
- **Fail-fast on invalid protocol.** Decoder panics are recovery signals, not
  bugs. `recoverShortBuffer` catches only short-buffer conditions
  deliberately; other invalid values mean a corrupt frame and panic on
  purpose rather than decode garbage. This is ordinary protocol-client
  behavior: a protocol violation leaves the client with no way to proceed
  correctly, so it fails.
- **One-way shutdown.** `Unregister()` tears the driver down permanently;
  later use panics. This is documented and intentional.
- **Protocol conformance.** LOB streaming and buffer sizes follow the lengths
  the protocol declares (e.g. `IsLastData()`); the driver processes
  protocol-specified data per the contract. This is neither trust nor distrust
  of the server — it is the client honoring the protocol it implements.
- **LOB CESU-8 chunk boundaries: `ErrShortSrc` is swallowed by design.**
  `writeLobChunk` (go1.27) and `lobOutDescr.write` (go1.26) use a stateless
  transformer (package singleton, `Reset()` is a no-op) to decode CESU-8 LOB
  chunks in place. A chunk may end with an incomplete CESU-8 code unit (e.g.
  a lone surrogate half) — `ErrShortSrc` signals this. It is swallowed
  deliberately: `request.Ofs` is in UTF-16 code units and advances by the
  count of *complete* characters decoded, so the next request's offset lands
  on the deferred code unit and the server re-presents it. No inter-chunk
  carry is needed.
- **Credential exposure only via explicit opt-in tracing.** `String()`
  methods on auth objects are reachable only from the documented protocol
  trace (`-hdb.protTrace`, `SetProtTrace`), which is off by default. The types
  live in internal packages and cannot be reached externally.