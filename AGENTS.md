# go-hdb Development Guidelines

## Proverbs
- Use Go idiomatically. Let the problem and data drive the design.
- No imposed paradigms or methodologies.
- No ceremony. No abstraction for its own sake.
- Design by reasoning, not by convention.
- Correctness over cleverness.
- Solve exactly what was asked. No extra features, abstractions, or over-engineering.
- Evidence over assumption.
- Less is more. Code that doesn't exist can't be wrong.
- Standard library over third-party packages.
- No dependency for straightforward code.
- Data over objects.
- Integration tests are the contract.
- Bugs become tests.

## Rules
- Aim for excellence. Every contribution must meet this standard.
- Understand before you touch. No second-guessing without evidence.
- No explanations of Go fundamentals.
- Be thorough on the first pass. Every finding matters.
- Trace every line. Check every error path. Verify every range guard.
- Run the tests. Check edge cases. Trace actual values.
- Reproduce bugs with a failing test. Refactors must leave tests green.
- Prove concurrent access before flagging a data race. Trace both sides.
- Verify the concurrency model before flagging a mutex pattern.
- Verify semantics before flagging similar types as duplicates.
- Name alternatives before implementing. No silent picks.
- No edits without explicit approval. Show findings one by one.
- No silent changes or reformatting of surrounding code.
- Code must conform to go fmt, go vet, and godoc.
- Idiomatic Go names only. No Hungarian notation or invented conventions.
- Use US spelling throughout. Code, comments, and documentation.
- Only export identifiers used externally.

## Mandatory context (all agentic tools)
This file is always loaded; the skill files below are not. Before starting the
corresponding task, read the skill file in full and follow it -- never rely on
prior knowledge of its contents.

- Reviewing or rating the driver, protocol, commands, or tests, or judging
  repo quality: read `.agents/skills/repo-review/SKILL.md`.
- Cutting a release or upgrading the supported Go version: read
  `.agents/skills/release/SKILL.md`.
