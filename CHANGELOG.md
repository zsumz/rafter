# Changelog

Notable Rafter changes, newest first. Entries describe what a consumer of the
published crates or a contributor to this repository would observe; the
release mechanics and exact publish lists live in [RELEASE.md](./RELEASE.md).

## Unreleased

- The repository's architecture is now executable policy: a zrail contract
  (`zrail.toml`, lock-bound) declares the layer graph, sans-IO source scopes,
  capability owners, qualification gates, and per-file shrink-only size
  ratchets, checked on every pull request beside the existing guard tests.
  Macro expansion is part of that surface: every macro invocation in the
  workspace binds one of the contract's named, provenance-pinned allowances,
  so an unreviewed macro fails the pull-request gate.
- One command qualifies a checkout: `zcheck` runs the pull-request-tier local
  gates through the task graph in `zcheck.toml`, with logs and durable
  receipts; `zcheck run full` adds the locally runnable deeper evidence lanes.
- A repository-wide readability campaign brought every splittable production
  file to its 300-line target (crate roots to declarative facades), gave every
  module an architectural `//!` contract — now enforced across all crates by
  the readability guard — and added `docs/architecture.md`, the embedder's
  tour of the layer machine.
- Wall-clock process-evidence tests are Linux-authoritative in the suite:
  they run unchanged in Linux CI and are ignored by default on other hosts,
  where loaded-machine timing inverts them.

## 0.0.2-alpha.1 — 2026-08

- First coordinated prerelease of the current product graph: publishes
  `rafter-transport-tls`, the authenticated production transport, alongside
  the ten-crate embedding family it compiles against. Every Rafter dependency
  inside the family is exact-pinned to `=0.0.2-alpha.1`, so a package archive
  cannot mix registry generations.
- The kernel's simulation self-check became the documented public
  `Node::validate_derived_state`; the hidden `internal-test-hooks` feature is
  gone, and `rafter` declares no features at all.
- Public API, wire formats, storage formats, and operational contracts remain
  explicitly alpha.

## 0.0.1 — 2026-08

- Initial published family: the deterministic sans-IO kernel (`rafter`), the
  persist-before-output runtime boundary and durable node
  (`rafter-runtime-api`, `rafter-runtime`, `rafter-storage`, `rafter-crc32`),
  the embedded application layer (`rafter-app`), async service handles and
  transport traits (`rafter-service`), the many-group host
  (`rafter-multiraft`), the peer wire format (`rafter-codec`), and the
  deliberately insecure development transport
  (`rafter-transport-tcp-insecure`).
