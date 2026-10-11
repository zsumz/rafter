# Changelog

Notable Rafter changes, newest first. Entries describe what a consumer of the
published crates or a contributor to this repository would observe; the
release mechanics and exact publish lists live in [RELEASE.md](./RELEASE.md).

## Unreleased

- The default-off `rafter-service/durable` feature adds a uniquely owned durable
  service with cloneable bounded client senders and the existing `RaftHandle`
  API. Writes acknowledge durable application completion; local and quorum
  reads preserve application freshness floors. Cancellation, uncollected
  results, shutdown and ambiguous outcomes have explicit ownership contracts.
  It reuses the existing persistence/application workers and TLS transport.
- Ready durable queries retain their admission turn while the application view
  is busy, preventing queued writes from repeatedly displacing them. Canceling
  the query releases its turn; reads still awaiting quorum allow write admission.
- Durable drivers support application snapshot handoff, local snapshot
  maintenance, leadership transfer and bounded membership proposal admission.
  A membership receipt identifies a durable append; applications must still
  observe the exact configuration commit and manage authorization, peer fencing
  and retirement history. Automatic membership-plan execution is not included.
- Managed service admission now counts completed results until collection or
  drop, and canceled metrics watches release their wake registrations.
  Durable senders expose retained count/byte charges and cumulative admission
  refusals; owners expose existing opt-in persistence-worker diagnostics.
- Membership helpers correctly remove stable learners, finish promotion plans
  with `LeaveJoint`, and offer explicit voter-removal plans. Transport-backed
  membership calls return typed protocol rejections with leader hints.
  Existing numeric write/read/transfer error-kind values remain stable.
- Storage recovery bounds retained metadata and rejects malformed WAL counts
  and truncated checkpoint lengths without eager allocations based on their
  claims. Checksums, error precedence and selected-file authority remain intact;
  these changes do not alter the version-1 storage formats.
- Retained-log replay grows frame buffers incrementally when a source ends
  before its earlier metadata length, preserving completed entries and the
  exact partial-frame error without preallocating the full claimed frame.
- The public transport requires patched rustls `0.23.45`; the invariant
  verifier's test-only quick-xml dependency moves to `0.41.0`. Public API checks
  compare default and all-feature builds of all eleven public crates; dependency
  audits cover their full feature graph.
  Exact-package tests qualify storage recovery, bidirectional wire exchange and
  rolling TLS upgrades against the published alpha.
- Local builds pin Rust `1.88.0`, with a separate current-stable CI lane. CI
  evidence uses pinned runtime inputs and reviewed test selections. The
  standalone durable TLS counter and checked integration guide exercise real
  recovery, quorum loss, bounded overload and shutdown through the public APIs.

- `rafter-runtime` now provides an opt-in bounded ordered application worker.
  It drains only ready work, retains entry and byte credits through completion
  consumption, independently bounds application batch work, rejects
  applied-index gaps, verifies successful durable floors and outcome counts,
  and closes admission with owned-work evidence on failure.
- The repository's architecture is now executable policy: a zrail contract
  (`zrail.toml`, lock-bound) declares the layer graph, sans-IO source scopes,
  capability owners, qualification gates, and per-file shrink-only size
  ratchets, checked on every pull request with zrail `0.0.3-rc.8`.
  Duplicate sibling-test, size, and mutation checks now belong to zrail;
  stricter syntax, compatibility, public API, and evidence guards remain.
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
