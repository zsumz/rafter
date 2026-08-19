# Release Checklist

## Current work-completion preflight

The initial reference-consumer and public-surface work is complete as an
engineering milestone, not as a release candidate. The stable capability-to-job
map is [`docs/work-completion.md`](docs/work-completion.md); its references are
checked by `scripts/work-completion-check`.

The current publishable graph is the eleven-crate product family listed in the
0.0.2-alpha.1 section below. It adds `rafter-transport-tls` to the historical
ten-crate embedding graph without publishing the simulator, invariant tooling,
Maelstrom harness, or trusted detector-test helpers. `rafter-sim`'s hidden
`internal-test-hooks` dependency is gone, and that blocker is closed: the
kernel self-check the simulator reached for is now the documented public
`Node::validate_derived_state`, returning a typed `StateValidationError`, and
the feature is deleted from `rafter` entirely. The design and what it
deliberately defers are recorded in [`docs/api-promotions.md`](docs/api-promotions.md).
C1 streaming snapshots and C2 replication pipelining remain additive future
work and are not preconditions smuggled into this preflight.

For a non-release completion run at one checked-out SHA, run and retain:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
scripts/rustdoc-check
scripts/work-completion-check
scripts/reference-source-check
scripts/reference-package-check
scripts/reference-process-check --list-only
scripts/reference-process-check
scripts/reference-package-process-check
cargo run --locked -p rafter-invariants -- run-all --profile pr
scripts/maelstrom-lin-kv
scripts/raft-burn-in
scripts/counter-profile counter-nightly
scripts/counter-profile counter-weekly
```

The scheduled `invariants-nightly`, `invariants-weekly`, and pinned Maelstrom
jobs remain the authoritative Linux evidence for their full profiles. The
fenced-lock and sharded-counter authenticated transport cases are included in
the reviewed process inventories. `rafter-transport-tls` is now part of the
exact publishable archive set; the fixtures prove bounded composability and are
not a generic server, certificate platform, or deployment controller.

This section does not authorize a version change, RC, `1.0.0`, tag, publish,
mandatory release workflow, or mixed-version compatibility claim.

## 0.0.2-alpha.1 Product Preview

Rafter 0.0.2-alpha.1 is the first coordinated prerelease of the current product
graph. It publishes the production TLS transport with the embedding crates it
actually compiles against, while preserving the explicit alpha status of the
public API, wire formats, storage formats, and operational contracts.

Every Rafter dependency inside this family is exact-pinned to
`=0.0.2-alpha.1`. A package archive therefore cannot silently combine this
source generation with the spent 0.0.1 registry generation or a later alpha.

Publish these crates for 0.0.2-alpha.1:

```text
rafter
rafter-runtime-api
rafter-crc32
rafter-storage
rafter-codec
rafter-transport-tcp-insecure
rafter-runtime
rafter-app
rafter-service
rafter-multiraft
rafter-transport-tls
```

Do not publish these crates for 0.0.2-alpha.1:

```text
rafter-sim
rafter-maelstrom
rafter-invariants
rafter-invariant-test
rafter-invariant-test-macros
rafter-fuzz
bench-compare
```

The first list is the complete versioned dependency closure a public product
consumer can reach. The second list contains repository-only verification,
simulation, fuzzing, and benchmark tooling; each package remains guarded by
`publish = false`.

### Publish Order

Publish one verified archive at a time in this dependency order:

```text
rafter
rafter-runtime-api
rafter-crc32
rafter-storage
rafter-codec
rafter-transport-tcp-insecure
rafter-runtime
rafter-app
rafter-service
rafter-multiraft
rafter-transport-tls
```

Run `cargo publish --dry-run -p <crate>` immediately before each matching
`cargo publish -p <crate>`. Do not advance after either command fails. The
release is complete only after all eleven registry records, owners, checksums,
downloaded archives, and docs.rs builds are independently verified.

### Release Notes

Use explicit alpha wording:

```text
Rafter 0.0.2-alpha.1 is a coordinated product-family preview.

It includes the deterministic sans-I/O Raft core, current peer and storage
formats, durable runtime and application layers, managed multi-Raft hosting,
the insecure TCP demonstration transport, and the bounded mutually
authenticated TLS transport.

It does not promise stable public API, wire compatibility, storage
compatibility, operational compatibility, or performance leadership.
```

## 0.0.1 Preview

Rafter 0.0.1 is a pre-alpha packaging and API preview release. It reserves the
crate names, validates the published crate graph, and lets early readers inspect
the current embedding surface without implying API, wire, storage, transport, or
performance stability.

Publish these crates for 0.0.1:

```text
rafter
rafter-runtime-api
rafter-crc32
rafter-storage
rafter-codec
rafter-transport-tcp-insecure
rafter-runtime
rafter-app
rafter-service
rafter-multiraft
```

`rafter-crc32` is small, but it is not optional. A published `rafter-codec` or
`rafter-storage` carries `rafter-crc32 = { version = "0.0.1" }`, so the two
format crates cannot resolve on crates.io until it is live there. This list is
the complete set of Rafter crates a consumer graph can reach.

Do not publish these crates for 0.0.1:

```text
rafter-sim
rafter-maelstrom
rafter-invariants
rafter-invariant-test
rafter-invariant-test-macros
rafter-transport-tls
rafter-fuzz
bench-compare
```

Together the two lists cover every crate in the repository: the workspace
members plus `rafter-fuzz` and `bench-compare`, which keep their own manifests
outside the workspace. For the 0.0.1 release, every crate in the second list
carried `publish = false`; `rafter-transport-tls` is promoted only by the
0.0.2-alpha.1 section above. No crate in the first list carries the guard.

Naming a crate here is not the same as checking it. `rafter-fuzz` and
`bench-compare` are outside the workspace, so no `--workspace` or `--all`
command below reaches either one; the Verification section records what does
reach them and what still does not.

`rafter-sim` stays workspace-only, but no longer because of a hidden surface.
Its dependency on the core crate's `internal-test-hooks` feature is removed and
the feature is deleted; the hook it needed was promoted into
`Node::validate_derived_state`, a documented public kernel API. The crate is
absent from the 0.0.1 list because that release publishes the embedding graph,
and adding a simulation and model-checking harness to it is a separate decision
nobody has taken.

### Publish Order

For the first 0.0.1 release, publish in dependency order. Cargo checks versioned
path dependencies against the registry when packaging, so dependent crates cannot
fully package until their predecessors are live on crates.io.

Registry state, verified 2026-08-15 against the live index: `rafter`,
`rafter-runtime-api`, `rafter-storage`, `rafter-codec`,
`rafter-transport-tcp-insecure`, `rafter-runtime`, `rafter-app`,
`rafter-service`, and `rafter-multiraft` are already on crates.io at 0.0.1,
published 2026-07-09. `rafter-crc32` is not there at all; its name is
unclaimed. Those nine archives predate this checkout and cannot be replaced:
the published `rafter-storage` and `rafter-codec` declare neither `rafter-crc32`
nor `fs4`, and the published `rafter` has no `ClientProposalInput`. So 0.0.1 is
spent. Publishing the current sources needs a new version across all ten
crates; `cargo publish -p rafter` at 0.0.1 is rejected as already uploaded.
Until that bump lands, a per-crate `cargo publish --dry-run` for any crate with
a Rafter dependency fails, because Cargo resolves that dependency to the stale
registry 0.0.1 instead of to this checkout.

`rafter-crc32` 0.0.1 was subsequently published on 2026-08-19 as the first
prerequisite for the coordinated 0.0.2-alpha.1 product release.

```sh
cargo publish --dry-run -p rafter
cargo publish -p rafter

cargo publish --dry-run -p rafter-runtime-api
cargo publish -p rafter-runtime-api

cargo publish --dry-run -p rafter-crc32
cargo publish -p rafter-crc32

cargo publish --dry-run -p rafter-storage
cargo publish -p rafter-storage

cargo publish --dry-run -p rafter-codec
cargo publish -p rafter-codec

cargo publish --dry-run -p rafter-transport-tcp-insecure
cargo publish -p rafter-transport-tcp-insecure

cargo publish --dry-run -p rafter-runtime
cargo publish -p rafter-runtime

cargo publish --dry-run -p rafter-app
cargo publish -p rafter-app

cargo publish --dry-run -p rafter-service
cargo publish -p rafter-service

cargo publish --dry-run -p rafter-multiraft
cargo publish -p rafter-multiraft
```

That sequence is a valid dependency order and stays the order to publish in,
but the `--dry-run` half of each pair only proves anything once the crates
above it are live. The check a checkout can run today packages the whole set in
one command, so Cargo verifies each archive against the sibling archives the
same run just built rather than against the registry:

```sh
cargo package -p rafter -p rafter-runtime-api -p rafter-crc32 \
  -p rafter-storage -p rafter-codec -p rafter-transport-tcp-insecure \
  -p rafter-runtime -p rafter-app -p rafter-service -p rafter-multiraft
```

Run that and require it green before the first `cargo publish` of a release.

### Verification

Cargo packages only files under a crate's own directory, so every publishable
crate keeps a physical copy of the repository's `LICENSE` and `NOTICE` beside
its manifest. The root files stay authoritative: the per-crate copies must match
them byte for byte, and a copy that drifts ships a licence the project did not
grant. That is enforced: `cargo test -p rafter --test publish_metadata_contract`
reads the publish list out of this file, compares every crate it names against
the root `LICENSE` and `NOTICE` byte for byte, and holds those same crates to
the description, readme, keywords, and categories a crates.io upload needs. It
runs under `cargo test --workspace`.

Before publishing, run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
scripts/rustdoc-check
cargo fmt --manifest-path fuzz/Cargo.toml --all -- --check
cargo clippy --manifest-path fuzz/Cargo.toml --all-targets -- -D warnings
cargo clippy --manifest-path bench-compare/Cargo.toml --locked \
  --no-default-features --all-targets -- -D warnings
cargo run --release -p rafter-sim --bin rafter-model-check-fast -- --profile fast
scripts/tla-model-check
scripts/reference-source-check
scripts/reference-package-check
scripts/reference-package-process-check
```

`scripts/rustdoc-check` is the gate on the HTML that reaches docs.rs. It builds
the publish list above by name as well as the whole workspace, and both builds
still earn their place: naming the publish list pins the exact set a crates.io
reader lands on under `-D missing-docs`, while the workspace build also covers
crates that never reach docs.rs but are still read here. The two no longer
differ in the *shape* of `rafter`. They used to: `rafter-sim` depended on
`rafter` with the hidden `internal-test-hooks` feature and resolver 2 unified
it into every `--workspace` invocation, so a workspace-only doc build
documented `rafter` in a shape no published consumer could produce. That
feature no longer exists.

`scripts/reference-package-check` and
`scripts/reference-package-process-check` disable Cargo's per-archive
verification.
Once a Rafter version is already published, Cargo can verify a dependent
archive against that older registry sibling rather than the sibling archive
produced by the checkout. The lane instead unpacks the complete newly packaged
set and builds and tests consumers against those exact archives. The process
command also runs every reviewed ignored process suite from the unpacked set,
then rebuilds and tests that same set in public-feature shape on Rust 1.88 with
a process smoke. Archive creation remains on the newer packaging toolchain.

These commands leave one thing unchecked, which is not implied away elsewhere
in this file. Two prior gaps have closed. Per-crate `LICENSE` and `NOTICE`
copies are now compared byte for byte against the root by the publish-metadata
contract above. And `rafter` previously had no published-shape build outside
`scripts/reference-package-check` phase 6, because every `--workspace`
invocation resolved `rafter-sim` and turned the hidden feature on; with
`internal-test-hooks` removed, `rafter` has no features, every check builds
the shape a published consumer gets, and phase 6 is corroboration rather than
the sole evidence.

- **`bench-compare` formatting, and its comparison binaries' lints.**
  `bench-compare` is not format-checked by any command here or in CI, and it is
  not currently `rustfmt` clean. Its `raft-rs` and `openraft` binaries sit
  behind default features that need `protoc`; only `benchmarks.yml` installs
  that, and it compiles those binaries without linting them. The
  `--no-default-features` invocation above lints the Rafter-only binaries and
  the shared library, which is all of `bench-compare` except those two files.

Run `scripts/private-name-scan` with private downstream names supplied through
`RAFTER_PRIVATE_NAME_PATTERNS` before tagging a public release. It reads every
file a package archive ships and every file a repository visitor reads,
including each crate's own markdown, `LICENSE`, `NOTICE`, and the plaintext
`.hex` codec and storage vectors. It is a manual step: no workflow supplies the
patterns, which live outside this repository.

The first of those two claims is checked rather than trusted. Before scanning,
the script asks Cargo which files each publishable crate actually ships and
fails if any of them falls outside its own include globs, so a new kind of
shipped file fails the gate until the globs cover it. Three archive entries are
exempt because Cargo synthesizes them at package time and each is covered by
scanning its source instead: `Cargo.toml.orig`, `.cargo_vcs_info.json`, and the
per-crate `Cargo.lock` generated from the workspace lockfile at the root.

Two things this scan does not do. It does not read binary or base64-heavy
assets — `--include-assets` is a separate, human-reviewed pass. And the second
claim, "every file a repository visitor reads", has no equivalent proof behind
it: that target list is still maintained by hand, and nothing fails when a new
top-level directory appears. Treat it as a reviewed list, not a closed one.

### Release Notes

Use plain preview wording:

```text
Rafter 0.0.1 is an initial preview release.

It includes a deterministic sans-IO Raft core, current-only peer codec v1,
current-only storage formats v1, a durable persisted-output runtime API,
application and service layers, a multi-Raft host, and an insecure TCP demo
transport.

It does not promise stable public API, stable wire compatibility, stable storage
compatibility, production transport security, or performance leadership yet.
```
