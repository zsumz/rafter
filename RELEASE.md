# Release Checklist

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
rafter-fuzz
bench-compare
```

Together the two lists cover every crate in the repository: the workspace
members plus `rafter-fuzz` and `bench-compare`, which keep their own manifests
outside the workspace. Every crate in the second list carries `publish = false`;
no crate in the first list does.

Naming a crate here is not the same as checking it. `rafter-fuzz` and
`bench-compare` are outside the workspace, so no `--workspace` or `--all`
command below reaches either one; the Verification section records what does
reach them and what still does not.

`rafter-sim` stays workspace-only until its dependency on the core crate's
hidden `internal-test-hooks` feature is either removed or promoted into an
intentional public simulation hook.

### Publish Order

For the first 0.0.1 release, publish in dependency order. Cargo checks versioned
path dependencies against the registry when packaging, so dependent crates cannot
fully package until their predecessors are live on crates.io.

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

### Verification

Cargo packages only files under a crate's own directory, so every publishable
crate keeps a physical copy of the repository's `LICENSE` and `NOTICE` beside
its manifest. The root files stay authoritative: the per-crate copies must match
them byte for byte, and a copy that drifts ships a licence the project did not
grant. Nothing enforces that today; comparing each copy against the root is a
good CI check to add.

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
```

`scripts/rustdoc-check` is the gate on the HTML that reaches docs.rs. It builds
the publish list above by name as well as the whole workspace, because
`rafter-sim` depends on `rafter` with the hidden `internal-test-hooks` feature
and resolver 2 unifies that feature into every `--workspace` invocation: a
workspace-only doc build documents `rafter` in a shape no published consumer
can produce.

`scripts/reference-package-check` disables Cargo's per-archive verification.
Once a Rafter version is already published, Cargo can verify a dependent
archive against that older registry sibling rather than the sibling archive
produced by the checkout. The lane instead unpacks the complete newly packaged
set and builds and tests consumers against those exact archives. CI runs this
lane on 1.96.1; fast source mode holds the consumer sources to the workspace's
1.88 compatibility floor.

These commands leave three things unchecked, none of which is implied away
elsewhere in this file:

- **Per-crate `LICENSE` and `NOTICE` copies.** Nothing compares them against
  the root, as the paragraph above says.
- **`bench-compare` formatting, and its comparison binaries' lints.**
  `bench-compare` is not format-checked by any command here or in CI, and it is
  not currently `rustfmt` clean. Its `raft-rs` and `openraft` binaries sit
  behind default features that need `protoc`; only `benchmarks.yml` installs
  that, and it compiles those binaries without linting them. The
  `--no-default-features` invocation above lints the Rafter-only binaries and
  the shared library, which is all of `bench-compare` except those two files.
- **`rafter` in its published feature shape, outside the package lane.** Every
  `--workspace` invocation resolves `rafter-sim` and therefore builds `rafter`
  with `internal-test-hooks` on. `scripts/reference-package-check` phase 6 is
  the only check that the crate composes with that feature off, which is the
  shape a published consumer gets.

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
