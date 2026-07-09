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
rafter-storage
rafter-codec
rafter-transport-tcp-insecure
rafter-runtime
rafter-app
rafter-service
rafter-multiraft
```

Do not publish these crates for 0.0.1:

```text
rafter-sim
rafter-maelstrom
rafter-fuzz
bench-compare
```

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

Before publishing, run:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run --release -p rafter-sim --bin rafter-model-check-fast -- --profile fast
scripts/tla-model-check
```

Run `scripts/private-name-scan` with private downstream names supplied through
`RAFTER_PRIVATE_NAME_PATTERNS` before tagging a public release.

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
