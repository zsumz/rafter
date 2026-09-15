# Standalone Rafter counter

Three separate server processes, file-backed Raft and application state, and
mutually authenticated TLS peer connections. The
[getting-started walkthrough](../../getting-started.md) explains how the crates
connect, what Rafter handles, and what your application implements. Run commands
and a complete source reference follow the explanations.

This folder is its own Cargo project. Copy it anywhere; its Rafter dependencies
come from a pinned Git revision, with no dependency on the surrounding checkout.
Generated certificates and data stay under `certs/` and `data/` in the directory
you run it from. The client HTTP listeners bind to loopback only.

Run the additional checks with Rust 1.88, OpenSSL, and Python 3 installed:

```sh
cargo test --locked
cargo build --locked
python3 smoke.py target/debug/rafter-counter
```

The smoke test uses temporary directories and its own local ports. It checks
TLS client authentication, writes and reads, leader loss, full-cluster crash
recovery, missing or corrupt state, quorum loss, and clean shutdown. It only
stops processes it started.

From the Rafter repository root, `python3 scripts/check-counter-guide` also
checks that its teaching excerpts match this application, extracts the complete
source reference into a fresh folder, and builds and tests that copy. After
editing application files, update the explanations and excerpts as needed;
`python3 scripts/check-counter-guide --update` refreshes the complete source
reference and checks the excerpts without running Cargo.
