# Standalone Rafter counter

Three separate server processes, file-backed Raft and application state, and
mutually authenticated TLS peer connections. Follow the complete
[getting-started walkthrough](../../getting-started.md) for setup, curl requests,
and restart recovery. Every application file is also included in that guide.

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
extracts the guide's code blocks into a fresh folder and builds that copy.
After editing application files, run `python3 scripts/check-counter-guide --update`
to refresh their inline copies before running the check.
