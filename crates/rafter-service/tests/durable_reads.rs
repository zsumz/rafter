#![cfg(feature = "durable")]
//! Managed queries exercise real owners, workers, and kernel quorum rounds.

#[path = "durable_reads/mod.rs"]
mod read_tests;
#[allow(
    dead_code,
    unused_imports,
    reason = "shared real-worker fixture also serves the write-only suite"
)]
#[path = "durable_client/support.rs"]
mod write_support;
