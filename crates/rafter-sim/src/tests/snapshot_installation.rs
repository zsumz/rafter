//! Snapshot transfer and installation scenarios.
//!
//! Groups the catch-up and chunk-stream suites: how a lagging follower is
//! brought forward by a snapshot rather than by log entries.

mod catchup;
mod fixtures;
mod stream;
