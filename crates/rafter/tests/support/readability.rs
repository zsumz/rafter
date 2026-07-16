//! Shared presentation contracts used by readability ratchets.

pub(super) const FACADE_PATHS: &[&str] = &[
    "crates/rafter-codec/src/lib.rs",
    "crates/rafter-codec/src/v1/mod.rs",
    "crates/rafter/src/lib.rs",
    "crates/rafter/src/message/mod.rs",
    "crates/rafter/src/node/mod.rs",
    "crates/rafter/src/node/bootstrap/mod.rs",
    "crates/rafter/src/node/commit/mod.rs",
    "crates/rafter/src/node/config/mod.rs",
    "crates/rafter/src/node/event/mod.rs",
    "crates/rafter/src/node/membership/mod.rs",
    "crates/rafter/src/node/replication/mod.rs",
    "crates/rafter/src/node/replication/snapshot/mod.rs",
    "crates/rafter/src/node/state.rs",
    "crates/rafter/src/node/state/membership/mod.rs",
    "crates/rafter/src/types/mod.rs",
    "crates/rafter/src/types/snapshot/mod.rs",
    "crates/rafter-sim/src/model_check/state/logical_log.rs",
    "crates/rafter-sim/src/model_check/state/logical_log/types.rs",
];

/// Test modules that only map a mature domain to focused scenario files.
pub(super) const TEST_FACADE_PATHS: &[&str] = &[
    "crates/rafter-codec/src/tests/mod.rs",
    "crates/rafter/src/node/tests.rs",
    "crates/rafter/src/node/tests/bootstrap.rs",
    "crates/rafter/src/node/tests/bootstrap/snapshot.rs",
    "crates/rafter/src/node/tests/config.rs",
    "crates/rafter/src/node/tests/election.rs",
    "crates/rafter/src/node/tests/membership.rs",
    "crates/rafter/src/node/tests/read.rs",
    "crates/rafter/src/node/tests/replication.rs",
    "crates/rafter/src/node/tests/replication/leader.rs",
    "crates/rafter/src/node/tests/replication/pipelining.rs",
    "crates/rafter/src/node/tests/snapshot.rs",
    "crates/rafter/src/node/tests/snapshot/chunks.rs",
    "crates/rafter/src/node/tests/snapshot/install.rs",
    "crates/rafter/src/node/tests/snapshot/streaming.rs",
    "crates/rafter/src/node/tests/transfer.rs",
];
