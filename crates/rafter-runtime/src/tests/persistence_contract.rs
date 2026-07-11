//! Compile-checked runtime persistence contract for kernel outputs.
//!
//! Keep `runtime_output_persistence_dependency` exhaustive and without a
//! wildcard arm: adding a new `RaftOutput` variant must force this module to
//! classify the new output before the runtime test suite compiles.

use super::*;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RuntimeStoreOperation {
    PreLogHardStateWrite,
    LogSuffixAppend,
    BatchedLogSuffixAppend,
    FinalHardStateWrite,
    SnapshotChunkStage,
    SnapshotPromote,
    SnapshotCompactAfterPromote,
}

struct FailureCoverage {
    operation: RuntimeStoreOperation,
    path: &'static str,
    source: &'static str,
    symbol: &'static str,
}

const EXPECTED_PS02_FAILURE_OPERATIONS: &[RuntimeStoreOperation] = &[
    RuntimeStoreOperation::PreLogHardStateWrite,
    RuntimeStoreOperation::LogSuffixAppend,
    RuntimeStoreOperation::BatchedLogSuffixAppend,
    RuntimeStoreOperation::FinalHardStateWrite,
    RuntimeStoreOperation::SnapshotChunkStage,
    RuntimeStoreOperation::SnapshotPromote,
    RuntimeStoreOperation::SnapshotCompactAfterPromote,
];

const PS02_FAILURE_COVERAGE: &[FailureCoverage] = &[
    FailureCoverage {
        operation: RuntimeStoreOperation::PreLogHardStateWrite,
        path: "crates/rafter-runtime/src/tests/hard_state/voting.rs",
        source: include_str!("hard_state/voting.rs"),
        symbol: "hard_state_write_failure_suppresses_vote_requests",
    },
    FailureCoverage {
        operation: RuntimeStoreOperation::LogSuffixAppend,
        path: "crates/rafter-runtime/src/tests/persistence_ordering.rs",
        source: include_str!("persistence_ordering.rs"),
        symbol: "log_append_failure_suppresses_apply_outputs",
    },
    FailureCoverage {
        operation: RuntimeStoreOperation::BatchedLogSuffixAppend,
        path: "crates/rafter-runtime/src/tests/group_commit/failure.rs",
        source: include_str!("group_commit/failure.rs"),
        symbol: "a_failed_batch_releases_no_output_and_poisons_the_runtime",
    },
    FailureCoverage {
        operation: RuntimeStoreOperation::FinalHardStateWrite,
        path: "crates/rafter-runtime/src/tests/hard_state/commit_failure.rs",
        source: include_str!("hard_state/commit_failure.rs"),
        symbol: "final_hard_state_write_failure_suppresses_apply_and_success_response",
    },
    FailureCoverage {
        operation: RuntimeStoreOperation::SnapshotChunkStage,
        path: "crates/rafter-runtime/src/tests/snapshot/install.rs",
        source: include_str!("snapshot/install.rs"),
        symbol: "runtime_snapshot_write_failure_poisons_runtime_until_restart",
    },
    FailureCoverage {
        operation: RuntimeStoreOperation::SnapshotPromote,
        path: "crates/rafter-runtime/src/tests/snapshot/install.rs",
        source: include_str!("snapshot/install.rs"),
        symbol: "runtime_snapshot_promote_failure_suppresses_apply_and_success_response",
    },
    FailureCoverage {
        operation: RuntimeStoreOperation::SnapshotCompactAfterPromote,
        path: "crates/rafter-runtime/src/tests/snapshot/install.rs",
        source: include_str!("snapshot/install.rs"),
        symbol: "runtime_snapshot_compaction_failure_suppresses_apply_and_success_response",
    },
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeOutputPersistenceDependency {
    /// The runtime's normal step fence applies: hard state, snapshot side
    /// effects, log suffix repair/append, and final hard state are complete
    /// before this output can escape.
    StepPersistenceFence,
    /// The inbound chunk must be durable in snapshot staging before the
    /// paired acknowledgement may escape.
    SnapshotChunkStage,
    /// The staged snapshot must be promoted and the durable log compacted
    /// before the apply/install acknowledgement may escape.
    SnapshotPromoteAndCompact,
    /// Runtime transports resolve this directive from the snapshot store
    /// after all step persistence has completed; an unresolvable chunk is
    /// dropped rather than emitted as an incomplete message.
    SnapshotChunkResolve,
}

fn runtime_output_persistence_dependency(
    output: &RaftOutput,
) -> RuntimeOutputPersistenceDependency {
    match output {
        RaftOutput::StageSnapshotChunk { .. } => {
            RuntimeOutputPersistenceDependency::SnapshotChunkStage
        }
        RaftOutput::ApplySnapshot { .. } => {
            RuntimeOutputPersistenceDependency::SnapshotPromoteAndCompact
        }
        RaftOutput::SendSnapshotChunk { .. } => {
            RuntimeOutputPersistenceDependency::SnapshotChunkResolve
        }
        RaftOutput::LocalProposalAppended { .. }
        | RaftOutput::LocalProposalDropped { .. }
        | RaftOutput::Apply { .. }
        | RaftOutput::RejectProposal { .. }
        | RaftOutput::LeadershipTransferRejected { .. }
        | RaftOutput::ReadIndexGranted { .. }
        | RaftOutput::ReadIndexRejected { .. }
        | RaftOutput::ReadIndexCanceled { .. }
        | RaftOutput::Send { .. } => RuntimeOutputPersistenceDependency::StepPersistenceFence,
    }
}

#[test]
fn all_raft_outputs_have_declared_runtime_persistence_dependency() {
    // This is intentionally a compile-time guard: the classifier above has
    // no wildcard arm, so any new `RaftOutput` variant must be assigned to a
    // runtime persistence dependency before this test can compile.
    let _: fn(&RaftOutput) -> RuntimeOutputPersistenceDependency =
        runtime_output_persistence_dependency;
}

#[test]
fn ps02_failure_matrix_covers_each_runtime_store_operation() {
    let mut covered = BTreeSet::new();
    for record in PS02_FAILURE_COVERAGE {
        assert!(
            record.source.contains(record.symbol),
            "{} must contain PS-02 failure test `{}`",
            record.path,
            record.symbol,
        );
        assert!(
            covered.insert(record.operation),
            "{:?} has duplicate PS-02 coverage entries",
            record.operation,
        );
    }

    for operation in EXPECTED_PS02_FAILURE_OPERATIONS {
        assert!(
            covered.contains(operation),
            "{operation:?} must have a PS-02 fail-stop test"
        );
    }
    assert_eq!(
        covered.len(),
        EXPECTED_PS02_FAILURE_OPERATIONS.len(),
        "PS-02 matrix must classify only reviewed runtime store operations",
    );
}
