//! The failure renderings every snapshot detector reports through.
//!
//! Each constructor fixes one invariant id and one failure kind, so a detector
//! chooses which obligation it broke rather than restating the classification
//! at each call site. Deciding whether a violation occurred is the detectors'
//! job; nothing here inspects state.

use crate::Cluster;

use super::{catalog, summarize, Action, ExplorationState, Failure};

pub(super) fn snapshot_failure(
    state: &ExplorationState,
    trace: &[Action],
    message: String,
) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE,
        message,
        trace: trace.to_vec(),
        state: summarize(state.cluster()),
    }
}

pub(super) fn snapshot_harness_failure(
    state: &ExplorationState,
    trace: &[Action],
    message: String,
) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::HarnessError,
        invariant: catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE,
        message,
        trace: trace.to_vec(),
        state: summarize(state.cluster()),
    }
}

pub(super) fn snapshot_coverage_failure(
    state: &ExplorationState,
    trace: &[Action],
    message: String,
) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::CoverageNotReached,
        invariant: catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE,
        message,
        trace: trace.to_vec(),
        state: summarize(state.cluster()),
    }
}

pub(super) fn ss03_failure(cluster: &Cluster, trace: &[Action], message: String) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::SS_03_SNAPSHOT_LOG_INDEX_GEOMETRY,
        message,
        trace: trace.to_vec(),
        state: summarize(cluster),
    }
}

pub(super) fn ss04_failure(cluster: &Cluster, trace: &[Action], message: String) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
        message,
        trace: trace.to_vec(),
        state: summarize(cluster),
    }
}
