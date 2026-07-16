mod types;

pub use types::{
    Action, Bounds, EnvelopeIdentity, ExplorationCompletion, Failure, FailureKind, MessageKind,
    NodeSummary, ProposalId, StateSummary, Summary,
};

mod tla;

pub use tla::{
    project_raft_trace_to_tla, render_tla_trace_spec, require_tla_projectable_raft_trace,
    TlaAbstractionGap, TlaAction, TlaProjection, TlaProjectionFailure, TlaTraceRenderError,
    TlaTraceSpec, TlaTraceStep,
};

mod replay;

pub use replay::{replay_raft_trace, ReplayCheck, ReplayError, ReplayExpectation, ReplayReport};

mod helpers;

#[cfg(test)]
use helpers::summarize;

mod catalog;

pub use catalog::reviewed_invariant_id;

mod invariants;

mod linearizability;

mod observations;

mod scheduling;

mod state;

use state::{ExplorationState, RestartSnapshotState};

mod soak;

pub use soak::{
    SoakAction, SoakActionKind, SoakConfig, SoakExecutionParameters, SoakFailure, SoakSummary,
};

mod liveness;

mod explorers;

mod checks;

pub use checks::{
    check_raft_commit_safety, check_raft_election_safety,
    check_raft_joint_membership_restart_and_snapshot_safety, check_raft_leadership_noop_safety,
    check_raft_lease_fast_path_read_safety, check_raft_membership_safety,
    check_raft_production_config_commit_safety, check_raft_read_index_safety,
    check_raft_restart_and_snapshot_safety, check_raft_seeded_commit_safety,
    check_raft_semantic_witness_safety, check_raft_window_one_backpressure_safety,
};

mod soak_runner;

pub use soak_runner::run_raft_random_soak;

#[cfg(test)]
mod tests;
