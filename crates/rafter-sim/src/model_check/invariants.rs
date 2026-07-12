use std::collections::BTreeMap;

use rafter::{
    CommittedConfiguration, LogEntry, LogIndex, MembershipConfig, NodeId, Role, SharedPayload, Term,
};

use crate::Cluster;

use super::catalog;
use super::explorers::ElectionSafetyExplorer;
use super::helpers::summarize;
use super::linearizability::{
    check_client_history_linearizable, CLIENT_HISTORY_LINEARIZABILITY_INVARIANT,
};
use super::state::{ClientReadOutcome, ClientWriteStatus};
use super::state::{ExplorationState, RestartSnapshotState};
use super::{Action, Failure, ReplayCheck};
use applied::{
    check_applied_order, check_applied_payload_agreement, check_forbidden_applied_payloads,
    check_internal_derived_state, check_required_applied_payloads,
};
pub(super) use client::check_read_barrier_safety;
use client::{check_client_history_linearizability, check_client_history_read_write_invariants};
#[cfg(test)]
use commit::check_no_overlapping_uncommitted_configurations_in_bootstrap;
use commit::{
    check_commit_index_monotonicity, check_committed_configuration_monotonicity,
    check_committed_prefixes, check_membership_quorum_validity,
    check_no_overlapping_uncommitted_configurations, check_required_committed_configurations,
};
pub(super) use election::{check_election_history, check_election_safety};
pub(super) use history::{check_commit_history, check_log_history};
pub(super) use persistence::{
    check_applied_floor_recovery, check_exact_durable_restart, AppliedFloorRecovery,
};
pub(super) use snapshot::check_restart_snapshot_safety;
use snapshot::check_snapshot_log_geometry;
#[cfg(test)]
use snapshot::{
    check_snapshot_boundary_monotonicity, check_snapshot_log_geometry_shape,
    check_snapshot_payload_binding, check_snapshot_transfer_identity,
    check_snapshot_transfer_integrity,
};

pub(super) fn check_commit_safety(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_internal_derived_state(state.cluster(), trace)?;
    check_commit_index_monotonicity(state, trace)?;
    check_committed_configuration_monotonicity(state, trace)?;
    check_applied_payload_agreement(state.cluster(), trace)?;
    check_applied_order(state.cluster(), trace)?;
    check_snapshot_log_geometry(state.cluster(), trace)?;
    check_committed_prefixes(state.cluster(), trace)?;
    check_membership_quorum_validity(state.cluster(), trace)?;
    check_no_overlapping_uncommitted_configurations(state.cluster(), trace)?;
    check_client_history_read_write_invariants(state, trace)?;
    check_client_history_linearizability(state, trace)?;
    check_forbidden_applied_payloads(state, trace)?;
    check_required_applied_payloads(state, trace)?;
    check_required_committed_configurations(state, trace)
}

mod applied;
mod client;
mod commit;
mod election;
mod history;
mod persistence;
mod snapshot;
pub(super) fn run_replay_check(
    state: &ExplorationState,
    check: ReplayCheck,
    trace: &[Action],
) -> Result<(), Failure> {
    match check {
        ReplayCheck::ElectionSafety => {
            check_election_safety(state.cluster(), trace)?;
            check_election_history(state, trace)?;
            check_log_history(state, trace)?;
            check_commit_history(state, trace)
        }
        ReplayCheck::CommitSafety => {
            check_election_safety(state.cluster(), trace)?;
            check_election_history(state, trace)?;
            check_log_history(state, trace)?;
            check_commit_history(state, trace)?;
            check_commit_safety(state, trace)?;
            check_read_barrier_safety(state.cluster(), trace)
        }
    }
}

#[cfg(test)]
mod tests;
