//! Enumeration of the actions enabled from a model-check state.
//!
//! An action is offered only when the state and the configured bounds both
//! permit it, so a driver cannot exceed its proposal, restart, read, or
//! membership budget by construction, and every queued envelope carries a
//! distinct scheduler identity. Choosing among them is the driver's job.

use rafter::{Role, Term};

use crate::Cluster;

use super::state::{ExplorationState, RestartSnapshotState};
use super::{Action, Bounds, ProposalId};

mod envelope;
mod membership;
mod operation;
mod soak;

pub(in crate::model_check) use envelope::{deliver_action, envelope_identity, SchedulingError};
use membership::enabled_membership_actions;
use operation::EnabledAction;
pub(in crate::model_check) use operation::{Operation, SoakOperation};
pub(super) use soak::{enabled_soak_actions, soak_preferred_kind};
pub(super) fn enabled_actions(cluster: &Cluster) -> Result<Vec<EnabledAction>, SchedulingError> {
    let mut actions = cluster
        .nodes
        .keys()
        .copied()
        .map(|node_id| EnabledAction {
            trace: Action::Tick(node_id),
            operation: Operation::Tick(node_id),
        })
        .collect::<Vec<_>>();

    for (position, queued) in cluster.network.iter().enumerate() {
        if queued.ready_at <= cluster.clock.now() {
            actions.push(EnabledAction {
                trace: deliver_action(cluster, position)?,
                operation: Operation::DeliverReadyAt(position),
            });
        }
    }

    Ok(actions)
}

pub(super) fn enabled_commit_actions(
    state: &ExplorationState,
    bounds: Bounds,
) -> Result<Vec<EnabledAction>, SchedulingError> {
    let mut actions = enabled_actions(state.cluster())?;
    if state.proposals_issued() < bounds.proposal_count as u64 {
        let proposal_id = ProposalId(state.proposals_issued() + 1);
        for (node_id, node) in &state.cluster().nodes {
            if node.role() != Role::Leader {
                continue;
            }
            let stale_leader = newer_term_has_leader(state.cluster(), node.current_term());
            actions.push(EnabledAction {
                trace: Action::Propose {
                    to: *node_id,
                    proposal_id,
                },
                operation: Operation::Propose {
                    to: *node_id,
                    proposal_id,
                    stale_leader,
                },
            });
        }
    }

    actions.extend(enabled_membership_actions(state, bounds));
    if state.restarts_issued() < bounds.restart_count as u64 {
        actions.extend(
            state
                .cluster()
                .nodes
                .keys()
                .copied()
                .map(|node_id| EnabledAction {
                    trace: Action::ApplicationLossRestart(node_id),
                    operation: Operation::ApplicationLossRestart(node_id),
                }),
        );
    }

    Ok(actions)
}

pub(super) fn enabled_read_index_actions(
    state: &ExplorationState,
    bounds: Bounds,
) -> Result<Vec<EnabledAction>, SchedulingError> {
    let mut actions = enabled_commit_actions(state, bounds)?;
    if state.read_indexes_issued() >= bounds.read_index_count as u64 {
        return Ok(actions);
    }

    let request_id = state.read_indexes_issued() + 1;
    for (node_id, node) in &state.cluster().nodes {
        if node.role() != Role::Leader {
            continue;
        }
        actions.push(EnabledAction {
            trace: Action::ReadIndex {
                to: *node_id,
                request_id,
            },
            operation: Operation::ReadIndex {
                to: *node_id,
                request_id,
            },
        });
    }
    Ok(actions)
}

pub(super) fn enabled_restart_snapshot_actions(
    state: &RestartSnapshotState,
    bounds: Bounds,
) -> Result<Vec<EnabledAction>, SchedulingError> {
    let mut actions = if state.expected_snapshot.is_some() {
        state
            .state
            .cluster()
            .network
            .iter()
            .enumerate()
            .filter(|(_, queued)| queued.ready_at <= state.state.cluster().clock.now())
            .map(|(position, _queued)| {
                Ok(EnabledAction {
                    trace: deliver_action(state.state.cluster(), position)?,
                    operation: Operation::DeliverReadyAt(position),
                })
            })
            .collect::<Result<Vec<_>, SchedulingError>>()?
    } else {
        enabled_actions(state.state.cluster())?
    };

    if state.state.restarts_issued() < bounds.restart_count as u64 {
        actions.extend(
            state
                .state
                .cluster()
                .nodes
                .keys()
                .copied()
                .map(|node_id| EnabledAction {
                    trace: Action::Restart(node_id),
                    operation: Operation::Restart(node_id),
                }),
        );
        if state.expected_snapshot.is_none() {
            actions.extend(state.state.cluster().nodes.keys().copied().map(|node_id| {
                EnabledAction {
                    trace: Action::ApplicationLossRestart(node_id),
                    operation: Operation::ApplicationLossRestart(node_id),
                }
            }));
        }
    }

    Ok(actions)
}
fn newer_term_has_leader(cluster: &Cluster, term: Term) -> bool {
    cluster
        .nodes
        .values()
        .any(|node| node.role() == Role::Leader && node.current_term() > term)
}

#[cfg(test)]
#[path = "scheduling_test.rs"]
mod tests;
