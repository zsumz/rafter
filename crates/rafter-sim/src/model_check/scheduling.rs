use rafter::{Role, Term};

use crate::{Cluster, Envelope};

use super::state::{ExplorationState, RestartSnapshotState};
use super::{Action, Bounds, MessageKind, ProposalId};

mod membership;
mod operation;
mod soak;

use membership::enabled_membership_actions;
use operation::EnabledAction;
pub(in crate::model_check) use operation::{Operation, SoakOperation};
pub(super) use soak::{enabled_soak_actions, soak_preferred_kind};
pub(super) fn enabled_actions(cluster: &Cluster) -> Vec<EnabledAction> {
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
                trace: deliver_action(&queued.envelope),
                operation: Operation::DeliverReadyAt(position),
            });
        }
    }

    actions
}

pub(super) fn enabled_commit_actions(
    state: &ExplorationState,
    bounds: Bounds,
) -> Vec<EnabledAction> {
    let mut actions = enabled_actions(state.cluster());
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

    actions
}

pub(super) fn enabled_read_index_actions(
    state: &ExplorationState,
    bounds: Bounds,
) -> Vec<EnabledAction> {
    let mut actions = enabled_commit_actions(state, bounds);
    if state.read_indexes_issued() >= bounds.read_index_count as u64 {
        return actions;
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
    actions
}

pub(super) fn enabled_restart_snapshot_actions(
    state: &RestartSnapshotState,
    bounds: Bounds,
) -> Vec<EnabledAction> {
    let mut actions = if state.expected_snapshot.is_some() {
        state
            .state
            .cluster()
            .network
            .iter()
            .enumerate()
            .filter(|(_, queued)| queued.ready_at <= state.state.cluster().clock.now())
            .map(|(position, queued)| EnabledAction {
                trace: deliver_action(&queued.envelope),
                operation: Operation::DeliverReadyAt(position),
            })
            .collect::<Vec<_>>()
    } else {
        enabled_actions(state.state.cluster())
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
    }

    actions
}
fn newer_term_has_leader(cluster: &Cluster, term: Term) -> bool {
    cluster
        .nodes
        .values()
        .any(|node| node.role() == Role::Leader && node.current_term() > term)
}
fn deliver_action(envelope: &Envelope) -> Action {
    Action::Deliver {
        from: envelope.from,
        to: envelope.to,
        message: MessageKind::from(&envelope.message),
    }
}
