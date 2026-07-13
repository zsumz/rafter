use rafter::{Role, Term};

use crate::Cluster;

use super::state::{ExplorationState, RestartSnapshotState};
use super::{Action, Bounds, EnvelopeIdentity, MessageKind, ProposalId};

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
                trace: deliver_action(cluster, position),
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
            .map(|(position, _queued)| EnabledAction {
                trace: deliver_action(state.state.cluster(), position),
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
pub(in crate::model_check) fn deliver_action(cluster: &Cluster, position: usize) -> Action {
    let queued = cluster
        .network
        .get(position)
        .expect("enabled delivery position must name a queued envelope");
    let envelope = &queued.envelope;
    Action::Deliver {
        from: envelope.from,
        to: envelope.to,
        message: MessageKind::from(&envelope.message),
        identity: envelope_identity(cluster, position),
    }
}

pub(in crate::model_check) fn envelope_identity(
    cluster: &Cluster,
    position: usize,
) -> EnvelopeIdentity {
    let queued = cluster
        .network
        .get(position)
        .expect("envelope identity position must be queued");
    let kind = MessageKind::from(&queued.envelope.message);
    let matching_ordinal = cluster
        .network
        .iter()
        .take(position)
        .filter(|candidate| {
            candidate.envelope.from == queued.envelope.from
                && candidate.envelope.to == queued.envelope.to
                && MessageKind::from(&candidate.envelope.message) == kind
        })
        .count();
    EnvelopeIdentity::new(
        queued.ready_at,
        u64::try_from(matching_ordinal).expect("queue ordinal fits in u64"),
    )
}

#[cfg(test)]
mod tests {
    use rafter::{LogIndex, Message, NodeConfig, NodeId, RequestVote, Term};

    use crate::{Cluster, SimSeed};

    use super::{enabled_soak_actions, ExplorationState};
    use crate::model_check::{SoakAction, SoakConfig};

    #[test]
    fn soak_actions_distinguish_duplicate_envelopes() {
        let config = NodeConfig::new(NodeId(1), vec![NodeId(2)], 3).expect("test config is valid");
        let mut state = ExplorationState::new(Cluster::new(vec![config]));
        let message = Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        });
        state.inject_message(NodeId(2), NodeId(1), message.clone());
        state.inject_message(NodeId(2), NodeId(1), message);

        let identities = enabled_soak_actions(&state, SoakConfig::new(SimSeed(7), 1))
            .into_iter()
            .filter_map(|action| match action.trace {
                SoakAction::Deliver { identity, .. } => Some(identity),
                _ => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(identities.len(), 2);
        assert_eq!(identities[0].matching_ordinal(), 0);
        assert_eq!(identities[1].matching_ordinal(), 1);
        assert_ne!(identities[0], identities[1]);
    }
}
