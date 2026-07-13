use std::fmt;

use rafter::{Role, Term};

use crate::Cluster;

use super::state::{ExplorationState, RestartSnapshotState};
use super::{
    helpers::summarize, Action, Bounds, EnvelopeIdentity, Failure, FailureKind, MessageKind,
    ProposalId,
};

mod membership;
mod operation;
mod soak;

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
pub(in crate::model_check) fn deliver_action(
    cluster: &Cluster,
    position: usize,
) -> Result<Action, SchedulingError> {
    let queued = cluster
        .network
        .get(position)
        .ok_or(SchedulingError::MissingEnvelope {
            position,
            queue_len: cluster.network.len(),
        })?;
    let envelope = &queued.envelope;
    Ok(Action::Deliver {
        from: envelope.from,
        to: envelope.to,
        message: MessageKind::from(&envelope.message),
        identity: envelope_identity(cluster, position)?,
    })
}

pub(in crate::model_check) fn envelope_identity(
    cluster: &Cluster,
    position: usize,
) -> Result<EnvelopeIdentity, SchedulingError> {
    let queued = cluster
        .network
        .get(position)
        .ok_or(SchedulingError::MissingEnvelope {
            position,
            queue_len: cluster.network.len(),
        })?;
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
    Ok(EnvelopeIdentity::new(
        queued.ready_at,
        u64::try_from(matching_ordinal)
            .map_err(|_| SchedulingError::EnvelopeOrdinalOverflow { matching_ordinal })?,
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::model_check) enum SchedulingError {
    MissingEnvelope { position: usize, queue_len: usize },
    EnvelopeOrdinalOverflow { matching_ordinal: usize },
}

impl SchedulingError {
    pub(in crate::model_check) fn into_failure(
        self,
        cluster: &Cluster,
        trace: &[Action],
    ) -> Failure {
        Failure {
            kind: FailureKind::HarnessError,
            invariant: "model-check scheduling harness",
            message: self.to_string(),
            trace: trace.to_vec(),
            state: summarize(cluster),
        }
    }
}

impl fmt::Display for SchedulingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvelope {
                position,
                queue_len,
            } => write!(
                formatter,
                "scheduler selected envelope position {position} from queue length {queue_len}"
            ),
            Self::EnvelopeOrdinalOverflow { matching_ordinal } => write!(
                formatter,
                "scheduler envelope matching ordinal {matching_ordinal} does not fit in u64"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use rafter::{LogIndex, Message, NodeConfig, NodeId, RequestVote, Term};

    use crate::{Cluster, SimSeed};

    use super::{deliver_action, enabled_soak_actions, envelope_identity, ExplorationState};
    use crate::model_check::{FailureKind, SoakAction, SoakConfig};

    #[test]
    fn missing_envelope_is_a_deterministic_scheduler_harness_error() {
        let config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("test config is valid");
        let cluster = Cluster::new(vec![config]);

        let identity_error = envelope_identity(&cluster, 7)
            .expect_err("missing envelope identity must be handled explicitly");
        let action_error =
            deliver_action(&cluster, 7).expect_err("missing delivery must be handled explicitly");
        assert_eq!(identity_error, action_error);
        let failure = action_error.into_failure(&cluster, &[]);
        assert_eq!(failure.kind(), FailureKind::HarnessError);
        assert_eq!(failure.invariant(), "model-check scheduling harness");
        assert_eq!(
            failure.message(),
            "scheduler selected envelope position 7 from queue length 0"
        );
    }

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
            .expect("fixture envelopes have valid scheduler identities")
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
