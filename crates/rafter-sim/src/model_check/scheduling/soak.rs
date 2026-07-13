use rafter::{NodeId, Role};

use crate::Envelope;

use super::super::{
    EnvelopeIdentity, ExplorationState, MessageKind, ProposalId, SoakAction, SoakActionKind,
    SoakConfig,
};
use super::membership::{
    enabled_membership_operations, soak_membership_operation, soak_membership_trace,
};
use super::newer_term_has_leader;
use super::operation::{EnabledSoakAction, SoakOperation};

pub(in crate::model_check) fn enabled_soak_actions(
    state: &ExplorationState,
    config: SoakConfig,
) -> Vec<EnabledSoakAction> {
    let mut actions = state
        .cluster()
        .nodes
        .keys()
        .copied()
        .map(|node_id| EnabledSoakAction {
            trace: SoakAction::Tick(node_id),
            operation: SoakOperation::Tick(node_id),
        })
        .collect::<Vec<_>>();

    if state.proposals_issued() < config.max_proposals as u64 {
        let proposal_id = ProposalId(state.proposals_issued() + 1);
        for (node_id, node) in &state.cluster().nodes {
            if node.role() != Role::Leader {
                continue;
            }
            let stale_leader = newer_term_has_leader(state.cluster(), node.current_term());
            actions.push(EnabledSoakAction {
                trace: SoakAction::Propose {
                    to: *node_id,
                    proposal_id,
                },
                operation: SoakOperation::Propose {
                    to: *node_id,
                    proposal_id,
                    stale_leader,
                },
            });
        }
    }

    for (position, queued) in state.cluster().network.iter().enumerate() {
        let action = soak_message_action(
            &queued.envelope,
            super::envelope_identity(state.cluster(), position),
        );
        if queued.ready_at <= state.cluster().clock.now() {
            actions.push(EnabledSoakAction {
                trace: SoakAction::Deliver {
                    from: action.from,
                    to: action.to,
                    message: action.message,
                    identity: action.identity,
                },
                operation: SoakOperation::DeliverReadyAt(position),
            });
        }
        actions.push(EnabledSoakAction {
            trace: SoakAction::Delay {
                from: action.from,
                to: action.to,
                message: action.message,
                identity: action.identity,
                ticks: 1,
            },
            operation: SoakOperation::DelayAt(position, 1),
        });
        actions.push(EnabledSoakAction {
            trace: SoakAction::Drop {
                from: action.from,
                to: action.to,
                message: action.message,
                identity: action.identity,
            },
            operation: SoakOperation::DropAt(position),
        });
        actions.push(EnabledSoakAction {
            trace: SoakAction::Duplicate {
                from: action.from,
                to: action.to,
                message: action.message,
                identity: action.identity,
            },
            operation: SoakOperation::DuplicateAt(position),
        });
    }

    if state.restarts_issued() < config.max_restarts as u64 {
        actions.extend(
            state
                .cluster()
                .nodes
                .keys()
                .copied()
                .map(|node_id| EnabledSoakAction {
                    trace: SoakAction::Restart(node_id),
                    operation: SoakOperation::Restart(node_id),
                }),
        );
    }

    actions.extend(enabled_soak_fault_actions(state, config));

    actions
}

/// The A2 fault-and-protocol families: read barriers, leadership transfers,
/// sustained partitions with healing, and floor-truncating lossy restarts.
fn enabled_soak_fault_actions(
    state: &ExplorationState,
    config: SoakConfig,
) -> Vec<EnabledSoakAction> {
    let mut actions = Vec::new();
    if state.read_indexes_issued() < config.max_read_indexes as u64 {
        let request_id = state.read_indexes_issued() + 1;
        for (node_id, node) in &state.cluster().nodes {
            if node.role() == Role::Leader {
                actions.push(EnabledSoakAction {
                    trace: SoakAction::ReadIndex {
                        to: *node_id,
                        request_id,
                    },
                    operation: SoakOperation::ReadIndex {
                        to: *node_id,
                        request_id,
                    },
                });
            }
        }
    }

    if state.transfers_issued() < config.max_transfers as u64 {
        for (from, node) in &state.cluster().nodes {
            if node.role() != Role::Leader {
                continue;
            }
            for target in state.cluster().nodes.keys() {
                if target != from {
                    actions.push(EnabledSoakAction {
                        trace: SoakAction::Transfer {
                            from: *from,
                            target: *target,
                        },
                        operation: SoakOperation::Transfer {
                            from: *from,
                            target: *target,
                        },
                    });
                }
            }
        }
    }

    if state.partitions_issued() < config.max_partitions as u64 {
        let node_ids: Vec<NodeId> = state.cluster().nodes.keys().copied().collect();
        for (position, a) in node_ids.iter().enumerate() {
            for b in &node_ids[position + 1..] {
                if !state.cluster().partitioned(*a, *b) {
                    actions.push(EnabledSoakAction {
                        trace: SoakAction::Partition { a: *a, b: *b },
                        operation: SoakOperation::Partition { a: *a, b: *b },
                    });
                }
            }
        }
    }
    if state.cluster().nodes.keys().any(|a| {
        state
            .cluster()
            .nodes
            .keys()
            .any(|b| state.cluster().partitioned(*a, *b))
    }) {
        actions.push(EnabledSoakAction {
            trace: SoakAction::Heal,
            operation: SoakOperation::Heal,
        });
    }

    if state.lossy_restarts_issued() < config.max_lossy_restarts as u64 {
        actions.extend(
            state
                .cluster()
                .nodes
                .keys()
                .copied()
                .map(|node_id| EnabledSoakAction {
                    trace: SoakAction::LossyRestart(node_id),
                    operation: SoakOperation::LossyRestart(node_id),
                }),
        );
    }

    if state.membership_changes_issued() < config.max_membership_changes as u64 {
        actions.extend(
            enabled_membership_operations(state)
                .into_iter()
                .map(|operation| EnabledSoakAction {
                    trace: soak_membership_trace(&operation),
                    operation: soak_membership_operation(operation),
                }),
        );
    }

    actions
}

pub(in crate::model_check) const fn soak_preferred_kind(step: usize) -> SoakActionKind {
    match step % 18 {
        0 | 7 => SoakActionKind::Tick,
        1 | 8 => SoakActionKind::Deliver,
        2 => SoakActionKind::Propose,
        3 => SoakActionKind::Delay,
        4 => SoakActionKind::Drop,
        5 => SoakActionKind::Duplicate,
        6 => SoakActionKind::Restart,
        9 => SoakActionKind::ReadIndex,
        10 => SoakActionKind::Transfer,
        11 => SoakActionKind::Partition,
        12 => SoakActionKind::AddLearner,
        13 => SoakActionKind::PromoteLearner,
        14 => SoakActionKind::LeaveJoint,
        15 => SoakActionKind::RemoveVoter,
        16 => SoakActionKind::RemoveLearner,
        _ => SoakActionKind::EnterJoint,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SoakMessageAction {
    from: NodeId,
    to: NodeId,
    message: MessageKind,
    identity: EnvelopeIdentity,
}

fn soak_message_action(envelope: &Envelope, identity: EnvelopeIdentity) -> SoakMessageAction {
    SoakMessageAction {
        from: envelope.from,
        to: envelope.to,
        message: MessageKind::from(&envelope.message),
        identity,
    }
}
