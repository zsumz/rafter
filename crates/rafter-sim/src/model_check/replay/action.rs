use rafter::NodeId;

use super::super::{
    scheduling::Operation,
    state::{apply_to_state, restart_node, restart_node_losing_application_state},
    Action, ExplorationState, MessageKind, ProposalId,
};
use super::ReplayError;

pub(super) fn replay_action(
    state: &mut ExplorationState,
    action_index: usize,
    action: &Action,
    replayed: &[Action],
) -> Result<(), ReplayError> {
    match action {
        Action::Tick(node_id) => {
            apply_to_state(state, Operation::Tick(*node_id));
            Ok(())
        }
        Action::ReadIndex { to, request_id } => {
            apply_to_state(
                state,
                Operation::ReadIndex {
                    to: *to,
                    request_id: *request_id,
                },
            );
            Ok(())
        }
        Action::AddLearner { .. }
        | Action::RemoveLearner { .. }
        | Action::PromoteLearner { .. }
        | Action::RemoveVoter { .. }
        | Action::EnterJoint { .. }
        | Action::LeaveJoint { .. } => replay_membership_action(state, action_index, action),
        Action::Restart(node_id) => replay_restart_action(state, *node_id, replayed),
        Action::ApplicationLossRestart(node_id) => {
            replay_application_loss_restart_action(state, *node_id, replayed)
        }
        Action::Propose { to, proposal_id } => {
            replay_proposal_action(state, *to, *proposal_id);
            Ok(())
        }
        Action::Deliver { .. } => replay_deliver_action(state, action_index, action),
    }
}

fn replay_membership_action(
    state: &mut ExplorationState,
    action_index: usize,
    action: &Action,
) -> Result<(), ReplayError> {
    match action {
        Action::AddLearner { to, learner_id } => {
            apply_to_state(
                state,
                Operation::AddLearner {
                    to: *to,
                    learner_id: *learner_id,
                },
            );
            Ok(())
        }
        Action::RemoveLearner { to, learner_id } => {
            apply_to_state(
                state,
                Operation::RemoveLearner {
                    to: *to,
                    learner_id: *learner_id,
                },
            );
            Ok(())
        }
        Action::PromoteLearner { to, learner_id } => {
            let Some(promotion_barrier) = state.cluster().promotion_barrier(*to, *learner_id)
            else {
                return Err(ReplayError::MissingPromotionBarrier {
                    action_index,
                    action: action.clone(),
                });
            };
            apply_to_state(
                state,
                Operation::PromoteLearner {
                    to: *to,
                    learner_id: *learner_id,
                    promotion_barrier,
                },
            );
            Ok(())
        }
        Action::RemoveVoter { to, voter_id } => {
            apply_to_state(
                state,
                Operation::RemoveVoter {
                    to: *to,
                    voter_id: *voter_id,
                },
            );
            Ok(())
        }
        Action::EnterJoint { to, target } => {
            apply_to_state(
                state,
                Operation::EnterJoint {
                    to: *to,
                    target: target.clone(),
                    promotion_barriers: Vec::new(),
                },
            );
            Ok(())
        }
        Action::LeaveJoint { to } => {
            apply_to_state(state, Operation::LeaveJoint { to: *to });
            Ok(())
        }
        Action::Tick(_)
        | Action::ReadIndex { .. }
        | Action::Restart(_)
        | Action::ApplicationLossRestart(_)
        | Action::Propose { .. }
        | Action::Deliver { .. } => unreachable!("caller filters membership replay actions"),
    }
}

fn replay_restart_action(
    state: &mut ExplorationState,
    node_id: NodeId,
    replayed: &[Action],
) -> Result<(), ReplayError> {
    restart_node(state, node_id, replayed).map_err(|failure| ReplayError::UnexpectedFailure {
        expected: "successful restart replay",
        actual: failure,
    })
}

fn replay_application_loss_restart_action(
    state: &mut ExplorationState,
    node_id: NodeId,
    replayed: &[Action],
) -> Result<(), ReplayError> {
    restart_node_losing_application_state(state, node_id, replayed).map_err(|failure| {
        ReplayError::UnexpectedFailure {
            expected: "successful application-loss restart replay",
            actual: failure,
        }
    })
}

fn replay_proposal_action(state: &mut ExplorationState, to: NodeId, proposal_id: ProposalId) {
    let stale_leader = state.cluster().nodes.get(&to).is_some_and(|node| {
        state
            .cluster()
            .nodes
            .values()
            .any(|other| other.current_term() > node.current_term())
    });
    apply_to_state(
        state,
        Operation::Propose {
            to,
            proposal_id,
            stale_leader,
        },
    );
}

fn replay_deliver_action(
    state: &mut ExplorationState,
    action_index: usize,
    action: &Action,
) -> Result<(), ReplayError> {
    let Action::Deliver {
        from,
        to,
        message,
        identity,
    } = action
    else {
        unreachable!("caller filters delivery replay actions");
    };
    let mut selected = None;
    for (position, queued) in state.cluster().network.iter().enumerate() {
        let actual_identity =
            super::super::scheduling::envelope_identity(state.cluster(), position).map_err(
                |error| ReplayError::SchedulingFailure {
                    action_index,
                    action: action.clone(),
                    message: error.to_string(),
                },
            )?;
        if queued.ready_at <= state.cluster().clock.now()
            && queued.envelope.from == *from
            && queued.envelope.to == *to
            && MessageKind::from(&queued.envelope.message) == *message
            && actual_identity == *identity
        {
            selected = Some(position);
            break;
        }
    }
    let Some(position) = selected else {
        return Err(ReplayError::MissingReadyMessage {
            action_index,
            action: action.clone(),
        });
    };
    apply_to_state(state, Operation::DeliverReadyAt(position));
    Ok(())
}
