use std::{error::Error, fmt};

use super::{
    apply_to_state, restart_node, run_replay_check, summarize, Action, Cluster, ExplorationState,
    Failure, MessageKind, NodeConfig, Operation, ProposalId, StateSummary,
};

/// Invariant suite to run while replaying a model-check trace.
///
/// This enum is exhaustive because replay currently supports this closed set
/// of invariant suites.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayCheck {
    ElectionSafety,
    CommitSafety,
}

/// Expected replay result.
///
/// This enum is exhaustive because replay expectations are limited to
/// successful final-state matching or one named invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayExpectation<'a> {
    FinalState(&'a StateSummary),
    FailureInvariant(&'static str),
}

/// Result of replaying a model-check trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayReport {
    state: StateSummary,
    failure: Option<Failure>,
}

impl ReplayReport {
    /// Returns the final or failed state summary produced by replay.
    #[must_use]
    pub const fn state(&self) -> &StateSummary {
        &self.state
    }

    /// Returns the invariant failure observed during replay, if any.
    #[must_use]
    pub const fn failure(&self) -> Option<&Failure> {
        self.failure.as_ref()
    }
}

/// Error returned when a model-check trace cannot be replayed as expected.
///
/// This enum is exhaustive because replay failures are closed over these trace
/// and expectation mismatch cases.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplayError {
    MissingReadyMessage {
        action_index: usize,
        action: Action,
    },
    MissingPromotionBarrier {
        action_index: usize,
        action: Action,
    },
    UnexpectedFailure {
        expected: &'static str,
        actual: Failure,
    },
    ExpectedFailureNotReached {
        expected: &'static str,
        final_state: StateSummary,
    },
    FinalStateMismatch {
        expected: StateSummary,
        actual: StateSummary,
    },
}

impl fmt::Display for ReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingReadyMessage {
                action_index,
                action,
            } => write!(
                formatter,
                "trace action {action_index} could not find a ready message for `{action}`"
            ),
            Self::MissingPromotionBarrier {
                action_index,
                action,
            } => write!(
                formatter,
                "trace action {action_index} could not find a promotion barrier for `{action}`"
            ),
            Self::UnexpectedFailure { expected, actual } => write!(
                formatter,
                "expected replay failure `{expected}`, found `{}`",
                actual.invariant()
            ),
            Self::ExpectedFailureNotReached {
                expected,
                final_state: _,
            } => write!(
                formatter,
                "expected replay failure `{expected}`, but trace completed"
            ),
            Self::FinalStateMismatch {
                expected: _,
                actual: _,
            } => formatter.write_str("replayed trace ended in a different final state"),
        }
    }
}

impl Error for ReplayError {}

/// Replays a model-check action trace against a fresh cluster.
///
/// # Errors
///
/// Returns [`ReplayError`] when the trace cannot be replayed or does not match
/// the requested expectation.
pub fn replay_raft_trace(
    configs: Vec<NodeConfig>,
    trace: &[Action],
    check: ReplayCheck,
    expectation: ReplayExpectation<'_>,
) -> Result<ReplayReport, ReplayError> {
    let mut state = ExplorationState::new(Cluster::new(configs));
    let mut replayed = Vec::new();

    for (action_index, action) in trace.iter().enumerate() {
        replay_action(&mut state, action_index, action, &replayed)?;
        replayed.push(action.clone());
        if let Err(failure) = run_replay_check(&state, check, &replayed) {
            return match expectation {
                ReplayExpectation::FailureInvariant(expected)
                    if failure.invariant() == expected =>
                {
                    Ok(ReplayReport {
                        state: summarize(&state.cluster),
                        failure: Some(failure),
                    })
                }
                ReplayExpectation::FailureInvariant(expected) => {
                    Err(ReplayError::UnexpectedFailure {
                        expected,
                        actual: failure,
                    })
                }
                ReplayExpectation::FinalState(_) => Err(ReplayError::UnexpectedFailure {
                    expected: "no replay failure",
                    actual: failure,
                }),
            };
        }
    }

    let actual = summarize(&state.cluster);
    match expectation {
        ReplayExpectation::FinalState(expected) if expected == &actual => Ok(ReplayReport {
            state: actual,
            failure: None,
        }),
        ReplayExpectation::FinalState(expected) => Err(ReplayError::FinalStateMismatch {
            expected: expected.clone(),
            actual,
        }),
        ReplayExpectation::FailureInvariant(expected) => {
            Err(ReplayError::ExpectedFailureNotReached {
                expected,
                final_state: actual,
            })
        }
    }
}

fn replay_action(
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
            apply_membership_operation(
                state,
                Operation::AddLearner {
                    to: *to,
                    learner_id: *learner_id,
                },
            );
            Ok(())
        }
        Action::RemoveLearner { to, learner_id } => {
            apply_membership_operation(
                state,
                Operation::RemoveLearner {
                    to: *to,
                    learner_id: *learner_id,
                },
            );
            Ok(())
        }
        Action::PromoteLearner { to, learner_id } => {
            let Some(promotion_barrier) = state.cluster.promotion_barrier(*to, *learner_id) else {
                return Err(ReplayError::MissingPromotionBarrier {
                    action_index,
                    action: action.clone(),
                });
            };
            apply_membership_operation(
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
            apply_membership_operation(
                state,
                Operation::RemoveVoter {
                    to: *to,
                    voter_id: *voter_id,
                },
            );
            Ok(())
        }
        Action::EnterJoint { to, target } => {
            apply_membership_operation(
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
            apply_membership_operation(state, Operation::LeaveJoint { to: *to });
            Ok(())
        }
        Action::Tick(_)
        | Action::ReadIndex { .. }
        | Action::Restart(_)
        | Action::Propose { .. }
        | Action::Deliver { .. } => unreachable!("caller filters membership replay actions"),
    }
}

fn apply_membership_operation(state: &mut ExplorationState, operation: Operation) {
    apply_to_state(state, operation);
}

fn replay_restart_action(
    state: &mut ExplorationState,
    node_id: rafter::NodeId,
    replayed: &[Action],
) -> Result<(), ReplayError> {
    restart_node(state, node_id, replayed).map_err(|failure| ReplayError::UnexpectedFailure {
        expected: "successful restart replay",
        actual: failure,
    })
}

fn replay_proposal_action(
    state: &mut ExplorationState,
    to: rafter::NodeId,
    proposal_id: ProposalId,
) {
    let stale_leader = state.cluster.nodes.get(&to).is_some_and(|node| {
        state
            .cluster
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
    let Action::Deliver { from, to, message } = action else {
        unreachable!("caller filters delivery replay actions");
    };
    let Some(position) = state.cluster.network.iter().position(|queued| {
        queued.ready_at <= state.cluster.clock.now()
            && queued.envelope.from == *from
            && queued.envelope.to == *to
            && MessageKind::from(&queued.envelope.message) == *message
    }) else {
        return Err(ReplayError::MissingReadyMessage {
            action_index,
            action: action.clone(),
        });
    };
    apply_to_state(state, Operation::DeliverReadyAt(position));
    Ok(())
}
