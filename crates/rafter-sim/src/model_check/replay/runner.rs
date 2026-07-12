use rafter::NodeConfig;

use crate::Cluster;

use super::super::{
    helpers::summarize, invariants::run_replay_check, state::ExplorationState, Action,
};
use super::action::replay_action;
use super::{ReplayCheck, ReplayError, ReplayExpectation, ReplayReport};

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
                        state: summarize(state.cluster()),
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

    let actual = summarize(state.cluster());
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
