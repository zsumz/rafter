//! MB-03 checks proposal-time commitment, not a recovered replica's log shape.

use super::{catalog, summarize, Action, ExplorationState, Failure};

pub(super) fn check_serialized_configuration_proposals(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for proposal in &state.commit_history().configuration_proposals {
        let Some(predecessor) = proposal.predecessor else {
            continue;
        };
        if predecessor <= proposal.commit_before {
            continue;
        }
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::MB_03_SERIALIZED_CONFIGURATION_CHANGES,
            message: format!(
                "{} proposed configuration {} in term {} before predecessor {} was committed; proposal-time commit index was {}",
                proposal.proposer, proposal.index, proposal.term, predecessor, proposal.commit_before
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    Ok(())
}
