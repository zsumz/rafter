use super::super::{
    catalog,
    invariants::{
        check_commit_history, check_election_history, check_election_safety, check_log_history,
    },
    scheduling::enabled_actions,
    state::apply_to_state,
    Action, Bounds, ExplorationState, Failure, Summary,
};
use super::budget::ExplorationBudget;

#[derive(Debug)]
pub(in crate::model_check) struct ElectionSafetyExplorer {
    budget: ExplorationBudget,
}

impl ElectionSafetyExplorer {
    pub(in crate::model_check) const INVARIANT: &'static str =
        catalog::EL_05_ELECTION_SAFETY_OVER_HISTORY;

    pub(in crate::model_check) fn new(bounds: Bounds) -> Self {
        Self {
            budget: ExplorationBudget::new(bounds),
        }
    }

    pub(in crate::model_check) fn summary(&self) -> Summary {
        self.budget.summary()
    }

    pub(in crate::model_check) fn explore(
        &mut self,
        state: &ExplorationState,
        trace: &mut Vec<Action>,
        depth: usize,
    ) -> Result<(), Failure> {
        if !self.budget.enter(state, depth) {
            return Ok(());
        }
        check_election_safety(state.cluster(), trace)?;
        check_election_history(state, trace)?;
        check_log_history(state, trace)?;
        check_commit_history(state, trace)?;

        if depth == self.budget.bounds.depth {
            return Ok(());
        }

        let actions = enabled_actions(state.cluster())
            .map_err(|error| error.into_failure(state.cluster(), trace))?;
        for action in actions {
            let mut next = state.clone();
            apply_to_state(&mut next, action.operation);
            self.budget.record_action();
            trace.push(action.trace);
            self.explore(&next, trace, depth + 1)?;
            trace.pop();
        }

        Ok(())
    }
}
