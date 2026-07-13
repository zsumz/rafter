use super::super::{
    invariants::{
        check_commit_history, check_commit_safety, check_election_history, check_election_safety,
        check_log_history, check_read_barrier_safety,
    },
    scheduling::enabled_read_index_actions,
    state::apply_scheduled_operation,
    Action, Bounds, ExplorationState, Failure, Summary,
};
use super::budget::ExplorationBudget;

#[derive(Debug)]
pub(in crate::model_check) struct ReadIndexSafetyExplorer {
    budget: ExplorationBudget,
}

impl ReadIndexSafetyExplorer {
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
        check_commit_safety(state, trace)?;
        check_read_barrier_safety(state.cluster(), trace)?;

        if depth == self.budget.bounds.depth {
            return Ok(());
        }

        let actions = enabled_read_index_actions(state, self.budget.bounds)
            .map_err(|error| error.into_failure(state.cluster(), trace))?;
        for action in actions {
            let mut next = state.clone();
            trace.push(action.trace);
            apply_scheduled_operation(&mut next, action.operation, trace)?;
            self.budget.record_action();
            self.explore(&next, trace, depth + 1)?;
            trace.pop();
        }

        Ok(())
    }
}
