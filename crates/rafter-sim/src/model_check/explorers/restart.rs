use rafter::NodeId;

use super::super::{
    invariants::{
        check_commit_history, check_election_history, check_election_safety, check_log_history,
        check_restart_snapshot_safety,
    },
    scheduling::enabled_restart_snapshot_actions,
    state::apply_to_restart_snapshot_state,
    Action, Bounds, Failure, RestartSnapshotState, Summary,
};
use super::budget::ExplorationBudget;

#[derive(Debug)]
pub(in crate::model_check) struct RestartSafetyExplorer {
    budget: ExplorationBudget,
    pub(in crate::model_check) observed_restart: bool,
    pub(in crate::model_check) observed_pending_snapshot: bool,
    pub(in crate::model_check) observed_installed_snapshot: bool,
}

impl RestartSafetyExplorer {
    pub(in crate::model_check) fn new(bounds: Bounds) -> Self {
        Self {
            budget: ExplorationBudget::new(bounds),
            observed_restart: false,
            observed_pending_snapshot: false,
            observed_installed_snapshot: false,
        }
    }

    pub(in crate::model_check) fn summary(&self) -> Summary {
        self.budget.summary()
    }

    pub(in crate::model_check) fn explore(
        &mut self,
        state: &RestartSnapshotState,
        trace: &mut Vec<Action>,
        depth: usize,
    ) -> Result<(), Failure> {
        if !self.budget.enter(state, depth) {
            return Ok(());
        }
        self.observe(state, trace);
        check_election_safety(state.state.cluster(), trace)?;
        check_election_history(&state.state, trace)?;
        check_log_history(&state.state, trace)?;
        check_commit_history(&state.state, trace)?;
        check_restart_snapshot_safety(state, trace)?;

        if depth == self.budget.bounds.depth {
            return Ok(());
        }

        let actions = enabled_restart_snapshot_actions(state, self.budget.bounds)
            .map_err(|error| error.into_failure(state.state.cluster(), trace))?;
        for action in actions {
            let mut next = state.clone();
            trace.push(action.trace);
            apply_to_restart_snapshot_state(&mut next, action.operation, trace)?;
            self.budget.record_action();
            self.explore(&next, trace, depth + 1)?;
            trace.pop();
        }

        Ok(())
    }

    fn observe(&mut self, state: &RestartSnapshotState, trace: &[Action]) {
        self.observed_restart |= trace.iter().any(|action| {
            matches!(
                action,
                Action::Restart(_) | Action::ApplicationLossRestart(_)
            )
        });

        let Some(expected) = &state.expected_snapshot else {
            return;
        };

        for (node_id, node) in &state.state.cluster().nodes {
            if *node_id != NodeId(2) {
                continue;
            }
            if node
                .pending_snapshot_transfer()
                .is_some_and(|pending| pending.received_bytes() > 0)
            {
                self.observed_pending_snapshot = true;
            }
            if node.snapshot_index() >= expected.snapshot.metadata.last_included_index {
                self.observed_installed_snapshot = true;
            }
        }
    }
}
