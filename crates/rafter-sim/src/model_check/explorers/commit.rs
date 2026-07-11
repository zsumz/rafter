use std::collections::BTreeSet;

use rafter::{LogIndex, NodeId};

use super::super::{
    application::apply_to_state,
    invariants::{
        check_commit_history, check_commit_safety, check_election_history, check_election_safety,
        check_log_history,
    },
    scheduling::enabled_commit_actions,
    Action, Bounds, ExplorationState, Failure, Summary,
};
use super::budget::ExplorationBudget;

#[derive(Debug)]
pub(in crate::model_check) struct CommitSafetyExplorer {
    budget: ExplorationBudget,
    observed_required_applies: BTreeSet<(NodeId, LogIndex)>,
    observed_required_configurations: BTreeSet<(NodeId, LogIndex)>,
    observed_required_commits: BTreeSet<(NodeId, LogIndex)>,
}

impl CommitSafetyExplorer {
    pub(in crate::model_check) fn new(bounds: Bounds) -> Self {
        Self {
            budget: ExplorationBudget::new(bounds),
            observed_required_applies: BTreeSet::new(),
            observed_required_configurations: BTreeSet::new(),
            observed_required_commits: BTreeSet::new(),
        }
    }

    pub(in crate::model_check) fn summary(&self) -> Summary {
        self.budget.summary()
    }

    pub(in crate::model_check) fn observed_required_applies(
        &self,
    ) -> &BTreeSet<(NodeId, LogIndex)> {
        &self.observed_required_applies
    }

    pub(in crate::model_check) fn observed_required_configurations(
        &self,
    ) -> &BTreeSet<(NodeId, LogIndex)> {
        &self.observed_required_configurations
    }

    pub(in crate::model_check) fn observed_required_commits(
        &self,
    ) -> &BTreeSet<(NodeId, LogIndex)> {
        &self.observed_required_commits
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
        check_election_safety(&state.cluster, trace)?;
        check_election_history(state, trace)?;
        check_log_history(state, trace)?;
        check_commit_history(state, trace)?;
        check_commit_safety(state, trace)?;
        self.observe_required_commit_points(state);

        if depth == self.budget.bounds.depth {
            return Ok(());
        }

        for action in enabled_commit_actions(state, self.budget.bounds) {
            let mut next = state.clone();
            apply_to_state(&mut next, action.operation);
            self.budget.record_action();
            trace.push(action.trace);
            self.explore(&next, trace, depth + 1)?;
            trace.pop();
        }

        Ok(())
    }

    fn observe_required_commit_points(&mut self, state: &ExplorationState) {
        for key in state.required_applied_payloads.keys() {
            let (node_id, index) = *key;
            if state.cluster.commit_index(node_id) >= index {
                self.observed_required_applies.insert(*key);
            }
        }
        for (key, expected) in &state.required_committed_configurations {
            let (node_id, index) = *key;
            if state.cluster.commit_index(node_id) >= index
                && state.cluster.committed_configuration_state(node_id) == Some(*expected)
            {
                self.observed_required_configurations.insert(*key);
            }
        }
        for key in &state.required_commit_indexes {
            let (node_id, index) = *key;
            if state.cluster.commit_index(node_id) >= index {
                self.observed_required_commits.insert(*key);
            }
        }
    }
}
