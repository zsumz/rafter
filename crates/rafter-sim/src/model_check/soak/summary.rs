use std::collections::{BTreeMap, BTreeSet};

use rafter::NodeId;

use crate::SimSeed;

use super::{SoakAction, SoakActionKind};

/// Summary returned after a successful randomized soak run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SoakSummary {
    seed: SimSeed,
    steps_executed: usize,
    observed_actions: BTreeSet<SoakActionKind>,
    action_counts: BTreeMap<SoakActionKind, usize>,
    restarted_nodes: BTreeSet<NodeId>,
}

impl SoakSummary {
    pub(in crate::model_check) fn from_trace(
        seed: SimSeed,
        steps_executed: usize,
        trace: &[SoakAction],
    ) -> Self {
        let mut observed_actions = BTreeSet::new();
        let mut action_counts = BTreeMap::<SoakActionKind, usize>::new();
        let mut restarted_nodes = BTreeSet::<NodeId>::new();
        for action in trace {
            let kind = action.kind();
            observed_actions.insert(kind);
            *action_counts.entry(kind).or_default() += 1;
            if let SoakAction::Restart(node_id) = action {
                restarted_nodes.insert(*node_id);
            }
        }
        Self {
            seed,
            steps_executed,
            observed_actions,
            action_counts,
            restarted_nodes,
        }
    }

    /// Returns the deterministic simulator seed.
    #[must_use]
    pub const fn seed(&self) -> SimSeed {
        self.seed
    }

    /// Returns the number of steps executed.
    #[must_use]
    pub const fn steps_executed(&self) -> usize {
        self.steps_executed
    }

    /// Returns the action families observed during the run.
    #[must_use]
    pub const fn observed_actions(&self) -> &BTreeSet<SoakActionKind> {
        &self.observed_actions
    }

    /// Returns how many times an action family was observed.
    #[must_use]
    pub fn action_count(&self, kind: SoakActionKind) -> usize {
        self.action_counts.get(&kind).copied().unwrap_or_default()
    }

    /// Returns nodes that were restarted during the run.
    #[must_use]
    pub const fn restarted_nodes(&self) -> &BTreeSet<NodeId> {
        &self.restarted_nodes
    }
}
