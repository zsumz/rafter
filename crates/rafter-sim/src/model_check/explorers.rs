use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    time::Instant,
};

use rafter::{LogIndex, NodeId};

use crate::Cluster;

use super::{
    application::{apply_to_cluster, apply_to_restart_snapshot_state, apply_to_state},
    invariants::{
        check_commit_safety, check_election_safety, check_read_barrier_safety,
        check_restart_snapshot_safety,
    },
    scheduling::{
        enabled_actions, enabled_commit_actions, enabled_read_index_actions,
        enabled_restart_snapshot_actions,
    },
    Action, Bounds, ExplorationState, Failure, RestartSnapshotState, Summary,
};

#[derive(Debug)]
pub(super) struct ElectionSafetyExplorer {
    budget: ExplorationBudget,
}

impl ElectionSafetyExplorer {
    pub(super) const INVARIANT: &'static str = "raft election safety";

    pub(super) fn new(bounds: Bounds) -> Self {
        Self {
            budget: ExplorationBudget::new(bounds),
        }
    }

    pub(super) fn summary(&self) -> Summary {
        self.budget.summary()
    }

    pub(super) fn explore(
        &mut self,
        cluster: &Cluster,
        trace: &mut Vec<Action>,
        depth: usize,
    ) -> Result<(), Failure> {
        if !self.budget.enter(cluster, depth) {
            return Ok(());
        }
        check_election_safety(cluster, trace)?;

        if depth == self.budget.bounds.depth {
            return Ok(());
        }

        for action in enabled_actions(cluster) {
            let mut next = cluster.clone();
            apply_to_cluster(&mut next, action.operation);
            self.budget.record_action();
            trace.push(action.trace);
            self.explore(&next, trace, depth + 1)?;
            trace.pop();
        }

        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct CommitSafetyExplorer {
    budget: ExplorationBudget,
    observed_required_applies: BTreeSet<(NodeId, LogIndex)>,
    observed_required_configurations: BTreeSet<(NodeId, LogIndex)>,
    observed_required_commits: BTreeSet<(NodeId, LogIndex)>,
}

impl CommitSafetyExplorer {
    pub(super) const INVARIANT: &'static str = "raft commit safety";

    pub(super) fn new(bounds: Bounds) -> Self {
        Self {
            budget: ExplorationBudget::new(bounds),
            observed_required_applies: BTreeSet::new(),
            observed_required_configurations: BTreeSet::new(),
            observed_required_commits: BTreeSet::new(),
        }
    }

    pub(super) fn summary(&self) -> Summary {
        self.budget.summary()
    }

    pub(super) fn observed_required_applies(&self) -> &BTreeSet<(NodeId, LogIndex)> {
        &self.observed_required_applies
    }

    pub(super) fn observed_required_configurations(&self) -> &BTreeSet<(NodeId, LogIndex)> {
        &self.observed_required_configurations
    }

    pub(super) fn observed_required_commits(&self) -> &BTreeSet<(NodeId, LogIndex)> {
        &self.observed_required_commits
    }

    pub(super) fn explore(
        &mut self,
        state: &ExplorationState,
        trace: &mut Vec<Action>,
        depth: usize,
    ) -> Result<(), Failure> {
        if !self.budget.enter(state, depth) {
            return Ok(());
        }
        check_election_safety(&state.cluster, trace)?;
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

#[derive(Debug)]
pub(super) struct ReadIndexSafetyExplorer {
    budget: ExplorationBudget,
}

impl ReadIndexSafetyExplorer {
    pub(super) fn new(bounds: Bounds) -> Self {
        Self {
            budget: ExplorationBudget::new(bounds),
        }
    }

    pub(super) fn summary(&self) -> Summary {
        self.budget.summary()
    }

    pub(super) fn explore(
        &mut self,
        state: &ExplorationState,
        trace: &mut Vec<Action>,
        depth: usize,
    ) -> Result<(), Failure> {
        if !self.budget.enter(state, depth) {
            return Ok(());
        }
        check_election_safety(&state.cluster, trace)?;
        check_commit_safety(state, trace)?;
        check_read_barrier_safety(&state.cluster, trace)?;

        if depth == self.budget.bounds.depth {
            return Ok(());
        }

        for action in enabled_read_index_actions(state, self.budget.bounds) {
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

#[derive(Debug)]
pub(super) struct RestartSafetyExplorer {
    budget: ExplorationBudget,
    pub(super) observed_restart: bool,
    pub(super) observed_pending_snapshot: bool,
    pub(super) observed_installed_snapshot: bool,
}

impl RestartSafetyExplorer {
    pub(super) const INVARIANT: &'static str = "raft restart and snapshot safety";

    pub(super) fn new(bounds: Bounds) -> Self {
        Self {
            budget: ExplorationBudget::new(bounds),
            observed_restart: false,
            observed_pending_snapshot: false,
            observed_installed_snapshot: false,
        }
    }

    pub(super) fn summary(&self) -> Summary {
        self.budget.summary()
    }

    pub(super) fn explore(
        &mut self,
        state: &RestartSnapshotState,
        trace: &mut Vec<Action>,
        depth: usize,
    ) -> Result<(), Failure> {
        if !self.budget.enter(state, depth) {
            return Ok(());
        }
        self.observe(state, trace);
        check_election_safety(&state.state.cluster, trace)?;
        check_restart_snapshot_safety(state, trace)?;

        if depth == self.budget.bounds.depth {
            return Ok(());
        }

        for action in enabled_restart_snapshot_actions(state, self.budget.bounds) {
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
        self.observed_restart |= trace
            .iter()
            .any(|action| matches!(action, Action::Restart(_)));

        let Some(expected) = &state.expected_snapshot else {
            return;
        };

        for (node_id, node) in &state.state.cluster.nodes {
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

#[derive(Debug)]
struct ExplorationBudget {
    bounds: Bounds,
    started_at: Instant,
    best_remaining_depth_by_state: BTreeMap<StateKey, usize>,
    unique_states: BTreeSet<StateKey>,
    explored_states: usize,
    explored_actions: usize,
}

impl ExplorationBudget {
    fn new(bounds: Bounds) -> Self {
        Self {
            bounds,
            started_at: Instant::now(),
            best_remaining_depth_by_state: BTreeMap::new(),
            unique_states: BTreeSet::new(),
            explored_states: 0,
            explored_actions: 0,
        }
    }

    fn summary(&self) -> Summary {
        Summary {
            explored_states: self.explored_states,
            unique_states: self.unique_states.len(),
            explored_actions: self.explored_actions,
            max_depth: self.bounds.depth,
        }
    }

    fn enter(&mut self, state: &impl Hash, depth: usize) -> bool {
        self.explored_states += 1;
        if self.wall_clock_exhausted() {
            return false;
        }

        let key = StateKey::from_hash(state);
        let remaining_depth = self.bounds.depth.saturating_sub(depth);
        if self
            .best_remaining_depth_by_state
            .get(&key)
            .is_some_and(|seen_remaining_depth| *seen_remaining_depth >= remaining_depth)
        {
            return false;
        }

        let is_new_state = !self.unique_states.contains(&key);
        if self
            .bounds
            .max_unique_states
            .is_some_and(|max| is_new_state && self.unique_states.len() >= max)
        {
            return false;
        }

        self.best_remaining_depth_by_state
            .insert(key, remaining_depth);
        self.unique_states.insert(key);
        true
    }

    fn record_action(&mut self) {
        self.explored_actions += 1;
    }

    fn wall_clock_exhausted(&self) -> bool {
        self.bounds
            .max_wall_clock
            .is_some_and(|max| self.started_at.elapsed() >= max)
    }
}

#[cfg(test)]
mod tests {
    use super::{Bounds, ExplorationBudget};

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    enum ToyState {
        Root,
        Detour,
        Shared,
        Descendant,
    }

    #[test]
    fn depth_aware_dedup_reexpands_shorter_path_to_reach_descendant() {
        let mut budget = ExplorationBudget::new(Bounds::new(2));
        let mut entered = Vec::new();

        explore_toy_graph(&mut budget, ToyState::Root, 0, &mut entered);

        assert_eq!(
            entered
                .iter()
                .filter(|state| **state == ToyState::Shared)
                .count(),
            2
        );
        assert!(entered.contains(&ToyState::Descendant));
        assert_eq!(budget.summary().unique_states(), 4);
        assert!(budget.summary().explored_states() > budget.summary().unique_states());
    }

    fn explore_toy_graph(
        budget: &mut ExplorationBudget,
        state: ToyState,
        depth: usize,
        entered: &mut Vec<ToyState>,
    ) {
        if !budget.enter(&state, depth) {
            return;
        }
        entered.push(state);
        if depth == budget.bounds.depth {
            return;
        }

        for next in toy_successors(state) {
            budget.record_action();
            explore_toy_graph(budget, *next, depth + 1, entered);
        }
    }

    fn toy_successors(state: ToyState) -> &'static [ToyState] {
        use ToyState::{Descendant, Detour, Root, Shared};

        match state {
            Root => &[Detour, Shared],
            Detour => &[Shared],
            Shared => &[Descendant],
            Descendant => &[],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StateKey {
    len: u64,
    hash_a: u64,
    hash_b: u64,
}

impl StateKey {
    fn from_hash(state: &impl Hash) -> Self {
        let mut hasher = StateKeyHasher::new();
        state.hash(&mut hasher);
        hasher.finish_key()
    }
}

struct StateKeyHasher {
    len: u64,
    hash_a: u64,
    hash_b: u64,
}

impl StateKeyHasher {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    const fn new() -> Self {
        Self {
            len: 0,
            hash_a: Self::FNV_OFFSET,
            hash_b: Self::FNV_OFFSET ^ 0x9e37_79b9_7f4a_7c15,
        }
    }

    const fn finish_key(self) -> StateKey {
        StateKey {
            len: self.len,
            hash_a: self.hash_a,
            hash_b: self.hash_b,
        }
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        self.len = self.len.saturating_add(bytes.len() as u64);
        for byte in bytes {
            let byte = u64::from(*byte);
            self.hash_a ^= byte;
            self.hash_a = self.hash_a.wrapping_mul(Self::FNV_PRIME);
            self.hash_b ^= byte.wrapping_add(0x517c_c1b7_2722_0a95);
            self.hash_b = self.hash_b.wrapping_mul(Self::FNV_PRIME).rotate_left(13);
        }
    }
}

impl Hasher for StateKeyHasher {
    fn finish(&self) -> u64 {
        self.hash_a ^ self.hash_b.rotate_left(17) ^ self.len.rotate_left(31)
    }

    fn write(&mut self, bytes: &[u8]) {
        self.write_bytes(bytes);
    }
}
