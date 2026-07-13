use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    time::Instant,
};

use super::super::{
    observations::ObservationSet, state::ExplorationState, Bounds, ExplorationCompletion,
    RestartSnapshotState, Summary,
};

#[derive(Debug)]
pub(super) struct ExplorationBudget {
    pub(super) bounds: Bounds,
    started_at: Instant,
    best_remaining_depth_by_state: BTreeMap<StateKey, usize>,
    unique_protocol_states: BTreeSet<StateKey>,
    explored_states: usize,
    explored_actions: usize,
    reached_depth: usize,
    completion: ExplorationCompletion,
    observations: ObservationSet,
}

impl ExplorationBudget {
    pub(super) fn new(bounds: Bounds) -> Self {
        Self {
            bounds,
            started_at: Instant::now(),
            best_remaining_depth_by_state: BTreeMap::new(),
            unique_protocol_states: BTreeSet::new(),
            explored_states: 0,
            explored_actions: 0,
            reached_depth: 0,
            completion: ExplorationCompletion::FrontierExhausted,
            observations: ObservationSet::default(),
        }
    }

    pub(super) fn summary(&self) -> Summary {
        Summary {
            explored_states: self.explored_states,
            unique_states: self.best_remaining_depth_by_state.len(),
            unique_protocol_states: self.unique_protocol_states.len(),
            explored_actions: self.explored_actions,
            configured_depth: self.bounds.depth,
            reached_depth: self.reached_depth,
            completion: self.completion,
            observations: self.observations,
        }
    }

    pub(super) fn enter(&mut self, state: &impl StateIdentity, depth: usize) -> bool {
        self.explored_states += 1;
        self.observations.union_with(state.observations());
        if self.wall_clock_exhausted() {
            self.completion = ExplorationCompletion::WallClockLimit;
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

        let is_new_state = !self.best_remaining_depth_by_state.contains_key(&key);
        if self
            .bounds
            .max_unique_states
            .is_some_and(|max| is_new_state && self.best_remaining_depth_by_state.len() >= max)
        {
            self.completion = ExplorationCompletion::UniqueStateLimit;
            return false;
        }

        self.best_remaining_depth_by_state
            .insert(key, remaining_depth);
        self.unique_protocol_states
            .insert(StateKey::from_protocol_state(state));
        self.reached_depth = self.reached_depth.max(depth);
        true
    }

    pub(super) fn record_action(&mut self) {
        self.explored_actions += 1;
    }

    pub(super) fn wall_clock_exhausted(&self) -> bool {
        self.bounds
            .max_wall_clock
            .is_some_and(|max| self.started_at.elapsed() >= max)
    }
}

pub(super) trait StateIdentity: Hash {
    fn hash_protocol_state<H: Hasher>(&self, state: &mut H);

    fn observations(&self) -> ObservationSet;
}

impl StateIdentity for ExplorationState {
    fn hash_protocol_state<H: Hasher>(&self, state: &mut H) {
        self.cluster().hash_protocol_state(state);
        self.proposals_issued().hash(state);
        self.restarts_issued().hash(state);
        self.read_indexes_issued().hash(state);
        self.membership_changes_issued().hash(state);
        self.transfers_issued().hash(state);
        self.partitions_issued().hash(state);
        self.lossy_restarts_issued().hash(state);
    }

    fn observations(&self) -> ObservationSet {
        self.observation_set()
    }
}

impl StateIdentity for RestartSnapshotState {
    fn hash_protocol_state<H: Hasher>(&self, state: &mut H) {
        self.state.hash_protocol_state(state);
    }

    fn observations(&self) -> ObservationSet {
        self.state.observation_set()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        hash::{Hash, Hasher},
        time::Duration,
    };

    use rafter::{LogIndex, NodeConfig, NodeId};

    use super::{Bounds, ExplorationBudget, ExplorationState, StateIdentity, StateKey};
    use crate::{Applied, Cluster};

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    enum ToyState {
        Root,
        Detour,
        Shared,
        Descendant,
    }

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    struct ToyVerifierState {
        protocol: u8,
        verifier_history: u8,
    }

    impl StateIdentity for ToyState {
        fn hash_protocol_state<H: Hasher>(&self, state: &mut H) {
            self.hash(state);
        }

        fn observations(&self) -> super::ObservationSet {
            super::ObservationSet::default()
        }
    }

    impl StateIdentity for ToyVerifierState {
        fn hash_protocol_state<H: Hasher>(&self, state: &mut H) {
            self.protocol.hash(state);
        }

        fn observations(&self) -> super::ObservationSet {
            super::ObservationSet::default()
        }
    }

    #[test]
    pub(super) fn depth_aware_dedup_reexpands_shorter_path_to_reach_descendant() {
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
        assert_eq!(budget.summary().unique_protocol_states(), 4);
        assert!(budget.summary().explored_states() > budget.summary().unique_states());
        assert_eq!(budget.summary().reached_depth(), 2);
    }

    #[test]
    fn protocol_count_collapses_verifier_history_divergence() {
        let mut budget = ExplorationBudget::new(Bounds::new(1));

        assert!(budget.enter(
            &ToyVerifierState {
                protocol: 7,
                verifier_history: 1,
            },
            0,
        ));
        assert!(budget.enter(
            &ToyVerifierState {
                protocol: 7,
                verifier_history: 2,
            },
            0,
        ));

        let summary = budget.summary();
        assert_eq!(summary.unique_states(), 2);
        assert_eq!(summary.unique_verifier_states(), 2);
        assert_eq!(summary.unique_protocol_states(), 1);
    }

    #[test]
    fn protocol_count_ignores_cluster_recorder_history() {
        let config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("fixture config is valid");
        let original = ExplorationState::new(Cluster::new(vec![config]));
        let mut applied_mutated = original.clone();
        applied_mutated.inject_applied_record(Applied {
            node_id: NodeId(1),
            application_epoch: 0,
            commit_index_at_emit: LogIndex(1),
            index: LogIndex(1),
            payload: b"recorder-only".to_vec().into(),
        });

        let mut cursor_mutated = original.clone();
        cursor_mutated.clear_execution_cursors();
        let mut reference_mutated = original.clone();
        reference_mutated.clear_initial_reference_states();
        let mut epoch_mutated = original.clone();
        epoch_mutated.clear_application_epochs();

        for recorder_mutated in [
            &applied_mutated,
            &cursor_mutated,
            &reference_mutated,
            &epoch_mutated,
        ] {
            assert_ne!(
                StateKey::from_hash(&original),
                StateKey::from_hash(recorder_mutated),
                "recorder state remains part of full verifier identity"
            );
            assert_eq!(
                StateKey::from_protocol_state(&original),
                StateKey::from_protocol_state(recorder_mutated),
                "recorder state must not change protocol identity"
            );
        }

        let mut budget = ExplorationBudget::new(Bounds::new(1));
        assert!(budget.enter(&original, 0));
        assert!(budget.enter(&applied_mutated, 0));
        let summary = budget.summary();
        assert_eq!(summary.unique_verifier_states(), 2);
        assert_eq!(summary.unique_protocol_states(), 1);
    }

    #[test]
    fn unique_state_cap_still_applies_to_verifier_states() {
        let mut budget = ExplorationBudget::new(Bounds::new(1).with_max_unique_states(1));

        assert!(budget.enter(
            &ToyVerifierState {
                protocol: 7,
                verifier_history: 1,
            },
            0,
        ));
        assert!(!budget.enter(
            &ToyVerifierState {
                protocol: 7,
                verifier_history: 2,
            },
            0,
        ));

        let summary = budget.summary();
        assert_eq!(summary.unique_states(), 1);
        assert_eq!(summary.unique_verifier_states(), 1);
        assert_eq!(summary.unique_protocol_states(), 1);
        assert_eq!(
            summary.completion(),
            super::ExplorationCompletion::UniqueStateLimit
        );
    }

    #[test]
    fn wall_clock_exhaustion_is_reported() {
        let mut budget = ExplorationBudget::new(Bounds::new(1).with_max_wall_clock(Duration::ZERO));

        assert!(!budget.enter(&ToyState::Root, 0));
        assert_eq!(
            budget.summary().completion(),
            super::ExplorationCompletion::WallClockLimit
        );
        assert_eq!(budget.summary().reached_depth(), 0);
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
    pub(super) fn from_hash(state: &impl Hash) -> Self {
        let mut hasher = StateKeyHasher::new();
        state.hash(&mut hasher);
        hasher.finish_key()
    }

    pub(super) fn from_protocol_state(state: &impl StateIdentity) -> Self {
        let mut hasher = StateKeyHasher::new();
        state.hash_protocol_state(&mut hasher);
        hasher.finish_key()
    }
}

pub(in crate::model_check) fn protocol_state_fingerprint(
    state: &ExplorationState,
) -> (u64, u64, u64) {
    let key = StateKey::from_protocol_state(state);
    (key.len, key.hash_a, key.hash_b)
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

    pub(super) fn write_bytes(&mut self, bytes: &[u8]) {
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
