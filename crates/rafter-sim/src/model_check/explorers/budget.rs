use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    time::Instant,
};

use super::super::{Bounds, Summary};

#[derive(Debug)]
pub(super) struct ExplorationBudget {
    pub(super) bounds: Bounds,
    started_at: Instant,
    best_remaining_depth_by_state: BTreeMap<StateKey, usize>,
    unique_states: BTreeSet<StateKey>,
    explored_states: usize,
    explored_actions: usize,
}

impl ExplorationBudget {
    pub(super) fn new(bounds: Bounds) -> Self {
        Self {
            bounds,
            started_at: Instant::now(),
            best_remaining_depth_by_state: BTreeMap::new(),
            unique_states: BTreeSet::new(),
            explored_states: 0,
            explored_actions: 0,
        }
    }

    pub(super) fn summary(&self) -> Summary {
        Summary {
            explored_states: self.explored_states,
            unique_states: self.unique_states.len(),
            explored_actions: self.explored_actions,
            max_depth: self.bounds.depth,
        }
    }

    pub(super) fn enter(&mut self, state: &impl Hash, depth: usize) -> bool {
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

    pub(super) fn record_action(&mut self) {
        self.explored_actions += 1;
    }

    pub(super) fn wall_clock_exhausted(&self) -> bool {
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
    pub(super) fn from_hash(state: &impl Hash) -> Self {
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
