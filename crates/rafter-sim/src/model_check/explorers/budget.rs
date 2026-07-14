use std::{
    collections::BTreeMap,
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
    verifier_states: BTreeMap<StateKey, Vec<VerifierStateIdentity>>,
    protocol_states: BTreeMap<StateKey, Vec<CanonicalStateIdentity>>,
    unique_verifier_states: usize,
    unique_protocol_states: usize,
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
            verifier_states: BTreeMap::new(),
            protocol_states: BTreeMap::new(),
            unique_verifier_states: 0,
            unique_protocol_states: 0,
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
            unique_states: self.unique_verifier_states,
            unique_protocol_states: self.unique_protocol_states,
            explored_actions: self.explored_actions,
            configured_depth: self.bounds.depth,
            reached_depth: self.reached_depth,
            completion: self.completion,
            observations: self.observations,
        }
    }

    pub(super) fn enter(&mut self, state: &impl StateIdentity, depth: usize) -> bool {
        let verifier_identity = ExactStateIdentity::from_hash(state);
        let protocol_identity = ExactStateIdentity::from_protocol_state(state);
        self.enter_with_identities(
            state.observations(),
            verifier_identity,
            protocol_identity,
            depth,
        )
    }

    fn enter_with_identities(
        &mut self,
        observations: ObservationSet,
        verifier_identity: ExactStateIdentity,
        protocol_identity: ExactStateIdentity,
        depth: usize,
    ) -> bool {
        self.explored_states += 1;
        self.observations.union_with(observations);
        if self.wall_clock_exhausted() {
            self.completion = ExplorationCompletion::WallClockLimit;
            return false;
        }

        let remaining_depth = self.bounds.depth.saturating_sub(depth);
        if let Some(identity) = self
            .verifier_states
            .get_mut(&verifier_identity.key)
            .and_then(|bucket| {
                bucket
                    .iter_mut()
                    .find(|seen| seen.canonical == verifier_identity.canonical)
            })
        {
            if identity.best_remaining_depth >= remaining_depth {
                return false;
            }
            identity.best_remaining_depth = remaining_depth;
            self.reached_depth = self.reached_depth.max(depth);
            return true;
        }

        if self
            .bounds
            .max_unique_states
            .is_some_and(|max| self.unique_verifier_states >= max)
        {
            self.completion = ExplorationCompletion::UniqueStateLimit;
            return false;
        }

        self.verifier_states
            .entry(verifier_identity.key)
            .or_default()
            .push(VerifierStateIdentity {
                canonical: verifier_identity.canonical,
                best_remaining_depth: remaining_depth,
            });
        self.unique_verifier_states += 1;

        let protocol_bucket = self
            .protocol_states
            .entry(protocol_identity.key)
            .or_default();
        if !protocol_bucket.contains(&protocol_identity.canonical) {
            protocol_bucket.push(protocol_identity.canonical);
            self.unique_protocol_states += 1;
        }
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
mod tests;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct StateKey {
    len: u64,
    hash_a: u64,
    hash_b: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExactStateIdentity {
    key: StateKey,
    canonical: CanonicalStateIdentity,
}

impl ExactStateIdentity {
    fn from_hash(state: &impl Hash) -> Self {
        let mut hasher = ExactStateIdentityHasher::new();
        state.hash(&mut hasher);
        hasher.finish_identity()
    }

    fn from_protocol_state(state: &impl StateIdentity) -> Self {
        let mut hasher = ExactStateIdentityHasher::new();
        state.hash_protocol_state(&mut hasher);
        hasher.finish_identity()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
// The model states use structural `Hash` implementations as their canonical
// representation. This is a lossless zero-run encoding of that complete byte
// stream; `StateKey` is only a compact index into collision buckets.
struct CanonicalStateIdentity(Box<[u8]>);

#[derive(Debug)]
struct VerifierStateIdentity {
    canonical: CanonicalStateIdentity,
    best_remaining_depth: usize,
}

impl StateKey {
    #[cfg(test)]
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

struct ExactStateIdentityHasher {
    key: StateKeyHasher,
    canonical: Vec<u8>,
    pending_zeros: usize,
}

impl ExactStateIdentityHasher {
    const fn new() -> Self {
        Self {
            key: StateKeyHasher::new(),
            canonical: Vec::new(),
            pending_zeros: 0,
        }
    }

    fn finish_identity(mut self) -> ExactStateIdentity {
        self.flush_zeros();
        ExactStateIdentity {
            key: self.key.finish_key(),
            canonical: CanonicalStateIdentity(self.canonical.into_boxed_slice()),
        }
    }

    fn flush_zeros(&mut self) {
        if self.pending_zeros == 0 {
            return;
        }

        self.canonical.push(0);
        let mut remaining = self.pending_zeros;
        while remaining >= 0x80 {
            let chunk = (remaining & 0x7f).to_le_bytes()[0];
            self.canonical.push(chunk | 0x80);
            remaining >>= 7;
        }
        self.canonical.push(remaining.to_le_bytes()[0]);
        self.pending_zeros = 0;
    }
}

impl Hasher for ExactStateIdentityHasher {
    fn finish(&self) -> u64 {
        self.key.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.key.write_bytes(bytes);
        for byte in bytes {
            if *byte == 0 {
                if self.pending_zeros == usize::MAX {
                    self.flush_zeros();
                }
                self.pending_zeros += 1;
            } else {
                self.flush_zeros();
                self.canonical.push(*byte);
            }
        }
    }
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
