//! Depth-aware state deduplication and the limits an exploration runs under.
//!
//! A state already seen is re-expanded only when reached with more depth
//! remaining, so deduplication never hides a descendant a shorter path could
//! reach. Verifier and protocol identities are counted apart from the exact
//! canonical state, and each cap reports the completion that stopped the run.

use std::{
    collections::BTreeMap,
    hash::{Hash, Hasher},
    time::Instant,
};

use super::super::{
    observations::ObservationSet, state::ExplorationState, Bounds, ExplorationCompletion,
    RestartSnapshotState, Summary,
};

mod identity;

pub(in crate::model_check) use identity::protocol_state_fingerprint;

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
