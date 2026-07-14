use std::{
    hash::{Hash, Hasher},
    time::Duration,
};

use rafter::{LogIndex, NodeConfig, NodeId};

use super::{
    Bounds, ExactStateIdentity, ExplorationBudget, ExplorationState, StateIdentity, StateKey,
};
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
fn compact_key_collision_retains_distinct_exact_states() {
    let first = ToyVerifierState {
        protocol: 7,
        verifier_history: 1,
    };
    let second = ToyVerifierState {
        protocol: 8,
        verifier_history: 2,
    };
    let first_verifier = ExactStateIdentity::from_hash(&first);
    let first_protocol = ExactStateIdentity::from_protocol_state(&first);
    let mut second_verifier = ExactStateIdentity::from_hash(&second);
    let mut second_protocol = ExactStateIdentity::from_protocol_state(&second);

    assert_ne!(first_verifier.canonical, second_verifier.canonical);
    assert_ne!(first_protocol.canonical, second_protocol.canonical);
    second_verifier.key = first_verifier.key;
    second_protocol.key = first_protocol.key;

    let mut budget = ExplorationBudget::new(Bounds::new(1));
    assert!(budget.enter_with_identities(
        super::ObservationSet::default(),
        first_verifier.clone(),
        first_protocol.clone(),
        0,
    ));
    assert!(budget.enter_with_identities(
        super::ObservationSet::default(),
        second_verifier,
        second_protocol,
        0,
    ));
    assert!(!budget.enter_with_identities(
        super::ObservationSet::default(),
        first_verifier,
        first_protocol,
        0,
    ));

    let summary = budget.summary();
    assert_eq!(summary.explored_states(), 3);
    assert_eq!(summary.unique_verifier_states(), 2);
    assert_eq!(summary.unique_protocol_states(), 2);
    assert_eq!(budget.verifier_states.len(), 1);
    assert_eq!(budget.protocol_states.len(), 1);
    assert_eq!(budget.verifier_states.values().next().unwrap().len(), 2);
    assert_eq!(budget.protocol_states.values().next().unwrap().len(), 2);
}

#[test]
fn canonical_identity_zero_run_encoding_is_injective() {
    fn encoded(bytes: &[u8]) -> super::CanonicalStateIdentity {
        let mut hasher = super::ExactStateIdentityHasher::new();
        hasher.write(bytes);
        hasher.finish_identity().canonical
    }

    assert_ne!(encoded(&[1, 0, 2]), encoded(&[1, 0, 0, 2]));
    assert_ne!(encoded(&[1, 0, 128]), encoded(&[1, 128, 0]));
    assert_eq!(encoded(&[1, 0, 0, 2]), encoded(&[1, 0, 0, 2]));
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
