//! Seeded transitions observe their violations as they are applied.
//!
//! A pending-replay seed carrying a conflicting durable vote and a snapshot
//! seed carrying a term regression must each be observed immediately by the
//! transition engine, so a seeded state can never enter the explored set with
//! its violation unrecorded.

use rafter::{BootstrapLogEntry, BootstrapState, LogIndex, NodeConfig, NodeId, Term};

use crate::{Cluster, SimTick};

use super::{
    apply_pending_application_replay_seed, apply_snapshot_bootstrap_seeds,
    PendingApplicationReplaySeed, SnapshotBootstrapSeed,
};
use crate::model_check::{
    helpers::{bootstrap_with_snapshot, test_snapshot},
    invariants::check_election_history,
    state::ExplorationState,
};

#[test]
fn pending_replay_seed_observes_conflicting_durable_vote_immediately() {
    let mut state = new_state();
    apply_pending_application_replay_seed(
        &mut state,
        PendingApplicationReplaySeed {
            node_id: NodeId(1),
            bootstrap: pending_replay_bootstrap(Term(4), NodeId(2)),
        },
    )
    .expect("first pending replay seed is valid");

    apply_pending_application_replay_seed(
        &mut state,
        PendingApplicationReplaySeed {
            node_id: NodeId(1),
            bootstrap: pending_replay_bootstrap(Term(4), NodeId(3)),
        },
    )
    .expect("second pending replay seed is valid");

    let failure = check_election_history(&state, &[])
        .expect_err("the seed transition must record a same-term vote conflict");
    assert_eq!(failure.invariant(), "EL-02 one durable vote per term");
}

#[test]
fn snapshot_seed_observes_term_regression_immediately() {
    let mut state = new_state();
    for term in [Term(4), Term(3)] {
        let (snapshot, payload) = test_snapshot(1, 1, 1, term.0, b"seeded snapshot");
        apply_snapshot_bootstrap_seeds(
            &mut state,
            vec![SnapshotBootstrapSeed {
                node_id: NodeId(1),
                snapshot: snapshot.clone(),
                payload,
                bootstrap: bootstrap_with_snapshot(term, snapshot, &[]),
            }],
        )
        .expect("snapshot seed is valid");
    }

    let failure = check_election_history(&state, &[])
        .expect_err("the seed transition must record a term regression");
    assert_eq!(failure.invariant(), "EL-01 term monotonicity");
}

fn new_state() -> ExplorationState {
    let config =
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3).expect("test config is valid");
    let state = ExplorationState::new(Cluster::new(vec![config]));
    assert_eq!(state.cluster().clock().now(), SimTick(0));
    state
}

fn pending_replay_bootstrap(term: Term, voted_for: NodeId) -> BootstrapState {
    BootstrapState {
        current_term: term,
        voted_for: Some(voted_for),
        commit_index: LogIndex(1),
        committed_configuration: None,
        snapshot: None,
        log: vec![BootstrapLogEntry::application(
            LogIndex(1),
            term,
            b"pending replay".to_vec(),
        )],
    }
}
