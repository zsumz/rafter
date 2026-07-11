use std::collections::BTreeSet;

use super::super::super::helpers::three_node_configs;
use super::super::super::liveness;
use super::super::super::{
    run_raft_random_soak, soak::SoakAction, state::ExplorationState, SoakActionKind, SoakConfig,
};
use crate::{Cluster, SimSeed};

#[derive(Clone, Copy, Debug)]
struct HistoryCounts {
    elections: usize,
    log_prefixes: usize,
    committed_prefixes: usize,
    commit_certificates: usize,
}

impl HistoryCounts {
    fn from_state(state: &ExplorationState) -> Self {
        Self {
            elections: state.election_history.elected_by_term.len(),
            log_prefixes: state.logical_log_history.prefixes_by_index_term.len(),
            committed_prefixes: state.commit_history.committed_prefixes.len(),
            commit_certificates: state.commit_history.certificates.len(),
        }
    }
}

#[test]
fn randomized_soak_liveness_phase_elects_leader_and_commits_probe() {
    let summary = run_raft_random_soak(three_node_configs(), SoakConfig::new(SimSeed(0x11_5e), 0))
        .expect("post-soak liveness phase should elect and commit without random steps");

    assert_eq!(summary.steps_executed(), 0);
    for kind in [
        SoakActionKind::Tick,
        SoakActionKind::Deliver,
        SoakActionKind::Propose,
    ] {
        assert!(
            summary.observed_actions().contains(&kind),
            "liveness phase should observe {kind:?}"
        );
    }
}

#[test]
fn randomized_soak_liveness_phase_completes_read_barrier_probe() {
    let summary = run_raft_random_soak(
        three_node_configs(),
        SoakConfig::new(SimSeed(0x11_5e), 0).with_max_read_indexes(1),
    )
    .expect("post-soak liveness phase should complete a read barrier");

    assert!(
        summary
            .observed_actions()
            .contains(&SoakActionKind::ReadIndex),
        "read-barrier liveness phase should observe a read-index action"
    );
}

#[test]
fn randomized_soak_liveness_phase_completes_membership_transition_probe() {
    let summary = run_raft_random_soak(
        three_node_configs(),
        SoakConfig::new(SimSeed(0x11_5e), 0).with_max_membership_changes(1),
    )
    .expect("post-soak liveness phase should complete a membership transition");

    for kind in [SoakActionKind::RemoveVoter, SoakActionKind::LeaveJoint] {
        assert!(
            summary.observed_actions().contains(&kind),
            "membership-transition liveness phase should observe {kind:?}"
        );
    }
}

#[test]
fn randomized_soak_liveness_phase_completes_leadership_transfer_probe() {
    let summary = run_raft_random_soak(
        three_node_configs(),
        SoakConfig::new(SimSeed(0x11_5e), 0).with_max_transfers(1),
    )
    .expect("post-soak liveness phase should complete a leadership transfer");

    assert!(
        summary
            .observed_actions()
            .contains(&SoakActionKind::Transfer),
        "leadership-transfer liveness phase should observe a transfer action"
    );
}

#[test]
fn liveness_phase_updates_p0_histories_through_instrumented_transitions() {
    let config = SoakConfig::new(SimSeed(0x11_5e), 0).with_max_transfers(1);
    let mut state =
        ExplorationState::new(Cluster::new_with_seed(three_node_configs(), config.seed));
    let before = HistoryCounts::from_state(&state);
    let mut trace = Vec::<SoakAction>::new();
    let mut observed_actions = BTreeSet::new();

    liveness::run_soak_liveness_check(&mut state, config, &mut trace, &mut observed_actions)
        .expect("instrumented liveness phase should converge");

    let after = HistoryCounts::from_state(&state);
    assert!(
        after.elections > before.elections,
        "liveness convergence should record election certificates"
    );
    assert!(
        after.log_prefixes > before.log_prefixes,
        "liveness convergence should refresh logical-log prefix witnesses"
    );
    assert!(
        after.committed_prefixes > before.committed_prefixes,
        "liveness convergence should refresh committed-prefix history"
    );
    assert!(
        after.commit_certificates > before.commit_certificates,
        "liveness convergence should record commit certificates"
    );
    assert!(
        observed_actions.contains(&SoakActionKind::Transfer),
        "liveness phase should exercise transfer instrumentation"
    );

    let target = state
        .cluster
        .leaders()
        .into_iter()
        .min_by_key(|node_id| node_id.0)
        .expect("transfer liveness should leave a leader");
    assert!(
        state
            .election_history
            .elected_by_term
            .values()
            .any(|certificate| certificate.leader_id == target),
        "the leader elected during transfer liveness should receive an election certificate"
    );
}

#[test]
fn snapshot_catchup_liveness_monitor_installs_expected_snapshot() {
    liveness::run_snapshot_catchup_liveness_check(
        SoakConfig::new(SimSeed(0x5a_a9), 0).with_snapshot_catchup_probe(),
        256,
    )
    .expect("snapshot catch-up liveness monitor should install the snapshot");
}

#[test]
fn randomized_soak_liveness_phase_runs_snapshot_catchup_probe() {
    run_raft_random_soak(
        three_node_configs(),
        SoakConfig::new(SimSeed(0x11_5e), 0).with_snapshot_catchup_probe(),
    )
    .expect("post-soak liveness phase should run the snapshot catch-up probe");
}
