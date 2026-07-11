use super::super::super::helpers::three_node_configs;
use super::super::super::liveness;
use super::super::super::{run_raft_random_soak, SoakActionKind, SoakConfig};
use crate::SimSeed;

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
