use std::collections::BTreeSet;

use rafter::NodeId;

use super::super::super::helpers::{elect_node_one, four_node_future_learner_configs};
use super::super::super::scheduling::enabled_soak_actions;
use super::super::super::state::ExplorationState;
use super::super::super::{run_raft_random_soak, SoakActionKind, SoakConfig};
use crate::{Cluster, SimSeed};

#[test]
fn randomized_membership_soak_exercises_dynamic_membership_actions() {
    let config = SoakConfig::new(SimSeed(0x6c35_ea5e), 320)
        .with_max_proposals(8)
        .with_max_restarts(4)
        .with_max_read_indexes(4)
        .with_max_membership_changes(8)
        .with_max_partitions(4)
        .with_tick_skew(NodeId(1), 3);
    let summary = run_raft_random_soak(four_node_future_learner_configs(), config)
        .expect("membership soak should preserve Raft invariants");

    for kind in [
        SoakActionKind::AddLearner,
        SoakActionKind::RemoveLearner,
        SoakActionKind::PromoteLearner,
        SoakActionKind::RemoveVoter,
        SoakActionKind::EnterJoint,
    ] {
        assert!(
            summary.observed_actions().contains(&kind),
            "membership soak should observe {kind:?}"
        );
    }
}

#[test]
fn later_commit_does_not_retroactively_fail_nightly_leader_completeness() {
    let config = SoakConfig::new(SimSeed(0x1f12_1013_6bdc_c08b), 1024)
        .with_max_proposals(64)
        .with_max_restarts(32)
        .with_max_read_indexes(4)
        .with_max_membership_changes(16)
        .with_max_transfers(2)
        .with_max_partitions(2)
        .with_max_lossy_restarts(2)
        .with_snapshot_catchup_probe()
        .with_tick_skew(NodeId(1), 3);

    run_raft_random_soak(four_node_future_learner_configs(), config)
        .expect("commit-time provenance must preserve the replayed nightly trace");
}

#[test]
fn enabled_membership_soak_actions_cover_joint_transition_phases() {
    let mut cluster = Cluster::new(four_node_future_learner_configs());
    elect_node_one(&mut cluster);
    let base_state = ExplorationState::new(cluster.clone());
    let base_kinds = enabled_soak_kinds(&base_state);
    for kind in [
        SoakActionKind::AddLearner,
        SoakActionKind::RemoveVoter,
        SoakActionKind::EnterJoint,
    ] {
        assert!(
            base_kinds.contains(&kind),
            "base membership state should enable {kind:?}"
        );
    }

    cluster.add_learner(NodeId(1), NodeId(4));
    cluster.deliver_all();
    let learner_state = ExplorationState::new(cluster.clone());
    let learner_kinds = enabled_soak_kinds(&learner_state);
    for kind in [
        SoakActionKind::RemoveLearner,
        SoakActionKind::PromoteLearner,
    ] {
        assert!(
            learner_kinds.contains(&kind),
            "learner membership state should enable {kind:?}"
        );
    }

    let promotion_barrier = cluster
        .promotion_barrier(NodeId(1), NodeId(4))
        .expect("caught-up learner should have a promotion barrier");
    cluster.promote_learner(NodeId(1), NodeId(4), promotion_barrier);
    cluster.deliver_all();
    let joint_state = ExplorationState::new(cluster);
    let joint_kinds = enabled_soak_kinds(&joint_state);
    assert!(
        joint_kinds.contains(&SoakActionKind::LeaveJoint),
        "joint membership state should enable leave-joint"
    );
}

fn enabled_soak_kinds(state: &ExplorationState) -> BTreeSet<SoakActionKind> {
    enabled_soak_actions(
        state,
        SoakConfig::new(SimSeed(0xfeed), 1).with_max_membership_changes(1),
    )
    .into_iter()
    .map(|action| action.trace.kind())
    .collect()
}
