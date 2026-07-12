use rafter::NodeId;

use super::super::super::helpers::{elect_node_one, three_node_configs};
use super::super::super::state::restart_node;
use super::super::super::state::ExplorationState;
use super::super::super::{run_raft_random_soak, SoakActionKind, SoakConfig};
use crate::{Cluster, SimSeed};

#[test]
fn randomized_raft_soak_fast_profile_is_deterministic() {
    let config = SoakConfig::new(SimSeed(0x9095), 96)
        .with_max_proposals(8)
        .with_max_restarts(4);
    let first = run_raft_random_soak(three_node_configs(), config)
        .expect("deterministic random soak should preserve Raft invariants");
    let second = run_raft_random_soak(three_node_configs(), config)
        .expect("same seed should preserve Raft invariants again");

    assert_eq!(first, second);
    assert_eq!(first.seed(), SimSeed(0x9095));
    assert_eq!(first.steps_executed(), 96);
    for kind in [
        SoakActionKind::Tick,
        SoakActionKind::Propose,
        SoakActionKind::Deliver,
        SoakActionKind::Delay,
        SoakActionKind::Drop,
        SoakActionKind::Duplicate,
        SoakActionKind::Restart,
    ] {
        assert!(
            first.observed_actions().contains(&kind),
            "fast soak should observe {kind:?}"
        );
    }
}

#[test]
fn ordinary_restart_preserves_durable_state_digest() {
    let mut cluster = Cluster::new(three_node_configs());
    elect_node_one(&mut cluster);
    cluster.propose(NodeId(1), b"digest-restart".to_vec());
    cluster.deliver_all();
    let mut state = ExplorationState::new(cluster);
    let before = state.cluster().durable_state_digest(NodeId(1));

    restart_node(&mut state, NodeId(1), &[]).expect("ordinary restart must preserve digest");

    assert_eq!(state.cluster().durable_state_digest(NodeId(1)), before);
}

#[test]
fn randomized_soak_exercises_repeated_restarts_across_nodes() {
    let config = SoakConfig::new(SimSeed(0x0725_7a17), 180)
        .with_max_proposals(12)
        .with_max_restarts(10);
    let summary = run_raft_random_soak(three_node_configs(), config)
        .expect("restart-heavy random soak should preserve Raft invariants");

    assert!(
        summary.action_count(SoakActionKind::Restart) >= 8,
        "restart-heavy soak should perform repeated restarts"
    );
    assert_eq!(
        summary.restarted_nodes().len(),
        3,
        "restart-heavy soak should restart arbitrary nodes"
    );
}
