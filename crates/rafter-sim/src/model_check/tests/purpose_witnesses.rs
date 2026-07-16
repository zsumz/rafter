use rafter::{NodeConfig, NodeId};

use crate::Cluster;

use super::super::{
    explorers::RestartSafetyExplorer,
    helpers::{deliver_all_in_state, elect_node_one_in_state},
    observations::Observation,
    scheduling::Operation,
    state::{apply_to_state, ExplorationState, RestartSnapshotState},
    Bounds, ProposalId,
};

#[test]
fn production_commit_witness_requires_effective_production_features() {
    let observed = committed_production_state(production_configs());
    assert!(observed
        .observation_set()
        .contains(Observation::ProductionConfigCommitObserved));

    let disabled = production_configs()
        .into_iter()
        .map(|config| config.with_check_quorum(false))
        .collect();
    let mutated = committed_production_state(disabled);
    assert!(mutated
        .observation_set()
        .contains(Observation::CommitFloorAdvances));
    assert!(!mutated
        .observation_set()
        .contains(Observation::ProductionConfigCommitObserved));
}

#[test]
fn window_one_witness_requires_a_full_single_batch_window() {
    let window_one = state_after_two_blocked_proposals(1);
    assert!(window_one
        .observation_set()
        .contains(Observation::WindowOneBackpressureObserved));

    let window_two = state_after_two_blocked_proposals(2);
    assert!(!window_two
        .observation_set()
        .contains(Observation::WindowOneBackpressureObserved));
}

#[test]
fn lease_fast_path_witness_requires_an_effective_active_lease() {
    let with_lease = state_after_read(production_configs_with_lease(true));
    assert!(with_lease
        .observation_set()
        .contains(Observation::LeaseFastPathReadGranted));

    let without_lease = state_after_read(production_configs_with_lease(false));
    assert!(!without_lease
        .observation_set()
        .contains(Observation::LeaseFastPathReadGranted));
}

#[test]
fn joint_recovery_witness_rejects_stable_membership_mutation() {
    let state = RestartSnapshotState::stable_snapshot_transfer()
        .expect("stable snapshot fixture membership is valid");
    let mut explorer = RestartSafetyExplorer::new(Bounds::new(12).with_max_restarts(1));
    let mut trace = Vec::new();
    explorer
        .explore(&state, &mut trace, 0)
        .expect("stable-membership mutation should remain protocol-safe");
    let summary = explorer.summary();

    assert!(explorer.observed_restart);
    assert!(explorer.observed_installed_snapshot);
    assert!(!summary
        .observations
        .contains(Observation::JointConfigRestartSnapshotRecovered));
}

fn committed_production_state(configs: Vec<NodeConfig>) -> ExplorationState {
    let mut state = ExplorationState::new(Cluster::new(configs));
    elect_node_one_in_state(&mut state);
    apply_to_state(
        &mut state,
        Operation::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
            stale_leader: false,
        },
    );
    deliver_all_in_state(&mut state);
    state
}

fn state_after_two_blocked_proposals(max_inflight_appends: usize) -> ExplorationState {
    let configs = minimal_configs()
        .into_iter()
        .map(|config| config.with_max_inflight_appends(max_inflight_appends))
        .collect();
    let mut state = ExplorationState::new(Cluster::new(configs));
    elect_node_one_in_state(&mut state);
    for proposal_id in [1, 2] {
        apply_to_state(
            &mut state,
            Operation::Propose {
                to: NodeId(1),
                proposal_id: ProposalId(proposal_id),
                stale_leader: false,
            },
        );
    }
    state
}

fn state_after_read(configs: Vec<NodeConfig>) -> ExplorationState {
    let mut state = committed_production_state(configs);
    assert!(
        state.cluster().read_lease_active(NodeId(1))
            || !state.cluster().configs[&NodeId(1)].lease_reads()
    );
    apply_to_state(
        &mut state,
        Operation::ReadIndex {
            to: NodeId(1),
            request_id: 1,
        },
    );
    state
}

fn production_configs() -> Vec<NodeConfig> {
    configs(|config| config)
}

fn production_configs_with_lease(enabled: bool) -> Vec<NodeConfig> {
    configs(|config| config.with_lease_reads(enabled))
}

fn minimal_configs() -> Vec<NodeConfig> {
    configs(|config| config.with_pre_vote(false).with_check_quorum(false))
}

fn configs(mut configure: impl FnMut(NodeConfig) -> NodeConfig) -> Vec<NodeConfig> {
    [(1, [2, 3]), (2, [1, 3]), (3, [1, 2])]
        .into_iter()
        .map(|(id, peers)| {
            configure(
                NodeConfig::new(NodeId(id), peers.into_iter().map(NodeId).collect(), 3)
                    .expect("purpose-witness config should be valid"),
            )
        })
        .collect()
}
