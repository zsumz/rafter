//! Commit and snapshot witnesses for the semantic detector leg.
//!
//! These scenarios reach a post-append joint commit certificate and a pair of
//! snapshot installs that share one boundary, both of which the broad bounded
//! explorers reach only rarely.

use rafter::{LogIndex, MembershipSet, NodeId, Term};

use crate::Cluster;

use super::super::super::{
    explorers::{CommitSafetyExplorer, RestartSafetyExplorer},
    helpers::{
        bootstrap_state, bootstrap_with_snapshot, config, elect_node_one_in_state,
        elect_node_one_with_node_three_in_state, summarize, test_snapshot, three_node_configs,
    },
    observations::Observation,
    scheduling::Operation,
    state::{
        apply_snapshot_bootstrap_seeds, apply_to_restart_snapshot_state, apply_to_state,
        SnapshotBootstrapSeed,
    },
    state::{ExpectedSnapshot, ExplorationState, RestartSnapshotState},
    Bounds, Failure, Summary,
};

use super::{require_observation, witness_harness_error};

pub(super) fn post_append_joint_commit_summary() -> Result<Summary, Failure> {
    let mut state = ExplorationState::new(Cluster::new(vec![config(1, &[], 1)]));
    elect_node_one_in_state(&mut state);
    let target = MembershipSet::new(vec![NodeId(1)], vec![NodeId(2)]).map_err(|error| {
        witness_harness_error(format!("build single-voter target membership: {error:?}"))
    })?;
    apply_to_state(
        &mut state,
        Operation::EnterJoint {
            to: NodeId(1),
            target,
            promotion_barriers: Vec::new(),
        },
    );
    let state_summary = summarize(state.cluster());
    let mut explorer = CommitSafetyExplorer::new(Bounds::new(0));
    explorer.explore(&state, &mut Vec::new(), 0)?;
    require_observation(
        explorer.summary(),
        Observation::PostAppendJointCommitCertificates,
        state_summary,
    )
}

pub(super) fn same_boundary_snapshot_pair_summary() -> Result<Summary, Failure> {
    let mut state = snapshot_pair_state()?;
    for _ in 0..256 {
        if state.state.cluster().snapshot_installs().len() >= 2 {
            break;
        }
        if state.state.cluster().pending().next().is_none() {
            break;
        }
        apply_to_restart_snapshot_state(&mut state, Operation::DeliverReadyAt(0), &[])?;
    }
    let state_summary = summarize(state.state.cluster());
    let mut explorer = RestartSafetyExplorer::new(Bounds::new(0));
    explorer.explore(&state, &mut Vec::new(), 0)?;
    require_observation(
        explorer.summary(),
        Observation::SameBoundarySnapshotInstallPairs,
        state_summary,
    )
}

pub(super) fn snapshot_pair_state() -> Result<RestartSnapshotState, Failure> {
    let mut cluster = Cluster::new(three_node_configs());
    let (snapshot, payload) = test_snapshot(1, 2, 1, 2, b"snapshot boundary");
    let mut visible_prefix = bootstrap_state(
        Term(2),
        &[
            (1, Term(1), b"old prefix"),
            (2, Term(1), b"snapshot boundary"),
        ],
    );
    visible_prefix.commit_index = LogIndex(2);
    cluster
        .restart_node_from_bootstrap(NodeId(1), visible_prefix)
        .map_err(|error| witness_harness_error(format!("seed visible leader: {error:?}")))?;
    for node_id in [NodeId(2), NodeId(3)] {
        cluster
            .restart_node_from_bootstrap(
                node_id,
                bootstrap_state(Term(2), &[(1, Term(1), b"old prefix")]),
            )
            .map_err(|error| {
                witness_harness_error(format!("seed lagging follower {node_id}: {error:?}"))
            })?;
    }
    let mut state = ExplorationState::new(cluster);
    apply_snapshot_bootstrap_seeds(
        &mut state,
        vec![SnapshotBootstrapSeed {
            node_id: NodeId(1),
            snapshot: snapshot.clone(),
            payload: payload.clone(),
            bootstrap: bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]),
        }],
    )
    .map_err(|error| witness_harness_error(format!("seed compacted leader: {error:?}")))?;
    elect_node_one_with_node_three_in_state(&mut state);
    Ok(RestartSnapshotState {
        state,
        expected_snapshot: Some(ExpectedSnapshot {
            snapshot,
            payload: payload.into(),
        }),
        divergent_payloads: Vec::new(),
    })
}
