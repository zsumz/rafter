use rafter::{MembershipConfig, MembershipSet, Message, NodeId, RaftSnapshot, SharedPayload, Term};

use crate::Cluster;

use super::super::helpers::{
    bootstrap_state, bootstrap_with_snapshot, elect_node_one_with_node_three_in_state,
    large_snapshot_payload, test_snapshot, test_snapshot_with_committed_membership,
    three_node_configs,
};
use super::ExplorationState;

/// The snapshot every healthy node must converge on: the descriptor the
/// kernel tracks plus the payload bytes the content invariants compare,
/// which the kernel no longer carries.
#[derive(Clone, Debug, Hash)]
pub(crate) struct ExpectedSnapshot {
    pub(crate) snapshot: RaftSnapshot,
    pub(crate) payload: SharedPayload,
}

#[derive(Clone, Debug, Hash)]
pub(crate) struct RestartSnapshotState {
    pub(crate) state: ExplorationState,
    pub(crate) expected_snapshot: Option<ExpectedSnapshot>,
    pub(crate) divergent_payloads: Vec<SharedPayload>,
}

impl RestartSnapshotState {
    pub(crate) fn new(state: ExplorationState) -> Self {
        Self {
            state,
            expected_snapshot: None,
            divergent_payloads: Vec::new(),
        }
    }

    pub(crate) fn snapshot_transfer() -> Self {
        Self::snapshot_transfer_with_committed_membership(None)
    }

    pub(crate) fn joint_snapshot_transfer() -> Self {
        let old = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("old snapshot membership is valid");
        let new = MembershipSet::new(vec![NodeId(1), NodeId(3)], Vec::new())
            .expect("new snapshot membership is valid");
        Self::snapshot_transfer_with_committed_membership(Some(MembershipConfig::joint(old, new)))
    }

    fn snapshot_transfer_with_committed_membership(
        committed_membership: Option<MembershipConfig>,
    ) -> Self {
        let mut cluster = Cluster::new(three_node_configs());
        let (snapshot, payload) = test_snapshot(1, 2, 1, 2, &large_snapshot_payload());
        let (snapshot, payload) = if let Some(membership) = committed_membership {
            test_snapshot_with_committed_membership(1, 2, 1, 2, &payload, membership)
        } else {
            (snapshot, payload)
        };
        let visible_prefix = &[
            (1, Term(1), b"old prefix".as_slice()),
            (2, Term(1), b"snapshot boundary".as_slice()),
        ];
        cluster
            .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(2), visible_prefix))
            .expect("visible leader bootstrap is valid");
        cluster
            .restart_node_from_bootstrap(
                NodeId(2),
                bootstrap_state(
                    Term(2),
                    &[
                        (1, Term(1), b"old prefix"),
                        (2, Term(2), b"divergent boundary"),
                        (3, Term(2), b"divergent suffix"),
                    ],
                ),
            )
            .expect("divergent follower bootstrap is valid");
        cluster
            .restart_node_from_bootstrap(NodeId(3), bootstrap_state(Term(2), visible_prefix))
            .expect("visible voter bootstrap is valid");
        let mut state = ExplorationState::new(cluster);
        for node_id in [NodeId(1), NodeId(3)] {
            state
                .cluster
                .seed_snapshot_payload(node_id, &snapshot, payload.clone());
            state
                .cluster
                .restart_node_from_bootstrap(
                    node_id,
                    bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]),
                )
                .expect("compacted voter bootstrap is valid");
        }
        state.refresh_log_history();
        state.refresh_seeded_commit_history();
        elect_node_one_with_node_three_in_state(&mut state);
        state.cluster.drop_matching(|envelope| {
            matches!(
                envelope.message,
                Message::RequestVote(_) | Message::RequestVoteResponse(_)
            ) || envelope.from == NodeId(3)
                || envelope.to == NodeId(3)
        });

        Self {
            state,
            expected_snapshot: Some(ExpectedSnapshot {
                snapshot,
                payload: payload.into(),
            }),
            divergent_payloads: vec![
                b"divergent boundary".to_vec().into(),
                b"divergent suffix".to_vec().into(),
            ],
        }
    }
}
