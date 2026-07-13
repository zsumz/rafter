use rafter::{
    BootstrapLogEntry, BootstrapState, CommittedConfiguration, ConfigurationEntry, ConfigurationId,
    LogIndex, MembershipConfig, MembershipSet, Message, NodeId, RaftSnapshot, SharedPayload,
    SnapshotCommittedConfiguration, Term,
};

use crate::Cluster;

use super::super::helpers::{
    bootstrap_with_snapshot, elect_node_one_with_node_three_in_state, large_snapshot_payload,
    test_snapshot, test_snapshot_with_committed_membership, three_node_configs,
};
use super::super::scheduling::SoakOperation;
use super::ExplorationState;
use super::{apply_snapshot_bootstrap_seeds, apply_soak_action, SnapshotBootstrapSeed};

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
        let payload = large_snapshot_payload();
        let (mut snapshot, _) = committed_membership.as_ref().map_or_else(
            || test_snapshot(1, 2, 1, 2, &payload),
            |membership| {
                test_snapshot_with_committed_membership(1, 2, 1, 2, &payload, membership.clone())
            },
        );
        let configuration = committed_membership.map(|membership| {
            let config_id = ConfigurationId(7);
            let entry = match &membership {
                MembershipConfig::Stable(stable) => {
                    ConfigurationEntry::stable(config_id, stable.clone())
                }
                MembershipConfig::Joint(joint) => {
                    ConfigurationEntry::joint(config_id, joint.clone())
                }
            };
            let committed = CommittedConfiguration {
                index: LogIndex(1),
                config_id,
            };
            snapshot = RaftSnapshot::from_payload(
                snapshot.metadata.clone().with_committed_configuration(
                    SnapshotCommittedConfiguration::new(Some(committed), membership),
                ),
                &payload,
            );
            (entry, committed)
        });
        let visible_prefix = witnessed_prefix_bootstrap(Term(2), &payload, configuration.as_ref());
        cluster
            .restart_node_from_bootstrap(NodeId(1), visible_prefix.clone())
            .expect("visible leader bootstrap is valid");
        cluster
            .restart_node_from_bootstrap(
                NodeId(2),
                divergent_prefix_bootstrap(Term(2), configuration.as_ref()),
            )
            .expect("divergent follower bootstrap is valid");
        cluster
            .restart_node_from_bootstrap(NodeId(3), visible_prefix)
            .expect("visible voter bootstrap is valid");
        let mut state = ExplorationState::new(cluster);
        apply_snapshot_bootstrap_seeds(
            &mut state,
            [NodeId(1), NodeId(3)]
                .into_iter()
                .map(|node_id| SnapshotBootstrapSeed {
                    node_id,
                    snapshot: snapshot.clone(),
                    payload: payload.clone(),
                    bootstrap: bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]),
                })
                .collect(),
        )
        .expect("compacted voter bootstrap is valid");
        state.witness_seeded_commit_authority(LogIndex::ZERO, LogIndex(2), Term(2));
        elect_node_one_with_node_three_in_state(&mut state);
        while let Some(position) = state.cluster().network.iter().position(|queued| {
            matches!(
                queued.envelope.message,
                Message::RequestVote(_) | Message::RequestVoteResponse(_)
            ) || queued.envelope.from == NodeId(3)
                || queued.envelope.to == NodeId(3)
        }) {
            apply_soak_action(&mut state, SoakOperation::DropAt(position));
        }

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

fn witnessed_prefix_bootstrap(
    current_term: Term,
    snapshot_payload: &[u8],
    configuration: Option<&(ConfigurationEntry, CommittedConfiguration)>,
) -> BootstrapState {
    let first = configuration.map_or_else(
        || BootstrapLogEntry::application(LogIndex(1), Term(1), b"old prefix".to_vec()),
        |(entry, _)| BootstrapLogEntry::configuration(LogIndex(1), Term(1), entry.clone()),
    );
    BootstrapState {
        current_term,
        voted_for: None,
        commit_index: LogIndex(2),
        committed_configuration: configuration.map(|(_, committed)| *committed),
        snapshot: None,
        log: vec![
            first,
            BootstrapLogEntry::application(LogIndex(2), Term(1), snapshot_payload.to_vec()),
        ],
    }
}

fn divergent_prefix_bootstrap(
    current_term: Term,
    configuration: Option<&(ConfigurationEntry, CommittedConfiguration)>,
) -> BootstrapState {
    let first = configuration.map_or_else(
        || BootstrapLogEntry::application(LogIndex(1), Term(1), b"old prefix".to_vec()),
        |(entry, _)| BootstrapLogEntry::configuration(LogIndex(1), Term(1), entry.clone()),
    );
    BootstrapState {
        current_term,
        voted_for: None,
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
        snapshot: None,
        log: vec![
            first,
            BootstrapLogEntry::application(LogIndex(2), Term(2), b"divergent boundary".to_vec()),
            BootstrapLogEntry::application(LogIndex(3), Term(2), b"divergent suffix".to_vec()),
        ],
    }
}
