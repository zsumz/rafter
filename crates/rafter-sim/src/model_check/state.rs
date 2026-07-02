use std::collections::{BTreeMap, BTreeSet};

use rafter::{
    AppendEntries, BootstrapLogEntry, BootstrapState, CommittedConfiguration, ConfigurationEntry,
    ConfigurationId, JointMembership, LogIndex, MembershipConfig, MembershipSet, Message,
    NodeConfig, NodeId, RaftSnapshot, SharedPayload, Term,
};

use crate::Cluster;

use super::helpers::{
    bootstrap_state, bootstrap_with_snapshot, config, elect_node_one_with_node_three,
    large_snapshot_payload, test_snapshot, test_snapshot_with_committed_membership,
    three_node_configs,
};
use super::ProposalId;

#[derive(Clone, Debug, Hash)]
pub(super) struct ExplorationState {
    pub(super) cluster: Cluster,
    pub(super) proposals_issued: u64,
    pub(super) restarts_issued: u64,
    pub(super) read_indexes_issued: u64,
    pub(super) membership_changes_issued: u64,
    pub(super) transfers_issued: u64,
    pub(super) partitions_issued: u64,
    pub(super) lossy_restarts_issued: u64,
    pub(super) commit_floor_by_node: BTreeMap<NodeId, LogIndex>,
    pub(super) committed_configuration_floor_by_node:
        BTreeMap<NodeId, Option<CommittedConfiguration>>,
    pub(super) client_history: ClientHistory,
    pub(super) forbidden_applied_payloads: BTreeSet<SharedPayload>,
    pub(super) required_applied_payloads: BTreeMap<(NodeId, LogIndex), SharedPayload>,
    pub(super) required_committed_configurations:
        BTreeMap<(NodeId, LogIndex), CommittedConfiguration>,
    pub(super) required_commit_indexes: BTreeSet<(NodeId, LogIndex)>,
}

impl ExplorationState {
    pub(super) fn new(cluster: Cluster) -> Self {
        let commit_floor_by_node = cluster
            .nodes
            .iter()
            .map(|(node_id, node)| (*node_id, node.commit_index()))
            .collect();
        let committed_configuration_floor_by_node = cluster
            .nodes
            .iter()
            .map(|(node_id, node)| (*node_id, node.committed_configuration_state()))
            .collect();
        Self {
            cluster,
            proposals_issued: 0,
            restarts_issued: 0,
            read_indexes_issued: 0,
            membership_changes_issued: 0,
            transfers_issued: 0,
            partitions_issued: 0,
            lossy_restarts_issued: 0,
            commit_floor_by_node,
            committed_configuration_floor_by_node,
            client_history: ClientHistory::default(),
            forbidden_applied_payloads: BTreeSet::new(),
            required_applied_payloads: BTreeMap::new(),
            required_committed_configurations: BTreeMap::new(),
            required_commit_indexes: BTreeSet::new(),
        }
    }

    pub(super) fn seeded_low_empty_probe(configs: Vec<NodeConfig>) -> Self {
        let mut cluster = Cluster::new(configs);
        cluster
            .restart_node_from_bootstrap(
                NodeId(2),
                bootstrap_state(Term(1), &[(1, Term(1), b"committed-one")]),
            )
            .expect("pre-committed follower seed is valid");
        cluster.deliver_message(
            NodeId(1),
            NodeId(2),
            Message::AppendEntries(AppendEntries {
                term: Term(1),
                leader_id: NodeId(1),
                prev_log_index: LogIndex(1),
                prev_log_term: Term(1),
                sequence: 0,
                entries: Vec::new(),
                leader_commit: LogIndex(1),
            }),
        );
        cluster.drop_matching(|_| true);
        cluster.queue_message(
            NodeId(1),
            NodeId(2),
            Message::AppendEntries(AppendEntries {
                term: Term(1),
                leader_id: NodeId(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term(0),
                sequence: 1,
                entries: Vec::new(),
                leader_commit: LogIndex(3),
            }),
        );
        Self::new(cluster)
    }

    pub(super) fn seeded_divergent_suffix_probe(configs: Vec<NodeConfig>) -> Self {
        let mut cluster = Cluster::new(configs);
        let leader_entries = &[
            (1, Term(1), b"committed-one".as_slice()),
            (2, Term(2), b"leader-two".as_slice()),
        ];
        for node_id in [NodeId(1), NodeId(3)] {
            cluster
                .restart_node_from_bootstrap(node_id, bootstrap_state(Term(2), leader_entries))
                .expect("committed leader-side seed is valid");
            cluster.deliver_message(
                NodeId(1),
                node_id,
                Message::AppendEntries(AppendEntries {
                    term: Term(2),
                    leader_id: NodeId(1),
                    prev_log_index: LogIndex(2),
                    prev_log_term: Term(2),
                    sequence: 0,
                    entries: Vec::new(),
                    leader_commit: LogIndex(2),
                }),
            );
        }
        cluster
            .restart_node_from_bootstrap(
                NodeId(2),
                bootstrap_state(
                    Term(2),
                    &[
                        (1, Term(1), b"committed-one"),
                        (2, Term(2), b"divergent-two"),
                    ],
                ),
            )
            .expect("pre-diverged follower seed is valid");
        cluster.deliver_message(
            NodeId(1),
            NodeId(2),
            Message::AppendEntries(AppendEntries {
                term: Term(2),
                leader_id: NodeId(1),
                prev_log_index: LogIndex(1),
                prev_log_term: Term(1),
                sequence: 0,
                entries: Vec::new(),
                leader_commit: LogIndex(1),
            }),
        );
        cluster.drop_matching(|_| true);
        cluster.queue_message(
            NodeId(1),
            NodeId(2),
            Message::AppendEntries(AppendEntries {
                term: Term(2),
                leader_id: NodeId(1),
                prev_log_index: LogIndex(1),
                prev_log_term: Term(1),
                sequence: 1,
                entries: Vec::new(),
                leader_commit: LogIndex(2),
            }),
        );

        let mut state = Self::new(cluster);
        state
            .forbidden_applied_payloads
            .insert(b"divergent-two".to_vec().into());
        state
    }

    pub(super) fn seeded_single_voter_prior_application_noop() -> Self {
        let payload = b"leadership-noop-prior-app".to_vec();
        let mut cluster = Cluster::new(vec![config(1, &[], 1)]);
        cluster
            .restart_node_from_bootstrap(
                NodeId(1),
                bootstrap_state(Term(1), &[(1, Term(1), payload.as_slice())]),
            )
            .expect("single-voter prior application seed is valid");

        let mut state = Self::new(cluster);
        state.require_applied_payload(NodeId(1), LogIndex(1), payload.into());
        state
    }

    pub(super) fn seeded_single_voter_prior_configuration_noop() -> Self {
        let config_id = ConfigurationId(7);
        let membership =
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("membership is valid");
        let configuration = ConfigurationEntry::stable(config_id, membership);
        let mut cluster = Cluster::new(vec![config(1, &[], 1)]);
        cluster
            .restart_node_from_bootstrap(
                NodeId(1),
                BootstrapState {
                    current_term: Term(1),
                    voted_for: None,
                    commit_index: LogIndex::ZERO,
                    committed_configuration: None,
                    snapshot: None,
                    log: vec![BootstrapLogEntry::configuration(
                        LogIndex(1),
                        Term(1),
                        configuration,
                    )],
                },
            )
            .expect("single-voter prior configuration seed is valid");

        let mut state = Self::new(cluster);
        state.require_committed_configuration(
            NodeId(1),
            CommittedConfiguration {
                index: LogIndex(1),
                config_id,
            },
        );
        state
    }

    pub(super) fn seeded_joint_self_quorum_prior_application_noop() -> Self {
        let payload = b"joint-self-quorum-prior-app".to_vec();
        let config_id = ConfigurationId(9);
        let membership =
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("membership is valid");
        let configuration = ConfigurationEntry::joint(
            config_id,
            JointMembership::new(membership.clone(), membership),
        );
        let mut cluster = Cluster::new(vec![config(1, &[], 1)]);
        cluster
            .restart_node_from_bootstrap(
                NodeId(1),
                BootstrapState {
                    current_term: Term(1),
                    voted_for: None,
                    commit_index: LogIndex(1),
                    committed_configuration: Some(CommittedConfiguration {
                        index: LogIndex(1),
                        config_id,
                    }),
                    snapshot: None,
                    log: vec![
                        BootstrapLogEntry::configuration(LogIndex(1), Term(1), configuration),
                        BootstrapLogEntry::application(LogIndex(2), Term(1), payload.clone()),
                    ],
                },
            )
            .expect("joint self-quorum prior application seed is valid");

        let mut state = Self::new(cluster);
        state.require_applied_payload(NodeId(1), LogIndex(2), payload.into());
        state
    }

    pub(super) fn seeded_leadership_transfer_noop_commit() -> Self {
        let mut cluster = Cluster::new(three_node_configs());
        elect_node_one_with_node_three(&mut cluster);
        cluster.deliver_all();
        cluster.transfer_leadership(NodeId(1), NodeId(2));
        let mut state = Self::new(cluster);
        state.require_commit_index(NodeId(2), LogIndex(2));
        state
    }

    pub(super) fn refresh_commit_floors(&mut self) {
        for (node_id, node) in &self.cluster.nodes {
            let floor = self.commit_floor_by_node.entry(*node_id).or_default();
            *floor = (*floor).max(node.commit_index());
            let config_floor = self
                .committed_configuration_floor_by_node
                .entry(*node_id)
                .or_insert(None);
            if let Some(actual) = node.committed_configuration_state() {
                match config_floor {
                    None => *config_floor = Some(actual),
                    Some(floor) if actual.index > floor.index => *config_floor = Some(actual),
                    Some(_) => {}
                }
            }
        }
    }

    pub(super) fn record_client_proposal(
        &mut self,
        node_id: NodeId,
        proposal_id: ProposalId,
        stale_leader: bool,
    ) {
        let status = if stale_leader {
            ClientWriteStatus::Unknown {
                reason: ClientWriteUnknownReason::StaleLeader,
            }
        } else {
            ClientWriteStatus::Pending
        };
        self.client_history.writes.insert(
            proposal_id,
            ClientWrite {
                proposal_id,
                node_id,
                payload: super::helpers::proposal_payload(proposal_id).into(),
                status,
            },
        );
    }

    pub(super) fn record_client_read(
        &mut self,
        node_id: NodeId,
        request_id: u64,
        committed_floor: LogIndex,
    ) {
        self.client_history.reads.insert(
            request_id,
            ClientRead {
                node_id,
                request_id,
                committed_floor,
                outcome: ClientReadOutcome::Pending,
            },
        );
    }

    pub(super) fn refresh_client_history(&mut self) {
        for write in self.client_history.writes.values_mut() {
            if matches!(write.status, ClientWriteStatus::Completed { .. }) {
                continue;
            }
            if let Some(applied) = self
                .cluster
                .applied()
                .iter()
                .find(|applied| applied.payload == write.payload)
            {
                write.status = ClientWriteStatus::Completed {
                    node_id: applied.node_id,
                    index: applied.index,
                };
            }
        }

        for read in self.client_history.reads.values_mut() {
            if matches!(read.outcome, ClientReadOutcome::Completed { .. }) {
                continue;
            }
            let Some(grant) =
                self.cluster.read_grants().iter().find(|grant| {
                    grant.node_id == read.node_id && grant.request_id == read.request_id
                })
            else {
                continue;
            };
            let proof = ClientReadProof {
                read_index: grant.read_index,
                local_applied_index: self.cluster.local_applied_index(read.node_id),
            };
            read.outcome = if proof.local_applied_index >= proof.read_index {
                ClientReadOutcome::Completed { proof }
            } else {
                ClientReadOutcome::ProofGranted { proof }
            };
        }
    }

    pub(super) fn reset_commit_floor(&mut self, node_id: NodeId) {
        if let Some(node) = self.cluster.nodes.get(&node_id) {
            self.commit_floor_by_node
                .insert(node_id, node.commit_index());
            self.committed_configuration_floor_by_node
                .insert(node_id, node.committed_configuration_state());
        }
    }

    fn require_applied_payload(
        &mut self,
        node_id: NodeId,
        index: LogIndex,
        payload: SharedPayload,
    ) {
        self.required_applied_payloads
            .insert((node_id, index), payload);
    }

    fn require_committed_configuration(
        &mut self,
        node_id: NodeId,
        configuration: CommittedConfiguration,
    ) {
        self.required_committed_configurations
            .insert((node_id, configuration.index), configuration);
    }

    fn require_commit_index(&mut self, node_id: NodeId, index: LogIndex) {
        self.required_commit_indexes.insert((node_id, index));
    }
}

#[derive(Clone, Debug, Default, Hash)]
pub(super) struct ClientHistory {
    pub(super) writes: BTreeMap<ProposalId, ClientWrite>,
    pub(super) reads: BTreeMap<u64, ClientRead>,
}

#[derive(Clone, Debug, Hash)]
pub(super) struct ClientWrite {
    pub(super) proposal_id: ProposalId,
    pub(super) node_id: NodeId,
    pub(super) payload: SharedPayload,
    pub(super) status: ClientWriteStatus,
}

#[derive(Clone, Debug, Hash)]
pub(super) enum ClientWriteStatus {
    Pending,
    Completed { node_id: NodeId, index: LogIndex },
    Unknown { reason: ClientWriteUnknownReason },
}

#[derive(Clone, Copy, Debug, Hash)]
pub(super) enum ClientWriteUnknownReason {
    StaleLeader,
}

#[derive(Clone, Debug, Hash)]
pub(super) struct ClientRead {
    pub(super) node_id: NodeId,
    pub(super) request_id: u64,
    pub(super) committed_floor: LogIndex,
    pub(super) outcome: ClientReadOutcome,
}

#[derive(Clone, Copy, Debug, Hash)]
pub(super) enum ClientReadOutcome {
    Pending,
    ProofGranted { proof: ClientReadProof },
    Completed { proof: ClientReadProof },
}

#[derive(Clone, Copy, Debug, Hash)]
pub(super) struct ClientReadProof {
    pub(super) read_index: LogIndex,
    pub(super) local_applied_index: LogIndex,
}

/// The snapshot every healthy node must converge on: the descriptor the
/// kernel tracks plus the payload bytes the content invariants compare,
/// which the kernel no longer carries.
#[derive(Clone, Debug, Hash)]
pub(super) struct ExpectedSnapshot {
    pub(super) snapshot: RaftSnapshot,
    pub(super) payload: SharedPayload,
}

#[derive(Clone, Debug, Hash)]
pub(super) struct RestartSnapshotState {
    pub(super) state: ExplorationState,
    pub(super) expected_snapshot: Option<ExpectedSnapshot>,
    pub(super) divergent_payloads: Vec<SharedPayload>,
}

impl RestartSnapshotState {
    pub(super) fn new(state: ExplorationState) -> Self {
        Self {
            state,
            expected_snapshot: None,
            divergent_payloads: Vec::new(),
        }
    }

    pub(super) fn snapshot_transfer() -> Self {
        Self::snapshot_transfer_with_committed_membership(None)
    }

    pub(super) fn joint_snapshot_transfer() -> Self {
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
        cluster.seed_snapshot_payload(NodeId(1), &snapshot, payload.clone());
        cluster
            .restart_node_from_bootstrap(
                NodeId(1),
                bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]),
            )
            .expect("leader bootstrap is valid");
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
        cluster.seed_snapshot_payload(NodeId(3), &snapshot, payload.clone());
        cluster
            .restart_node_from_bootstrap(
                NodeId(3),
                bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]),
            )
            .expect("voter bootstrap is valid");
        elect_node_one_with_node_three(&mut cluster);
        cluster.drop_matching(|envelope| {
            matches!(
                envelope.message,
                Message::RequestVote(_) | Message::RequestVoteResponse(_)
            ) || envelope.from == NodeId(3)
                || envelope.to == NodeId(3)
        });

        Self {
            state: ExplorationState::new(cluster),
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
