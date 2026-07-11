use std::collections::{BTreeMap, BTreeSet};

use rafter::{CommittedConfiguration, LogIndex, NodeId, SharedPayload};

use crate::{Cluster, Envelope};

mod client;
mod commit;
mod election;
mod logical_log;
mod restart_snapshot;
mod seeds;

use client::initial_register_value;
pub(super) use client::{ClientHistory, ClientReadOutcome, ClientWriteStatus};
#[cfg(test)]
pub(super) use client::{ClientRead, ClientReadProof, ClientWrite, ClientWriteUnknownReason};
pub(super) use commit::CommitHistory;
#[cfg(test)]
pub(super) use commit::CommitTransitionContext;
#[cfg(test)]
pub(super) use election::ElectionCertificate;
pub(super) use election::ElectionHistory;
pub(super) use election::{AuthorityTransitionViolationKind, PreVoteViolationKind};
pub(super) use logical_log::LogicalLogHistory;
pub(super) use restart_snapshot::{ExpectedSnapshot, RestartSnapshotState};

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
    pub(super) election_history: ElectionHistory,
    pub(super) logical_log_history: LogicalLogHistory,
    pub(super) commit_history: CommitHistory,
}

impl ExplorationState {
    pub(super) fn new(cluster: Cluster) -> Self {
        let initial_value = initial_register_value(&cluster);
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
        let mut state = Self {
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
            client_history: ClientHistory::with_initial_value(initial_value),
            forbidden_applied_payloads: BTreeSet::new(),
            required_applied_payloads: BTreeMap::new(),
            required_committed_configurations: BTreeMap::new(),
            required_commit_indexes: BTreeSet::new(),
            election_history: ElectionHistory::default(),
            logical_log_history: LogicalLogHistory::default(),
            commit_history: CommitHistory::default(),
        };
        state.observe_election_authority();
        state.refresh_log_history();
        state.refresh_committed_prefixes();
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

    pub(super) fn record_log_transition(
        &mut self,
        before: &Cluster,
        delivered: Option<&Envelope>,
        emitted: &[Envelope],
    ) {
        self.logical_log_history.record_append_entries_delivery(
            before,
            &self.cluster,
            delivered,
            emitted,
        );
    }

    pub(super) fn refresh_log_history(&mut self) {
        self.logical_log_history.observe_cluster(&self.cluster);
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
