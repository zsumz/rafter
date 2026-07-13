use std::collections::{BTreeMap, BTreeSet};

use rafter::{CommittedConfiguration, LogIndex, NodeId, SharedPayload};

use crate::{Cluster, Envelope};

#[path = "application.rs"]
mod application;
mod application_history;
mod client;
mod commit;
mod coverage;
mod election;
mod logical_log;
mod restart_snapshot;
mod seeds;
mod snapshot;

use self::application::InstrumentedCluster;
pub(super) use self::application::{
    apply_pending_application_replay_seed, apply_snapshot_bootstrap_seeds, apply_soak_action,
    apply_to_restart_snapshot_state, apply_to_state, restart_node, PendingApplicationReplaySeed,
    SnapshotBootstrapSeed,
};
use self::application_history::ApplicationHistory;
use super::observations::ObservationSet;
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
#[cfg(test)]
pub(super) use logical_log::{LogPrefixWitness, LogicalLogView};
pub(super) use restart_snapshot::{ExpectedSnapshot, RestartSnapshotState};
pub(super) use snapshot::snapshot_payload_binding_issue;

#[derive(Clone, Debug, Hash)]
pub(super) struct ExplorationState {
    cluster: InstrumentedCluster,
    proposals_issued: u64,
    restarts_issued: u64,
    read_indexes_issued: u64,
    membership_changes_issued: u64,
    transfers_issued: u64,
    partitions_issued: u64,
    lossy_restarts_issued: u64,
    commit_floor_by_node: BTreeMap<NodeId, LogIndex>,
    committed_configuration_floor_by_node: BTreeMap<NodeId, Option<CommittedConfiguration>>,
    application_history: ApplicationHistory,
    client_history: ClientHistory,
    forbidden_applied_payloads: BTreeSet<SharedPayload>,
    required_applied_payloads: BTreeMap<(NodeId, LogIndex), SharedPayload>,
    required_committed_configurations: BTreeMap<(NodeId, LogIndex), CommittedConfiguration>,
    required_commit_indexes: BTreeSet<(NodeId, LogIndex)>,
    election_history: ElectionHistory,
    logical_log_history: LogicalLogHistory,
    commit_history: CommitHistory,
    snapshot_history: snapshot::SnapshotHistory,
    observations: ObservationSet,
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
        let snapshot_history = snapshot::SnapshotHistory::from_cluster(&cluster);
        let application_history = ApplicationHistory::from_cluster(&cluster);
        let mut state = Self {
            cluster: InstrumentedCluster::new(cluster),
            proposals_issued: 0,
            restarts_issued: 0,
            read_indexes_issued: 0,
            membership_changes_issued: 0,
            transfers_issued: 0,
            partitions_issued: 0,
            lossy_restarts_issued: 0,
            commit_floor_by_node,
            committed_configuration_floor_by_node,
            application_history,
            client_history: ClientHistory::with_initial_value(initial_value),
            forbidden_applied_payloads: BTreeSet::new(),
            required_applied_payloads: BTreeMap::new(),
            required_committed_configurations: BTreeMap::new(),
            required_commit_indexes: BTreeSet::new(),
            election_history: ElectionHistory::default(),
            logical_log_history: LogicalLogHistory::default(),
            commit_history: CommitHistory::default(),
            snapshot_history,
            observations: ObservationSet::default(),
        };
        state.election_history.record_seeded_leaders(&state.cluster);
        state.observe_election_authority();
        state.refresh_log_history();
        state.refresh_seeded_commit_history();
        state.observe_state_coverage();
        state
    }

    pub(super) fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    pub(super) const fn proposals_issued(&self) -> u64 {
        self.proposals_issued
    }

    pub(super) const fn restarts_issued(&self) -> u64 {
        self.restarts_issued
    }

    pub(super) const fn read_indexes_issued(&self) -> u64 {
        self.read_indexes_issued
    }

    pub(super) const fn membership_changes_issued(&self) -> u64 {
        self.membership_changes_issued
    }

    pub(super) const fn transfers_issued(&self) -> u64 {
        self.transfers_issued
    }

    pub(super) const fn partitions_issued(&self) -> u64 {
        self.partitions_issued
    }

    pub(super) const fn lossy_restarts_issued(&self) -> u64 {
        self.lossy_restarts_issued
    }

    pub(super) const fn commit_floor_by_node(&self) -> &BTreeMap<NodeId, LogIndex> {
        &self.commit_floor_by_node
    }

    pub(super) const fn committed_configuration_floor_by_node(
        &self,
    ) -> &BTreeMap<NodeId, Option<CommittedConfiguration>> {
        &self.committed_configuration_floor_by_node
    }

    pub(super) const fn client_history(&self) -> &ClientHistory {
        &self.client_history
    }

    pub(super) fn application_history(&self) -> &[crate::ExecutionWitness] {
        self.application_history.witnesses()
    }

    pub(super) const fn execution_instrumentation_errors(
        &self,
    ) -> &BTreeSet<crate::network::ExecutionInstrumentationError> {
        self.application_history.instrumentation_errors()
    }

    pub(super) const fn forbidden_applied_payloads(&self) -> &BTreeSet<SharedPayload> {
        &self.forbidden_applied_payloads
    }

    pub(super) const fn required_applied_payloads(
        &self,
    ) -> &BTreeMap<(NodeId, LogIndex), SharedPayload> {
        &self.required_applied_payloads
    }

    pub(super) const fn required_committed_configurations(
        &self,
    ) -> &BTreeMap<(NodeId, LogIndex), CommittedConfiguration> {
        &self.required_committed_configurations
    }

    pub(super) const fn required_commit_indexes(&self) -> &BTreeSet<(NodeId, LogIndex)> {
        &self.required_commit_indexes
    }

    pub(super) const fn election_history(&self) -> &ElectionHistory {
        &self.election_history
    }

    pub(super) const fn logical_log_history(&self) -> &LogicalLogHistory {
        &self.logical_log_history
    }

    pub(super) const fn commit_history(&self) -> &CommitHistory {
        &self.commit_history
    }

    pub(super) const fn snapshot_history(&self) -> &snapshot::SnapshotHistory {
        &self.snapshot_history
    }

    pub(super) const fn observation_set(&self) -> ObservationSet {
        self.observations
    }

    #[cfg(test)]
    pub(super) fn commit_floor_by_node_mut(&mut self) -> &mut BTreeMap<NodeId, LogIndex> {
        &mut self.commit_floor_by_node
    }

    #[cfg(test)]
    pub(super) fn committed_configuration_floor_by_node_mut(
        &mut self,
    ) -> &mut BTreeMap<NodeId, Option<CommittedConfiguration>> {
        &mut self.committed_configuration_floor_by_node
    }

    #[cfg(test)]
    pub(super) fn client_history_mut(&mut self) -> &mut ClientHistory {
        &mut self.client_history
    }

    #[cfg(test)]
    pub(super) fn election_history_mut(&mut self) -> &mut ElectionHistory {
        &mut self.election_history
    }

    #[cfg(test)]
    pub(super) fn logical_log_history_mut(&mut self) -> &mut LogicalLogHistory {
        &mut self.logical_log_history
    }

    pub(super) fn scheduler_index(&mut self, len: usize) -> usize {
        application::scheduler_index(self, len)
    }

    pub(super) fn random_ready_position(&mut self) -> Option<usize> {
        application::random_ready_position(self)
    }

    #[cfg(test)]
    pub(super) fn inject_bootstrap_state(
        &mut self,
        node_id: NodeId,
        bootstrap: rafter::BootstrapState,
    ) -> Result<(), rafter::BootstrapValidationError> {
        self.cluster.restart_node_from_bootstrap(node_id, bootstrap)
    }

    #[cfg(test)]
    pub(super) fn inject_snapshot_payload(
        &mut self,
        node_id: NodeId,
        snapshot: &rafter::RaftSnapshot,
        payload: Vec<u8>,
    ) {
        self.cluster
            .seed_snapshot_payload(node_id, snapshot, payload);
    }

    #[cfg(test)]
    pub(super) fn inject_message(&mut self, from: NodeId, to: NodeId, message: rafter::Message) {
        self.cluster.queue_message(from, to, message);
    }

    #[cfg(test)]
    pub(super) fn drop_all_messages(&mut self) {
        self.cluster.drop_matching(|_| true);
    }

    #[cfg(test)]
    pub(super) fn inject_applied_record(&mut self, applied: crate::Applied) {
        self.cluster.inject_applied_record(applied);
    }

    #[cfg(test)]
    pub(super) fn inject_execution_witness(&mut self, witness: crate::ExecutionWitness) {
        self.cluster.inject_execution_witness(witness);
        self.refresh_application_history();
    }

    #[cfg(test)]
    pub(super) fn remove_execution_cursor(&mut self, node_id: NodeId) {
        self.cluster.remove_execution_cursor(node_id);
    }

    #[cfg(test)]
    pub(super) fn inject_read_grant(&mut self, grant: crate::ReadGranted) {
        self.cluster.inject_read_grant(grant);
    }

    #[cfg(test)]
    pub(super) fn inject_blocked_pair(&mut self, from: NodeId, to: NodeId) {
        self.cluster.inject_blocked_pair(from, to);
    }

    pub(super) fn refresh_commit_floors(&mut self) {
        let mut commit_advanced = false;
        let mut commit_bound_checked = false;
        let mut configuration_advanced = false;
        let mut configuration_identity_checked = false;
        for (node_id, node) in &self.cluster.nodes {
            let floor = self.commit_floor_by_node.entry(*node_id).or_default();
            commit_advanced |= node.commit_index() > *floor;
            commit_bound_checked |= node.commit_index() > LogIndex::ZERO
                && node.commit_index() <= node.last_log_index();
            *floor = (*floor).max(node.commit_index());
            let config_floor = self
                .committed_configuration_floor_by_node
                .entry(*node_id)
                .or_insert(None);
            if let Some(actual) = node.committed_configuration_state() {
                match config_floor {
                    None => {
                        configuration_advanced = true;
                        *config_floor = Some(actual);
                    }
                    Some(floor) if actual.index > floor.index => {
                        configuration_advanced = true;
                        *config_floor = Some(actual);
                    }
                    Some(floor)
                        if actual.index == floor.index && actual.config_id == floor.config_id =>
                    {
                        configuration_identity_checked = true;
                    }
                    Some(_) => {}
                }
            }
        }
        if commit_advanced {
            self.mark_observation(super::observations::Observation::CommitFloorAdvances);
        }
        if commit_bound_checked {
            self.mark_observation(
                super::observations::Observation::CommitIndexWithinLocalLogBoundsChecks,
            );
        }
        if configuration_advanced {
            self.mark_observation(super::observations::Observation::CommittedConfigurationAdvances);
        }
        if configuration_identity_checked {
            self.mark_observation(
                super::observations::Observation::SameIndexCommittedConfigurationIdentityChecks,
            );
        }
    }

    pub(super) fn record_log_transition(
        &mut self,
        before: &Cluster,
        delivered: Option<&Envelope>,
        emitted: &[Envelope],
    ) {
        self.logical_log_history
            .record_snapshot_installation(before, &self.cluster, delivered);
        let observations = self.logical_log_history.record_append_entries_delivery(
            before,
            &self.cluster,
            delivered,
            emitted,
        );
        self.observations.union_with(observations);
    }

    pub(super) fn refresh_log_history(&mut self) {
        let observations = self.logical_log_history.observe_cluster(&self.cluster);
        self.observations.union_with(observations);
    }

    pub(super) fn refresh_application_history(&mut self) {
        let observations = self.application_history.observe_cluster(&self.cluster);
        self.observations.union_with(observations);
    }

    pub(in crate::model_check) fn refresh_snapshot_history(&mut self) {
        let observations = self.snapshot_history.observe_cluster(&self.cluster);
        self.observations.union_with(observations);
    }

    pub(in crate::model_check) fn record_snapshot_transition(
        &mut self,
        before: &Cluster,
        delivered: Option<&Envelope>,
    ) {
        let observations =
            self.snapshot_history
                .record_transition(before, &self.cluster, delivered);
        self.observations.union_with(observations);
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
