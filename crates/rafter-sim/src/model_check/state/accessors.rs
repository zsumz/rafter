//! Read-only projections of one exploration state.
//!
//! Invariant checks and explorers reach recorded history only through these
//! accessors, so the wrapped cluster and its recorder histories can never be
//! mutated outside an observed transition.

use std::collections::{BTreeMap, BTreeSet};

use rafter::{CommittedConfiguration, LogIndex, NodeId, SharedPayload};

use crate::Cluster;

use super::super::observations::ObservationSet;
use super::application_history::{ExecutionHistoryInstrumentationError, ExecutionHistoryViolation};
use super::{
    application, snapshot, ClientHistory, CommitHistory, ElectionHistory, ExplorationState,
    LogicalLogHistory, TransitionInstrumentationError,
};

impl ExplorationState {
    pub(in crate::model_check) fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    pub(in crate::model_check) const fn proposals_issued(&self) -> u64 {
        self.proposals_issued
    }

    pub(in crate::model_check) const fn restarts_issued(&self) -> u64 {
        self.restarts_issued
    }

    pub(in crate::model_check) const fn read_indexes_issued(&self) -> u64 {
        self.read_indexes_issued
    }

    pub(in crate::model_check) const fn membership_changes_issued(&self) -> u64 {
        self.membership_changes_issued
    }

    pub(in crate::model_check) const fn transfers_issued(&self) -> u64 {
        self.transfers_issued
    }

    pub(in crate::model_check) const fn partitions_issued(&self) -> u64 {
        self.partitions_issued
    }

    pub(in crate::model_check) const fn lossy_restarts_issued(&self) -> u64 {
        self.lossy_restarts_issued
    }

    pub(in crate::model_check) const fn commit_floor_by_node(&self) -> &BTreeMap<NodeId, LogIndex> {
        &self.commit_floor_by_node
    }

    pub(in crate::model_check) const fn committed_configuration_floor_by_node(
        &self,
    ) -> &BTreeMap<NodeId, Option<CommittedConfiguration>> {
        &self.committed_configuration_floor_by_node
    }

    pub(in crate::model_check) const fn client_history(&self) -> &ClientHistory {
        &self.client_history
    }

    pub(in crate::model_check) const fn transition_instrumentation_errors(
        &self,
    ) -> &BTreeSet<TransitionInstrumentationError> {
        &self.transition_instrumentation_errors
    }

    pub(in crate::model_check) fn record_transition_instrumentation_error(
        &mut self,
        failure: &super::super::Failure,
    ) {
        self.transition_instrumentation_errors
            .insert(TransitionInstrumentationError {
                kind: failure.kind,
                invariant: failure.invariant,
                message: failure.message.clone(),
            });
    }

    pub(in crate::model_check) const fn execution_history_violations(
        &self,
    ) -> &BTreeSet<ExecutionHistoryViolation> {
        self.application_history.violations()
    }

    pub(in crate::model_check) const fn execution_instrumentation_errors(
        &self,
    ) -> &BTreeSet<ExecutionHistoryInstrumentationError> {
        self.application_history.instrumentation_errors()
    }

    pub(in crate::model_check) const fn required_applied_payloads(
        &self,
    ) -> &BTreeMap<(NodeId, LogIndex), SharedPayload> {
        &self.required_applied_payloads
    }

    pub(in crate::model_check) const fn required_committed_configurations(
        &self,
    ) -> &BTreeMap<(NodeId, LogIndex), CommittedConfiguration> {
        &self.required_committed_configurations
    }

    pub(in crate::model_check) const fn required_commit_indexes(
        &self,
    ) -> &BTreeSet<(NodeId, LogIndex)> {
        &self.required_commit_indexes
    }

    pub(in crate::model_check) const fn election_history(&self) -> &ElectionHistory {
        &self.election_history
    }

    pub(in crate::model_check) const fn logical_log_history(&self) -> &LogicalLogHistory {
        &self.logical_log_history
    }

    pub(in crate::model_check) const fn commit_history(&self) -> &CommitHistory {
        &self.commit_history
    }

    pub(in crate::model_check) const fn snapshot_history(&self) -> &snapshot::SnapshotHistory {
        &self.snapshot_history
    }

    pub(in crate::model_check) const fn observation_set(&self) -> ObservationSet {
        self.observations
    }

    #[cfg(test)]
    pub(in crate::model_check) fn commit_floor_by_node_mut(
        &mut self,
    ) -> &mut BTreeMap<NodeId, LogIndex> {
        &mut self.commit_floor_by_node
    }

    #[cfg(test)]
    pub(in crate::model_check) fn committed_configuration_floor_by_node_mut(
        &mut self,
    ) -> &mut BTreeMap<NodeId, Option<CommittedConfiguration>> {
        &mut self.committed_configuration_floor_by_node
    }

    #[cfg(test)]
    pub(in crate::model_check) fn client_history_mut(&mut self) -> &mut ClientHistory {
        &mut self.client_history
    }

    #[cfg(test)]
    pub(in crate::model_check) fn election_history_mut(&mut self) -> &mut ElectionHistory {
        &mut self.election_history
    }

    #[cfg(test)]
    pub(in crate::model_check) const fn election_transition_contexts_observed(&self) -> u64 {
        self.election_history.transition_contexts_observed
    }

    #[cfg(test)]
    pub(in crate::model_check) fn logical_log_history_mut(&mut self) -> &mut LogicalLogHistory {
        &mut self.logical_log_history
    }

    pub(in crate::model_check) fn scheduler_index(&mut self, len: usize) -> usize {
        application::scheduler_index(self, len)
    }

    #[cfg(test)]
    pub(in crate::model_check) fn inject_snapshot_payload(
        &mut self,
        node_id: NodeId,
        snapshot: &rafter::RaftSnapshot,
        payload: Vec<u8>,
    ) {
        self.cluster
            .seed_snapshot_payload(node_id, snapshot, payload);
    }

    #[cfg(test)]
    pub(in crate::model_check) fn observe_snapshot_cluster_for_detector(
        &mut self,
        cluster: &Cluster,
    ) {
        let observations = self.snapshot_history.observe_cluster(cluster);
        self.observations.union_with(observations);
    }

    #[cfg(test)]
    pub(in crate::model_check) fn inject_applied_record(&mut self, applied: crate::Applied) {
        self.cluster.inject_applied_record(applied);
    }

    #[cfg(test)]
    pub(in crate::model_check) fn clear_execution_cursors(&mut self) {
        self.cluster.clear_execution_cursors();
    }

    #[cfg(test)]
    pub(in crate::model_check) fn clear_initial_reference_states(&mut self) {
        self.cluster.clear_initial_reference_states();
    }

    #[cfg(test)]
    pub(in crate::model_check) fn clear_application_epochs(&mut self) {
        self.cluster.clear_application_epochs();
    }

    #[cfg(test)]
    pub(in crate::model_check) fn remove_execution_cursor(&mut self, node_id: NodeId) {
        self.cluster.remove_execution_cursor(node_id);
    }

    #[cfg(test)]
    pub(in crate::model_check) fn inject_read_grant(&mut self, grant: crate::ReadGranted) {
        self.cluster.inject_read_grant(grant);
    }

    #[cfg(test)]
    pub(in crate::model_check) fn inject_read_terminal_output(
        &mut self,
        output: crate::ReadTerminalOutput,
    ) {
        self.cluster.inject_read_terminal_output(output);
    }

    #[cfg(test)]
    pub(in crate::model_check) fn inject_blocked_pair(&mut self, from: NodeId, to: NodeId) {
        self.cluster.inject_blocked_pair(from, to);
    }
}
