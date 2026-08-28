//! Construction and transition observation for exploration states.
//!
//! Building a state seeds every recorder history from the initial cluster, and
//! each `record_*`/`refresh_*` entry folds one transition's observations into
//! the state so coverage is measured exactly where it is produced.

use std::collections::{BTreeMap, BTreeSet};

use rafter::{CommittedConfiguration, LogIndex, NodeId, SharedPayload};

use crate::{Cluster, Envelope};

use super::super::observations::ObservationSet;
use super::application::InstrumentedCluster;
use super::application_history::ApplicationHistory;
use super::client::initial_register_value;
use super::{
    snapshot, ClientHistory, CommitHistory, ElectionHistory, ExplorationState, LogicalLogHistory,
    PurposeWitnessHistory,
};

impl ExplorationState {
    pub(in crate::model_check) fn new(cluster: Cluster) -> Self {
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
            required_applied_payloads: BTreeMap::new(),
            required_committed_configurations: BTreeMap::new(),
            required_commit_indexes: BTreeSet::new(),
            election_history: ElectionHistory::default(),
            logical_log_history: LogicalLogHistory::default(),
            commit_history: CommitHistory::default(),
            snapshot_history,
            purpose_witness_history: PurposeWitnessHistory::default(),
            observations: ObservationSet::default(),
            transition_instrumentation_errors: BTreeSet::new(),
        };
        state.election_history.record_seeded_leaders(&state.cluster);
        state.observe_election_authority();
        state.refresh_log_history();
        state.refresh_seeded_commit_history();
        state.refresh_application_history();
        state.observe_state_coverage();
        state
    }

    pub(in crate::model_check) fn refresh_commit_floors(&mut self) {
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
            self.mark_observation(super::super::observations::Observation::CommitFloorAdvances);
        }
        if commit_bound_checked {
            self.mark_observation(
                super::super::observations::Observation::CommitIndexWithinLocalLogBoundsChecks,
            );
        }
        if configuration_advanced {
            self.mark_observation(
                super::super::observations::Observation::CommittedConfigurationAdvances,
            );
        }
        if configuration_identity_checked {
            self.mark_observation(
                super::super::observations::Observation::SameIndexCommittedConfigurationIdentityChecks,
            );
        }
    }

    pub(in crate::model_check) fn record_log_transition(
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

    pub(in crate::model_check) fn refresh_log_history(&mut self) {
        let observations = self.logical_log_history.observe_cluster(&self.cluster);
        self.observations.union_with(observations);
    }

    pub(in crate::model_check) fn refresh_application_history(&mut self) {
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

    pub(in crate::model_check) fn record_purpose_transition(
        &mut self,
        before: &Cluster,
        operation: &super::super::scheduling::Operation,
        emitted: &[Envelope],
        local_proposals: &[crate::records::LocalProposalEvent],
        read_registration: Option<&crate::ReadRegistered>,
    ) {
        let observations = self.purpose_witness_history.record_transition(
            before,
            &self.cluster,
            operation,
            emitted,
            local_proposals,
            read_registration,
        );
        for observation in observations {
            self.mark_observation(observation);
        }
    }

    pub(in crate::model_check) fn record_purpose_restart(
        &mut self,
        before: &Cluster,
        node_id: NodeId,
    ) {
        let observations =
            self.purpose_witness_history
                .record_restart(before, &self.cluster, node_id);
        for observation in observations {
            self.mark_observation(observation);
        }
    }

    pub(super) fn require_applied_payload(
        &mut self,
        node_id: NodeId,
        index: LogIndex,
        payload: SharedPayload,
    ) {
        self.required_applied_payloads
            .insert((node_id, index), payload);
    }

    pub(super) fn require_committed_configuration(
        &mut self,
        node_id: NodeId,
        configuration: CommittedConfiguration,
    ) {
        self.required_committed_configurations
            .insert((node_id, configuration.index), configuration);
    }

    pub(super) fn require_commit_index(&mut self, node_id: NodeId, index: LogIndex) {
        self.required_commit_indexes.insert((node_id, index));
    }
}
