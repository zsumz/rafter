//! Logical-prefix reference witnesses for installed snapshots.
//!
//! A snapshot must carry the exact application value, membership, and
//! configuration identity that replaying the committed prefix to its boundary
//! produces; an unwitnessed boundary is reported as a coverage gap, not a pass.

use rafter::{CommittedConfiguration, LogEntryKind, LogIndex, Message, NodeId};

use crate::{Cluster, Envelope, ReferenceState};

use super::{
    snapshot_payload_binding_issue, SnapshotHistory, SnapshotReferenceBindingCheck,
    SnapshotReferenceWitness,
};

impl SnapshotHistory {
    pub(super) fn record_snapshot_references(&mut self, cluster: &Cluster) {
        for node_id in cluster.nodes.keys().copied() {
            let Some(snapshot) = cluster.node(node_id).snapshot() else {
                continue;
            };
            let key = (node_id, snapshot.transfer_id());
            if self.reference_witnesses_by_snapshot.contains_key(&key) {
                continue;
            }
            if let Some(witness) =
                self.reference_witness(cluster, node_id, snapshot.metadata.last_included_index)
            {
                self.reference_witnesses_by_snapshot.insert(key, witness);
            }
        }
    }

    pub(super) fn record_logical_prefix_references(&mut self, cluster: &Cluster) {
        for node_id in cluster.nodes.keys().copied() {
            let bootstrap = cluster.bootstrap_state(node_id);
            let state = if let Some(snapshot) = bootstrap.snapshot.as_ref() {
                self.reference_witnesses_by_snapshot
                    .get(&(node_id, snapshot.transfer_id()))
                    .map(|witness| witness.state.clone())
            } else {
                cluster.initial_reference_states.get(&node_id).cloned()
            };
            let Some(mut state) = state else {
                continue;
            };
            for entry in bootstrap
                .log
                .into_iter()
                .filter(|entry| entry.index <= bootstrap.commit_index)
            {
                state = apply_reference_transition(&state, entry.index, &entry.kind);
                self.reference_witnesses_by_log_prefix.insert(
                    (node_id, entry.index),
                    SnapshotReferenceWitness {
                        boundary_term: entry.term,
                        state: state.clone(),
                    },
                );
            }
        }
    }

    fn reference_witness(
        &self,
        cluster: &Cluster,
        node_id: NodeId,
        boundary: LogIndex,
    ) -> Option<SnapshotReferenceWitness> {
        reference_witness_from_execution(cluster, node_id, boundary).or_else(|| {
            self.reference_witnesses_by_log_prefix
                .get(&(node_id, boundary))
                .cloned()
        })
    }

    pub(super) fn record_installed_snapshot_reference(
        &mut self,
        cluster: &Cluster,
        node_id: NodeId,
        delivered: Option<&Envelope>,
    ) {
        let Some(snapshot) = cluster.node(node_id).snapshot() else {
            return;
        };
        let key = (node_id, snapshot.transfer_id());
        if self.reference_witnesses_by_snapshot.contains_key(&key) {
            return;
        }
        let source = delivered.and_then(|envelope| {
            matches!(
                envelope.message,
                Message::InstallSnapshot(_) | Message::InstallSnapshotChunk(_)
            )
            .then_some(envelope.from)
        });
        let witness = source
            .and_then(|source_id| {
                self.reference_witnesses_by_snapshot
                    .get(&(source_id, snapshot.transfer_id()))
                    .cloned()
                    .or_else(|| {
                        self.reference_witness(
                            cluster,
                            source_id,
                            snapshot.metadata.last_included_index,
                        )
                    })
            })
            .or_else(|| {
                self.reference_witness(cluster, node_id, snapshot.metadata.last_included_index)
            });
        if let Some(witness) = witness {
            self.reference_witnesses_by_snapshot.insert(key, witness);
        }
    }

    pub(super) fn snapshot_reference_binding_check(
        &self,
        cluster: &Cluster,
        node_id: NodeId,
    ) -> SnapshotReferenceBindingCheck {
        let Some(snapshot) = cluster.node(node_id).snapshot() else {
            return SnapshotReferenceBindingCheck::CoverageUnavailable;
        };
        if let Some(issue) = snapshot_payload_binding_issue(cluster, node_id) {
            return SnapshotReferenceBindingCheck::Violation(issue);
        }
        let Some(expected) = self
            .reference_witnesses_by_snapshot
            .get(&(node_id, snapshot.transfer_id()))
        else {
            return SnapshotReferenceBindingCheck::CoverageUnavailable;
        };
        if snapshot.metadata.last_included_term != expected.boundary_term {
            return SnapshotReferenceBindingCheck::Violation(format!(
                "{node_id} snapshot boundary term {} differs from witnessed logical-prefix term {} at index {}",
                snapshot.metadata.last_included_term,
                expected.boundary_term,
                snapshot.metadata.last_included_index,
            ));
        }
        let Some(payload) = cluster.snapshot_payload(node_id, snapshot) else {
            return SnapshotReferenceBindingCheck::Violation(format!(
                "{node_id} installed snapshot descriptor has no durable payload bytes"
            ));
        };
        if payload != expected.state.application_value.as_ref() {
            return SnapshotReferenceBindingCheck::Violation(format!(
                "{node_id} snapshot application payload differs from witnessed reference state at index {}",
                snapshot.metadata.last_included_index,
            ));
        }
        let actual_membership = snapshot
            .metadata
            .committed_membership()
            .cloned()
            .or_else(|| {
                cluster
                    .initial_reference_states
                    .get(&node_id)
                    .map(|state| state.committed_membership.clone())
            });
        if actual_membership.as_ref() != Some(&expected.state.committed_membership) {
            return SnapshotReferenceBindingCheck::Violation(format!(
                "{node_id} snapshot committed membership differs from witnessed reference state at index {}",
                snapshot.metadata.last_included_index,
            ));
        }
        if snapshot.metadata.committed_configuration_state()
            != expected.state.committed_configuration
        {
            return SnapshotReferenceBindingCheck::Violation(format!(
                "{node_id} snapshot committed configuration identity differs from witnessed reference state at index {}",
                snapshot.metadata.last_included_index,
            ));
        }
        SnapshotReferenceBindingCheck::Verified
    }
}

pub(super) fn snapshot_reference_coverage_issue(cluster: &Cluster, node_id: NodeId) -> String {
    let Some(snapshot) = cluster.node(node_id).snapshot() else {
        return format!(
            "{node_id} advanced its snapshot boundary without an installed descriptor to bind to a reference witness"
        );
    };
    format!(
        "{node_id} snapshot transfer {} at boundary {} has no logical-prefix reference witness",
        snapshot.transfer_id(),
        snapshot.metadata.last_included_index
    )
}

fn reference_witness_from_execution(
    cluster: &Cluster,
    node_id: NodeId,
    boundary: LogIndex,
) -> Option<SnapshotReferenceWitness> {
    cluster
        .execution_history()
        .iter()
        .rev()
        .find(|witness| {
            witness.node_id == node_id
                && witness.entry.index == boundary
                && witness.commit_index_at_emit >= boundary
        })
        .map(|witness| SnapshotReferenceWitness {
            boundary_term: witness.entry.term,
            state: witness.resulting_state.clone(),
        })
}

fn apply_reference_transition(
    prior: &ReferenceState,
    index: LogIndex,
    kind: &LogEntryKind,
) -> ReferenceState {
    let mut result = prior.clone();
    match kind {
        LogEntryKind::Application(payload) => {
            result.application_value.clone_from(payload);
        }
        LogEntryKind::Configuration(configuration) => {
            result.committed_membership = configuration.membership_config();
            result.committed_configuration = Some(CommittedConfiguration {
                index,
                config_id: configuration.config_id(),
            });
        }
        LogEntryKind::Noop => {}
    }
    result
}
