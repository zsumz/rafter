//! Snapshot observation for cluster states and delivered transitions.
//!
//! Boundary advances, install semantics, geometry, payload binding, and
//! transfer identity are all witnessed from the same before/after pair, so an
//! install can never be recorded without the evidence that justified it.

use rafter::{Message, NodeId, RaftSnapshot};

use crate::{Cluster, Envelope};

use super::super::super::observations::{Observation, ObservationSet};
use super::identity::{snapshot_transfer_identity_check, SnapshotTransferIdentityCheck};
use super::{
    accepted_chunk_effect, chunk_identity_issue, chunk_offset_issue, descriptor_from_pending,
    install_completeness_issue, pending_snapshot_lifecycle_issue,
    snapshot_reference_coverage_issue, AcceptedSnapshotChunkEffect, AcceptedSnapshotChunkWitness,
    SnapshotHistory, SnapshotHistoryViolation, SnapshotReferenceBindingCheck,
    SnapshotTransferDescriptor,
};

impl SnapshotHistory {
    pub(in crate::model_check::state) fn from_cluster(cluster: &Cluster) -> Self {
        let mut history = Self {
            boundary_floor_by_node: cluster
                .nodes
                .iter()
                .map(|(node_id, node)| (*node_id, node.snapshot_index()))
                .collect(),
            ..Self::default()
        };
        history.record_logical_prefix_references(cluster);
        history.record_snapshot_references(cluster);
        let _ = history.observe_cluster(cluster);
        history
    }

    pub(in crate::model_check::state) fn observe_cluster(
        &mut self,
        cluster: &Cluster,
    ) -> ObservationSet {
        let mut observations = ObservationSet::default();
        self.record_logical_prefix_references(cluster);
        self.record_snapshot_references(cluster);
        for (node_id, node) in &cluster.nodes {
            if node.snapshot().is_some() {
                match self.snapshot_reference_binding_check(cluster, *node_id) {
                    SnapshotReferenceBindingCheck::Verified => {}
                    SnapshotReferenceBindingCheck::CoverageUnavailable => {
                        self.payload_binding_coverage_gaps
                            .insert(snapshot_reference_coverage_issue(cluster, *node_id));
                    }
                    SnapshotReferenceBindingCheck::Violation(issue) => {
                        self.payload_binding_violations.insert(issue);
                    }
                }
            }
            let current = node.snapshot_index();
            let floor = self.boundary_floor_by_node.entry(*node_id).or_default();
            if current < *floor {
                self.boundary_violations.insert(SnapshotHistoryViolation {
                    node_id: *node_id,
                    previous_boundary: *floor,
                    current_boundary: current,
                });
            } else {
                *floor = current;
            }

            let Some(pending) = node.pending_snapshot_transfer() else {
                continue;
            };
            match pending_snapshot_lifecycle_issue(*node_id, current, &pending) {
                Some(issue) => {
                    self.pending_lifecycle_violations.insert(issue);
                }
                None if pending.received_bytes() > 0 => {
                    observations.mark(Observation::PendingSnapshotLifecyclesChecked);
                }
                None => {}
            }
        }
        self.record_resurrected_entries(cluster);
        observations
    }

    pub(in crate::model_check::state) fn record_transition(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        delivered: Option<&Envelope>,
    ) -> ObservationSet {
        let mut observations = ObservationSet::default();
        if let Some(envelope) = delivered {
            self.record_chunk_transition(before, after, envelope, &mut observations);
        }

        for (node_id, node) in &after.nodes {
            let Some(previous) = before.nodes.get(node_id) else {
                continue;
            };
            let boundary_advanced = node.snapshot_index() > previous.snapshot_index();
            let snapshot_identity_changed = snapshot_identity_changed(before, after, *node_id);
            if !snapshot_identity_changed || node.snapshot_index() < previous.snapshot_index() {
                continue;
            }
            self.record_installed_snapshot_reference(after, *node_id, delivered);
            self.record_snapshot_semantics(before, after, *node_id);
            if boundary_advanced {
                observations.mark(Observation::SnapshotBoundaryAdvances);
            }
            self.record_geometry(after, *node_id, &mut observations);

            match self.snapshot_reference_binding_check(after, *node_id) {
                SnapshotReferenceBindingCheck::Verified => {
                    observations.mark(Observation::SnapshotPayloadBindingsChecked);
                }
                SnapshotReferenceBindingCheck::CoverageUnavailable => {
                    self.payload_binding_coverage_gaps
                        .insert(snapshot_reference_coverage_issue(after, *node_id));
                }
                SnapshotReferenceBindingCheck::Violation(issue) => {
                    self.payload_binding_violations.insert(issue);
                }
            }
            let delivered_install = delivered.is_some_and(|envelope| {
                envelope.to == *node_id
                    && matches!(
                        envelope.message,
                        Message::InstallSnapshot(_) | Message::InstallSnapshotChunk(_)
                    )
            });
            if !boundary_advanced && !delivered_install {
                continue;
            }
            match snapshot_transfer_identity_check(before, after, *node_id, delivered) {
                SnapshotTransferIdentityCheck::Verified => {
                    observations.mark(Observation::SnapshotTransferIdentitiesChecked);
                }
                SnapshotTransferIdentityCheck::CoverageUnavailable(issue) => {
                    self.transfer_identity_instrumentation_errors.insert(issue);
                }
                SnapshotTransferIdentityCheck::Violation(issue) => {
                    self.transfer_identity_violations.insert(issue);
                }
            }
        }
        self.record_resurrected_entries(after);
        observations
    }

    fn record_chunk_transition(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        envelope: &Envelope,
        observations: &mut ObservationSet,
    ) {
        let Message::InstallSnapshotChunk(request) = &envelope.message else {
            return;
        };
        let Some(effect) =
            accepted_chunk_effect(before, after, envelope.to, envelope.from, request)
        else {
            return;
        };

        self.remember_prior_descriptor(before, envelope.to);
        let witness = AcceptedSnapshotChunkWitness {
            node_id: envelope.to,
            leader_id: envelope.from,
            transfer_id: request.transfer_id,
            offset: request.offset,
            chunk_len: request.chunk.len() as u64,
            total_payload_len: request.total_payload_len,
            done: request.done,
            effect,
        };
        self.accepted_chunk_witnesses.insert(witness);

        match chunk_identity_issue(self, after, envelope, request, effect) {
            Some(issue) => {
                self.chunk_identity_violations.insert(issue);
            }
            None => observations.mark(Observation::SnapshotChunkIdentitiesChecked),
        }
        match chunk_offset_issue(after, envelope.to, request, effect) {
            Some(issue) => {
                self.chunk_offset_violations.insert(issue);
            }
            None => observations.mark(Observation::SnapshotChunkOffsetsChecked),
        }
        if matches!(effect, AcceptedSnapshotChunkEffect::Installed { .. }) {
            match install_completeness_issue(after, envelope.to, request, effect) {
                Some(issue) => {
                    self.install_completeness_violations.insert(issue);
                }
                None => observations.mark(Observation::SnapshotInstallCompletenessChecked),
            }
        }
    }

    fn remember_prior_descriptor(&mut self, before: &Cluster, node_id: NodeId) {
        let descriptor = before
            .snapshot_staging
            .get(&node_id)
            .map(|staged| SnapshotTransferDescriptor {
                leader_id: staged.leader_id,
                snapshot: RaftSnapshot::new(
                    staged.metadata.clone(),
                    staged.total_payload_len,
                    staged.application_payload_crc32,
                ),
            })
            .or_else(|| {
                before
                    .node(node_id)
                    .pending_snapshot_transfer()
                    .map(|pending| descriptor_from_pending(&pending))
            });
        if let Some(descriptor) = descriptor {
            self.chunk_descriptors
                .entry((node_id, descriptor.snapshot.transfer_id()))
                .or_insert(descriptor);
        }
    }
}

pub(super) fn snapshot_identity_changed(
    before: &Cluster,
    after: &Cluster,
    node_id: NodeId,
) -> bool {
    let before_snapshot = before.node(node_id).snapshot();
    let after_snapshot = after.node(node_id).snapshot();
    before_snapshot != after_snapshot
        || after_snapshot.is_some_and(|snapshot| {
            before.snapshot_payload(node_id, snapshot) != after.snapshot_payload(node_id, snapshot)
        })
}
