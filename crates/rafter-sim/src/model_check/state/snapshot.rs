use std::collections::{BTreeMap, BTreeSet};

use rafter::{
    CommittedConfiguration, InstallSnapshotChunk, LogEntryKind, LogIndex, Message, NodeId,
    PendingSnapshotTransfer, RaftSnapshot, SnapshotTransferId, Term,
};

use crate::{Cluster, Envelope, ReferenceState};

use super::super::observations::{Observation, ObservationSet};

#[path = "snapshot_identity.rs"]
mod identity;
use identity::{snapshot_transfer_identity_check, SnapshotTransferIdentityCheck};

#[derive(Clone, Debug, Default, Hash)]
pub(crate) struct SnapshotHistory {
    boundary_floor_by_node: BTreeMap<NodeId, LogIndex>,
    boundary_violations: BTreeSet<SnapshotHistoryViolation>,
    geometry_witnesses: BTreeSet<SnapshotGeometryWitness>,
    covered_prefix_violations: BTreeSet<String>,
    next_retained_index_violations: BTreeSet<String>,
    persisted_boundary_violations: BTreeSet<String>,
    payload_binding_violations: BTreeSet<String>,
    payload_binding_coverage_gaps: BTreeSet<String>,
    reference_witnesses_by_log_prefix: BTreeMap<(NodeId, LogIndex), SnapshotReferenceWitness>,
    reference_witnesses_by_snapshot:
        BTreeMap<(NodeId, SnapshotTransferId), SnapshotReferenceWitness>,
    transfer_identity_violations: BTreeSet<String>,
    transfer_identity_instrumentation_errors: BTreeSet<String>,
    chunk_descriptors: BTreeMap<(NodeId, SnapshotTransferId), SnapshotTransferDescriptor>,
    accepted_chunk_witnesses: BTreeSet<AcceptedSnapshotChunkWitness>,
    chunk_identity_violations: BTreeSet<String>,
    chunk_offset_violations: BTreeSet<String>,
    install_completeness_violations: BTreeSet<String>,
    pending_lifecycle_violations: BTreeSet<String>,
    semantic_violations: BTreeSet<String>,
    overwritten_entries: Vec<OverwrittenLogEntry>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SnapshotHistoryViolation {
    pub(crate) node_id: NodeId,
    pub(crate) previous_boundary: LogIndex,
    pub(crate) current_boundary: LogIndex,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct SnapshotGeometryWitness {
    node_id: NodeId,
    snapshot_index: LogIndex,
    first_retained_index: LogIndex,
    last_log_index: LogIndex,
    retained_log_len: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct OverwrittenLogEntry {
    node_id: NodeId,
    index: LogIndex,
    term: Term,
    kind: LogEntryKind,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SnapshotTransferDescriptor {
    leader_id: NodeId,
    snapshot: RaftSnapshot,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SnapshotReferenceWitness {
    boundary_term: Term,
    state: ReferenceState,
}

enum SnapshotReferenceBindingCheck {
    Verified,
    CoverageUnavailable,
    Violation(String),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum AcceptedSnapshotChunkEffect {
    Staged {
        before_received: u64,
        after_received: u64,
    },
    Installed {
        before_received: u64,
        snapshot_index: LogIndex,
    },
}

impl AcceptedSnapshotChunkEffect {
    const fn before_received(self) -> u64 {
        match self {
            Self::Staged {
                before_received, ..
            }
            | Self::Installed {
                before_received, ..
            } => before_received,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct AcceptedSnapshotChunkWitness {
    node_id: NodeId,
    leader_id: NodeId,
    transfer_id: SnapshotTransferId,
    offset: u64,
    chunk_len: u64,
    total_payload_len: u64,
    done: bool,
    effect: AcceptedSnapshotChunkEffect,
}

impl SnapshotHistory {
    pub(super) fn from_cluster(cluster: &Cluster) -> Self {
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

    pub(super) fn observe_cluster(&mut self, cluster: &Cluster) -> ObservationSet {
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

    pub(super) fn record_transition(
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

    fn record_snapshot_references(&mut self, cluster: &Cluster) {
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

    fn record_logical_prefix_references(&mut self, cluster: &Cluster) {
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

    fn record_installed_snapshot_reference(
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

    fn snapshot_reference_binding_check(
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

    fn record_snapshot_semantics(&mut self, before: &Cluster, after: &Cluster, node_id: NodeId) {
        let Some(snapshot) = after.node(node_id).snapshot() else {
            return;
        };
        let new_installs = after
            .snapshot_installs()
            .get(before.snapshot_installs().len()..)
            .unwrap_or_default();
        let recorded_as_snapshot = new_installs.iter().any(|install| {
            install.node_id == node_id
                && install.last_included_index == snapshot.metadata.last_included_index
                && install.last_included_term == snapshot.metadata.last_included_term
        });
        if !recorded_as_snapshot {
            self.semantic_violations.insert(format!(
                "{node_id} advanced snapshot boundary to {} term {} without a matching ApplySnapshot output record",
                snapshot.metadata.last_included_index, snapshot.metadata.last_included_term
            ));
        }
        let new_applies = after
            .applied()
            .get(before.applied().len()..)
            .unwrap_or_default();
        for applied in new_applies.iter().filter(|applied| {
            applied.node_id == node_id && applied.index <= snapshot.metadata.last_included_index
        }) {
            self.semantic_violations.insert(format!(
                "{node_id} emitted log Apply for index {} while installing snapshot boundary {}",
                applied.index, snapshot.metadata.last_included_index
            ));
        }

        let boundary = snapshot.metadata.last_included_index;
        let Some(previous_boundary_term) = before.node(node_id).term_at_index(boundary) else {
            return;
        };
        if previous_boundary_term == snapshot.metadata.last_included_term {
            return;
        }

        let before_bootstrap = before.bootstrap_state(node_id);
        let after_bootstrap = after.bootstrap_state(node_id);
        for entry in before_bootstrap.log {
            if entry.index < boundary || after_bootstrap.log.contains(&entry) {
                continue;
            }
            let overwritten = OverwrittenLogEntry {
                node_id,
                index: entry.index,
                term: entry.term,
                kind: entry.kind,
            };
            if !self.overwritten_entries.contains(&overwritten) {
                self.overwritten_entries.push(overwritten);
            }
        }
    }

    fn record_resurrected_entries(&mut self, cluster: &Cluster) {
        for overwritten in &self.overwritten_entries {
            let bootstrap = cluster.bootstrap_state(overwritten.node_id);
            if bootstrap.log.iter().any(|entry| {
                entry.index == overwritten.index
                    && entry.term == overwritten.term
                    && entry.kind == overwritten.kind
            }) {
                self.semantic_violations.insert(format!(
                    "{} resurrected overwritten log entry {} term {}",
                    overwritten.node_id, overwritten.index, overwritten.term
                ));
            }
        }
    }

    fn record_geometry(
        &mut self,
        cluster: &Cluster,
        node_id: NodeId,
        observations: &mut ObservationSet,
    ) {
        let node = cluster.node(node_id);
        let snapshot_index = node.snapshot_index();
        let first_retained_index = snapshot_index.next();
        let last_log_index = node.last_log_index();
        let retained_log_len = node.log_entries_from(first_retained_index).len();
        self.geometry_witnesses.insert(SnapshotGeometryWitness {
            node_id,
            snapshot_index,
            first_retained_index,
            last_log_index,
            retained_log_len,
        });

        let visible_from_one = node.log_entries_from(LogIndex(1));
        let visible_retained = node.log_entries_from(first_retained_index);
        if visible_from_one == visible_retained {
            observations.mark(Observation::SnapshotCoveredPrefixesChecked);
        } else {
            self.covered_prefix_violations.insert(format!(
                "{node_id} exposed entries covered through snapshot index {snapshot_index}"
            ));
        }

        let bootstrap = cluster.bootstrap_state(node_id);
        let actual_first = bootstrap
            .log
            .first()
            .map_or(first_retained_index, |entry| entry.index);
        let retained_geometry_matches = actual_first == first_retained_index
            && last_log_index >= snapshot_index
            && retained_log_len as u64 == last_log_index.0 - snapshot_index.0;
        if retained_geometry_matches {
            observations.mark(Observation::SnapshotNextRetainedIndicesChecked);
        } else {
            self.next_retained_index_violations.insert(format!(
                "{node_id} retained geometry after snapshot {snapshot_index} was first={actual_first}, last={last_log_index}, len={retained_log_len}; expected first={first_retained_index} and len=last-snapshot"
            ));
        }

        if let Some(entry) = bootstrap
            .log
            .iter()
            .find(|entry| entry.index <= snapshot_index)
        {
            self.persisted_boundary_violations.insert(format!(
                "{node_id} retained persisted entry {} at or behind snapshot index {snapshot_index}",
                entry.index
            ));
        } else {
            observations.mark(Observation::SnapshotPersistedBoundariesChecked);
        }
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

    pub(crate) const fn boundary_violations(&self) -> &BTreeSet<SnapshotHistoryViolation> {
        &self.boundary_violations
    }

    pub(crate) const fn covered_prefix_violations(&self) -> &BTreeSet<String> {
        &self.covered_prefix_violations
    }

    pub(crate) const fn next_retained_index_violations(&self) -> &BTreeSet<String> {
        &self.next_retained_index_violations
    }

    pub(crate) const fn persisted_boundary_violations(&self) -> &BTreeSet<String> {
        &self.persisted_boundary_violations
    }

    pub(crate) const fn payload_binding_violations(&self) -> &BTreeSet<String> {
        &self.payload_binding_violations
    }

    pub(crate) const fn payload_binding_coverage_gaps(&self) -> &BTreeSet<String> {
        &self.payload_binding_coverage_gaps
    }

    pub(crate) const fn transfer_identity_violations(&self) -> &BTreeSet<String> {
        &self.transfer_identity_violations
    }

    pub(crate) const fn transfer_identity_instrumentation_errors(&self) -> &BTreeSet<String> {
        &self.transfer_identity_instrumentation_errors
    }

    pub(crate) const fn chunk_identity_violations(&self) -> &BTreeSet<String> {
        &self.chunk_identity_violations
    }

    pub(crate) const fn chunk_offset_violations(&self) -> &BTreeSet<String> {
        &self.chunk_offset_violations
    }

    pub(crate) const fn install_completeness_violations(&self) -> &BTreeSet<String> {
        &self.install_completeness_violations
    }

    pub(crate) const fn pending_lifecycle_violations(&self) -> &BTreeSet<String> {
        &self.pending_lifecycle_violations
    }

    pub(crate) const fn semantic_violations(&self) -> &BTreeSet<String> {
        &self.semantic_violations
    }
}

fn snapshot_reference_coverage_issue(cluster: &Cluster, node_id: NodeId) -> String {
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

fn accepted_chunk_effect(
    before: &Cluster,
    after: &Cluster,
    node_id: NodeId,
    leader_id: NodeId,
    request: &InstallSnapshotChunk,
) -> Option<AcceptedSnapshotChunkEffect> {
    let after_node = after.node(node_id);
    if snapshot_identity_changed(before, after, node_id) {
        let before_received = before
            .snapshot_staging
            .get(&node_id)
            .filter(|staged| {
                staged.leader_id == leader_id
                    && staged.transfer_id == request.transfer_id
                    && staged.metadata == request.metadata
                    && staged.total_payload_len == request.total_payload_len
                    && staged.application_payload_crc32 == request.application_payload_crc32
            })
            .map_or(0, |staged| staged.bytes.len() as u64);
        return Some(AcceptedSnapshotChunkEffect::Installed {
            before_received,
            snapshot_index: after_node.snapshot_index(),
        });
    }

    let after_staged = after.snapshot_staging.get(&node_id)?;
    let before_staged = before.snapshot_staging.get(&node_id);
    let same_staged_transfer = before_staged.is_some_and(|staged| {
        staged.leader_id == after_staged.leader_id
            && staged.transfer_id == after_staged.transfer_id
            && staged.metadata == after_staged.metadata
            && staged.total_payload_len == after_staged.total_payload_len
            && staged.application_payload_crc32 == after_staged.application_payload_crc32
    });
    let before_received = if same_staged_transfer {
        before_staged.map_or(0, |staged| staged.bytes.len() as u64)
    } else {
        0
    };
    let after_received = after_staged.bytes.len() as u64;
    let expected_after = before_received.checked_add(request.chunk.len() as u64)?;
    let suffix_offset = usize::try_from(before_received).ok()?;
    let suffix = after_staged.bytes.get(suffix_offset..)?;
    (after_received == expected_after && suffix == request.chunk.as_slice()).then_some(
        AcceptedSnapshotChunkEffect::Staged {
            before_received,
            after_received,
        },
    )
}

fn snapshot_identity_changed(before: &Cluster, after: &Cluster, node_id: NodeId) -> bool {
    let before_snapshot = before.node(node_id).snapshot();
    let after_snapshot = after.node(node_id).snapshot();
    before_snapshot != after_snapshot
        || after_snapshot.is_some_and(|snapshot| {
            before.snapshot_payload(node_id, snapshot) != after.snapshot_payload(node_id, snapshot)
        })
}

fn chunk_identity_issue(
    history: &mut SnapshotHistory,
    after: &Cluster,
    envelope: &Envelope,
    request: &InstallSnapshotChunk,
    effect: AcceptedSnapshotChunkEffect,
) -> Option<String> {
    let advertised = SnapshotTransferDescriptor {
        leader_id: request.leader_id,
        snapshot: RaftSnapshot::new(
            request.metadata.clone(),
            request.total_payload_len,
            request.application_payload_crc32,
        ),
    };
    if envelope.from != request.leader_id {
        return Some(format!(
            "{} accepted transfer {} from envelope sender {} but request leader was {}",
            envelope.to, request.transfer_id, envelope.from, request.leader_id
        ));
    }
    if request.transfer_id != advertised.snapshot.transfer_id() {
        return Some(format!(
            "{} accepted transfer id {} with descriptor identity {}",
            envelope.to,
            request.transfer_id,
            advertised.snapshot.transfer_id()
        ));
    }
    let key = (envelope.to, request.transfer_id);
    if let Some(previous) = history.chunk_descriptors.get(&key) {
        if previous != &advertised {
            return Some(format!(
                "{} accepted chunk for transfer {} with a descriptor different from an earlier accepted chunk",
                envelope.to, request.transfer_id
            ));
        }
    } else {
        history.chunk_descriptors.insert(key, advertised.clone());
    }

    let actual_snapshot = match effect {
        AcceptedSnapshotChunkEffect::Staged { .. } => {
            after.snapshot_staging.get(&envelope.to).map(|staged| {
                RaftSnapshot::new(
                    staged.metadata.clone(),
                    staged.total_payload_len,
                    staged.application_payload_crc32,
                )
            })
        }
        AcceptedSnapshotChunkEffect::Installed { .. } => {
            after.node(envelope.to).snapshot().cloned()
        }
    };
    if actual_snapshot.as_ref() != Some(&advertised.snapshot) {
        return Some(format!(
            "{} accepted chunk for transfer {} but retained a different snapshot descriptor",
            envelope.to, request.transfer_id
        ));
    }
    None
}

fn chunk_offset_issue(
    after: &Cluster,
    node_id: NodeId,
    request: &InstallSnapshotChunk,
    effect: AcceptedSnapshotChunkEffect,
) -> Option<String> {
    let chunk_len = request.chunk.len() as u64;
    let Some(end) = request.offset.checked_add(chunk_len) else {
        return Some(format!(
            "{node_id} accepted a snapshot chunk whose byte range overflowed"
        ));
    };
    let valid_shape = request.offset <= request.total_payload_len
        && end <= request.total_payload_len
        && if request.done {
            end == request.total_payload_len
        } else {
            chunk_len > 0 && end < request.total_payload_len
        };
    if !valid_shape {
        return Some(format!(
            "{node_id} accepted invalid chunk range {}..{end} for payload length {} (done={})",
            request.offset, request.total_payload_len, request.done
        ));
    }
    if request.offset != effect.before_received() {
        return Some(format!(
            "{node_id} accepted chunk at offset {} while the staged prefix ended at {}",
            request.offset,
            effect.before_received()
        ));
    }
    if let AcceptedSnapshotChunkEffect::Staged { after_received, .. } = effect {
        if after_received != end {
            return Some(format!(
                "{node_id} recorded {after_received} received bytes after accepting range {}..{end}",
                request.offset
            ));
        }
        if after
            .node(node_id)
            .pending_snapshot_transfer()
            .is_none_or(|pending| pending.received_bytes() != after_received)
        {
            return Some(format!(
                "{node_id} staged {after_received} bytes without matching pending-transfer progress"
            ));
        }
    }
    None
}

fn install_completeness_issue(
    after: &Cluster,
    node_id: NodeId,
    request: &InstallSnapshotChunk,
    effect: AcceptedSnapshotChunkEffect,
) -> Option<String> {
    let AcceptedSnapshotChunkEffect::Installed {
        before_received, ..
    } = effect
    else {
        return None;
    };
    let end = request.offset.checked_add(request.chunk.len() as u64);
    if !request.done || request.offset != before_received || end != Some(request.total_payload_len)
    {
        return Some(format!(
            "{node_id} installed transfer {} before the complete byte range was present: staged={before_received}, offset={}, chunk={}, total={}, done={}",
            request.transfer_id,
            request.offset,
            request.chunk.len(),
            request.total_payload_len,
            request.done
        ));
    }
    if after.snapshot_staging.contains_key(&node_id)
        || after.node(node_id).pending_snapshot_transfer().is_some()
    {
        return Some(format!(
            "{node_id} installed transfer {} but retained partial transfer state",
            request.transfer_id
        ));
    }
    None
}

fn descriptor_from_pending(pending: &PendingSnapshotTransfer) -> SnapshotTransferDescriptor {
    SnapshotTransferDescriptor {
        leader_id: pending.leader_id,
        snapshot: RaftSnapshot::new(
            pending.metadata.clone(),
            pending.total_payload_len,
            pending.application_payload_crc32,
        ),
    }
}

fn pending_snapshot_lifecycle_issue(
    node_id: NodeId,
    installed_snapshot_index: LogIndex,
    pending: &PendingSnapshotTransfer,
) -> Option<String> {
    if pending.is_complete() {
        return Some(format!(
            "{node_id} retained a complete pending snapshot transfer"
        ));
    }
    if pending.metadata.last_included_index <= installed_snapshot_index {
        return Some(format!(
            "{node_id} retained a stale pending snapshot at {} after installing {}",
            pending.metadata.last_included_index, installed_snapshot_index
        ));
    }
    None
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

pub(crate) fn snapshot_payload_binding_issue(cluster: &Cluster, node_id: NodeId) -> Option<String> {
    let snapshot = cluster.node(node_id).snapshot()?;
    let Some(payload) = cluster.snapshot_payload(node_id, snapshot) else {
        return Some(format!(
            "{node_id} published snapshot transfer {} without payload bytes",
            snapshot.transfer_id()
        ));
    };
    if RaftSnapshot::from_payload(snapshot.metadata.clone(), payload) != *snapshot {
        return Some(format!(
            "{node_id} snapshot transfer {} does not bind its metadata to the visible payload bytes",
            snapshot.transfer_id()
        ));
    }
    None
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
