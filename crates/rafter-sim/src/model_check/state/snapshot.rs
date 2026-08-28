//! Snapshot history: boundaries, transfers, and the evidence behind them.
//!
//! Every boundary advance is retained alongside its geometry, payload binding,
//! reference, chunk, and transfer-identity findings, and a check that could
//! not be performed is recorded as a coverage gap, so an install never counts
//! as verified on evidence that was never gathered.

use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use rafter::{InstallSnapshotChunk, Message};
use rafter::{LogEntryKind, LogIndex, NodeId, RaftSnapshot, SnapshotTransferId, Term};

#[cfg(test)]
use crate::Envelope;
use crate::{Cluster, ReferenceState};

#[cfg(test)]
use super::super::observations::{Observation, ObservationSet};

#[path = "snapshot_identity.rs"]
mod identity;

mod chunks;
mod geometry;
mod reference;
mod transitions;

use chunks::{
    accepted_chunk_effect, chunk_identity_issue, chunk_offset_issue, descriptor_from_pending,
    install_completeness_issue,
};
use geometry::pending_snapshot_lifecycle_issue;
use reference::snapshot_reference_coverage_issue;

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
