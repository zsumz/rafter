//! Retained-log geometry and install semantics after a snapshot boundary.
//!
//! Installing a snapshot must emit the matching output record, expose no entry
//! at or behind the boundary, and never resurrect an entry the install
//! overwrote; each of those is checked here against the observed cluster.

use rafter::{LogIndex, NodeId, PendingSnapshotTransfer};

use crate::Cluster;

use super::super::super::observations::{Observation, ObservationSet};
use super::{OverwrittenLogEntry, SnapshotGeometryWitness, SnapshotHistory};

impl SnapshotHistory {
    pub(super) fn record_snapshot_semantics(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        node_id: NodeId,
    ) {
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

    pub(super) fn record_resurrected_entries(&mut self, cluster: &Cluster) {
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

    pub(super) fn record_geometry(
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
}

pub(super) fn pending_snapshot_lifecycle_issue(
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
