use std::collections::{BTreeMap, BTreeSet};

use rafter::{LogIndex, Message, NodeId, SnapshotTransferId, Term};

use crate::{Cluster, Envelope};

use super::super::catalog;
use super::super::observations::{Observation, ObservationSet};

mod types;

pub(crate) use types::{LogPrefixWitness, LogicalLogView, LogicalLogViolation};

#[derive(Clone, Debug, Default, Hash)]
pub(crate) struct LogicalLogHistory {
    pub(crate) leader_logs_by_term: BTreeMap<(NodeId, Term), LogicalLogView>,
    pub(crate) prefixes_by_index_term: BTreeMap<(LogIndex, Term), LogPrefixWitness>,
    pub(crate) snapshot_prefixes_by_owner_transfer:
        BTreeMap<(NodeId, SnapshotTransferId), LogPrefixWitness>,
    pub(crate) unwitnessed_snapshots: BTreeSet<(NodeId, SnapshotTransferId, LogIndex, Term)>,
    last_views_by_node: BTreeMap<NodeId, LogicalLogView>,
    pub(crate) violations: BTreeSet<LogicalLogViolation>,
    pub(crate) append_prev_log_violations: BTreeSet<LogicalLogViolation>,
    pub(crate) append_stored_suffix_violations: BTreeSet<LogicalLogViolation>,
}

impl LogicalLogHistory {
    pub(super) fn observe_cluster(&mut self, cluster: &Cluster) -> ObservationSet {
        let mut observations = ObservationSet::default();
        let views = cluster
            .nodes
            .keys()
            .copied()
            .map(|node_id| {
                let view = LogicalLogView::from_cluster(cluster, node_id);
                (node_id, self.attach_snapshot_prefix(node_id, view))
            })
            .collect::<BTreeMap<_, _>>();

        for (node_id, view) in &views {
            self.observe_prefixes(*node_id, view);
        }

        for (node_id, view) in &views {
            let node = cluster
                .nodes
                .get(node_id)
                .expect("observed node must exist");
            if node.role() != rafter::Role::Leader {
                continue;
            }
            let key = (*node_id, node.current_term());
            if let Some(previous) = self.leader_logs_by_term.get(&key) {
                match Self::log_extends(previous, view) {
                    Some(true) if logical_last_index(view) > logical_last_index(previous) => {
                        observations.mark(Observation::SameTermLeaderLogGrowth);
                    }
                    Some(true) | None => {}
                    Some(false) => {
                        self.violations.insert(LogicalLogViolation {
                            invariant: catalog::LG_01_LEADER_APPEND_ONLY,
                            message: format!(
                                "{node_id} leader term {} rewrote or deleted its own log",
                                node.current_term()
                            ),
                        });
                    }
                }
            }
            self.leader_logs_by_term.insert(key, view.clone());
        }

        self.last_views_by_node = views;
        observations
    }

    fn observe_prefixes(&mut self, node_id: NodeId, view: &LogicalLogView) {
        for (index, entry) in &view.entries {
            if let Some(prefix) = Self::prefix_from_view(view, *index) {
                self.insert_prefix(node_id, *index, entry.term, prefix);
            }
        }
    }

    fn insert_prefix(
        &mut self,
        node_id: NodeId,
        index: LogIndex,
        term: Term,
        prefix: LogPrefixWitness,
    ) {
        let key = (index, term);
        if let Some(previous) = self.prefixes_by_index_term.get(&key) {
            if previous != &prefix {
                self.violations.insert(LogicalLogViolation {
                    invariant: catalog::LG_03_LOG_MATCHING,
                    message: format!(
                        "{node_id} observed a different prefix for log entry ({index}, term {term})"
                    ),
                });
            }
            return;
        }
        self.prefixes_by_index_term.insert(key, prefix);
    }

    fn attach_snapshot_prefix(
        &mut self,
        node_id: NodeId,
        mut view: LogicalLogView,
    ) -> LogicalLogView {
        let Some(snapshot) = view.snapshot.as_ref() else {
            return view;
        };
        let transfer_id = snapshot.transfer_id;
        let index = snapshot.index;
        let term = snapshot.term;
        let local_prefix = self
            .last_views_by_node
            .get(&node_id)
            .and_then(|previous| Self::prefix_from_view(previous, index));
        let recorded_prefix = self
            .snapshot_prefixes_by_owner_transfer
            .get(&(node_id, transfer_id))
            .cloned();
        if let (Some(local), Some(recorded)) = (&local_prefix, &recorded_prefix) {
            if local != recorded {
                self.violations.insert(LogicalLogViolation {
                    invariant: catalog::LG_03_LOG_MATCHING,
                    message: format!(
                        "{node_id} snapshot transfer {transfer_id} conflicts with its local logical prefix"
                    ),
                });
            }
        }
        let prefix = local_prefix.or(recorded_prefix);

        let Some(prefix) = prefix else {
            self.unwitnessed_snapshots
                .insert((node_id, transfer_id, index, term));
            return view;
        };
        self.unwitnessed_snapshots
            .remove(&(node_id, transfer_id, index, term));
        if !self.insert_snapshot_prefix(node_id, transfer_id, index, term, prefix.clone()) {
            self.unwitnessed_snapshots
                .insert((node_id, transfer_id, index, term));
            return view;
        }
        if let Some(snapshot) = view.snapshot.as_mut() {
            snapshot.prefix = Some(Box::new(prefix));
        }
        view
    }

    fn insert_snapshot_prefix(
        &mut self,
        node_id: NodeId,
        transfer_id: SnapshotTransferId,
        index: LogIndex,
        term: Term,
        prefix: LogPrefixWitness,
    ) -> bool {
        let boundary_matches = prefix.through == index
            && usize::try_from(index.0)
                .is_ok_and(|expected_len| prefix.entries.len() == expected_len)
            && prefix
                .entries
                .last()
                .map_or(index == LogIndex::ZERO, |entry| entry.term == term);
        if !boundary_matches {
            self.violations.insert(LogicalLogViolation {
                invariant: catalog::LG_03_LOG_MATCHING,
                message: format!(
                    "{node_id} snapshot transfer {transfer_id} has a logical-prefix witness that does not match boundary ({index}, term {term})"
                ),
            });
            return false;
        }
        self.insert_prefix(node_id, index, term, prefix.clone());
        let key = (node_id, transfer_id);
        if let Some(previous) = self.snapshot_prefixes_by_owner_transfer.get(&key) {
            if previous != &prefix {
                self.violations.insert(LogicalLogViolation {
                    invariant: catalog::LG_03_LOG_MATCHING,
                    message: format!(
                        "{node_id} observed snapshot transfer {transfer_id} with a different logical prefix"
                    ),
                });
                return false;
            }
            return true;
        }
        self.snapshot_prefixes_by_owner_transfer.insert(key, prefix);
        true
    }

    pub(super) fn record_snapshot_installation(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        delivered: Option<&Envelope>,
    ) {
        let Some(envelope) = delivered else {
            return;
        };
        if !matches!(
            envelope.message,
            Message::InstallSnapshot(_) | Message::InstallSnapshotChunk(_)
        ) {
            return;
        }
        let before_snapshot = before.bootstrap_state(envelope.to).snapshot;
        let Some(after_snapshot) = after.bootstrap_state(envelope.to).snapshot else {
            return;
        };
        if before_snapshot.as_ref() == Some(&after_snapshot) {
            return;
        }
        let transfer_id = after_snapshot.transfer_id();
        let index = after_snapshot.metadata.last_included_index;
        let term = after_snapshot.metadata.last_included_term;
        let source_view = self
            .last_views_by_node
            .get(&envelope.from)
            .cloned()
            .unwrap_or_else(|| LogicalLogView::from_cluster(before, envelope.from));
        let Some(prefix) = Self::prefix_from_view(&source_view, index) else {
            return;
        };
        if self.insert_snapshot_prefix(envelope.to, transfer_id, index, term, prefix) {
            // A witnessed install intentionally replaces the receiver's old
            // logical prefix, which may be divergent. The next observation
            // must attach from the transfer witness, not that retired view.
            self.last_views_by_node.remove(&envelope.to);
        }
    }

    fn log_extends(previous: &LogicalLogView, current: &LogicalLogView) -> Option<bool> {
        let previous_last = logical_last_index(previous);
        if logical_last_index(current) < previous_last {
            return Some(false);
        }
        let previous_prefix = Self::prefix_from_view(previous, previous_last)?;
        let current_prefix = Self::prefix_from_view(current, previous_last)?;
        Some(previous_prefix == current_prefix)
    }

    #[cfg(test)]
    pub(crate) fn last_view(&self, node_id: NodeId) -> Option<&LogicalLogView> {
        self.last_views_by_node.get(&node_id)
    }

    pub(crate) fn observed_view(&self, cluster: &Cluster, node_id: NodeId) -> LogicalLogView {
        self.last_views_by_node
            .get(&node_id)
            .cloned()
            .unwrap_or_else(|| LogicalLogView::from_cluster(cluster, node_id))
    }

    #[cfg(test)]
    pub(crate) fn observed_log_extends(
        previous: &LogicalLogView,
        current: &LogicalLogView,
    ) -> Option<bool> {
        Self::log_extends(previous, current)
    }

    pub(super) fn record_append_entries_delivery(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        delivered: Option<&Envelope>,
        emitted: &[Envelope],
    ) -> ObservationSet {
        let mut observations = ObservationSet::default();
        let Some(envelope) = delivered else {
            return observations;
        };
        let Message::AppendEntries(request) = &envelope.message else {
            return observations;
        };
        let Some(response) = emitted.iter().find_map(|emitted| {
            if emitted.from != envelope.to || emitted.to != envelope.from {
                return None;
            }
            match &emitted.message {
                Message::AppendEntriesResponse(response)
                    if response.follower_id == envelope.to
                        && response.sequence == request.sequence =>
                {
                    Some(*response)
                }
                _ => None,
            }
        }) else {
            return observations;
        };
        if !response.success {
            return observations;
        }
        if !request.entries.is_empty() {
            observations.mark(Observation::SuccessfulNonemptyAppendObservations);
        }

        let before_view = LogicalLogView::from_cluster(before, envelope.to);
        let after_view = LogicalLogView::from_cluster(after, envelope.to);
        if before_view.term_at(request.prev_log_index) == Some(request.prev_log_term) {
            observations.mark(Observation::SuccessfulAppendPrevLogMatches);
        } else {
            self.record_append_prev_log_violation(format!(
                "{} accepted AppendEntries from {} without matching prev ({}, term {})",
                envelope.to, envelope.from, request.prev_log_index, request.prev_log_term
            ));
        }

        let expected_match_index =
            LogIndex(request.prev_log_index.0 + request.entries.len() as u64);
        let match_index_matches = response.match_index == expected_match_index;
        if !match_index_matches {
            self.record_append_stored_suffix_violation(format!(
                "{} reported match index {} for append ending at {}",
                envelope.to, response.match_index, expected_match_index
            ));
        }

        let mut stored_suffix_matches = true;
        for (offset, entry) in request.entries.iter().enumerate() {
            let index = LogIndex(request.prev_log_index.0 + offset as u64 + 1);
            if after_view.entry_at(index) == Some(entry) {
                continue;
            }
            stored_suffix_matches = false;
            self.record_append_stored_suffix_violation(format!(
                "{} acknowledged AppendEntries without storing leader entry at index {}",
                envelope.to, index
            ));
            break;
        }
        if !request.entries.is_empty() && match_index_matches && stored_suffix_matches {
            observations.mark(Observation::SuccessfulAppendStoredSuffixMatches);
        }
        observations
    }

    fn record_append_prev_log_violation(&mut self, message: String) {
        let violation = LogicalLogViolation {
            invariant: catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE,
            message,
        };
        self.append_prev_log_violations.insert(violation.clone());
        self.violations.insert(violation);
    }

    fn record_append_stored_suffix_violation(&mut self, message: String) {
        let violation = LogicalLogViolation {
            invariant: catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE,
            message,
        };
        self.append_stored_suffix_violations
            .insert(violation.clone());
        self.violations.insert(violation);
    }

    pub(in crate::model_check) fn prefix_from_view(
        view: &LogicalLogView,
        index: LogIndex,
    ) -> Option<LogPrefixWitness> {
        if index == LogIndex::ZERO {
            return Some(LogPrefixWitness::default());
        }

        let mut prefix = match view.snapshot.as_ref() {
            Some(snapshot) if snapshot.index > LogIndex::ZERO => {
                let prefix = snapshot.prefix.as_deref().cloned()?;
                if index <= prefix.through {
                    return prefix.slice_through(index);
                }
                prefix
            }
            _ => LogPrefixWitness::default(),
        };

        if index < prefix.through {
            return prefix.slice_through(index);
        }
        for raw_index in prefix.through.0 + 1..=index.0 {
            let index = LogIndex(raw_index);
            let entry = view.entries.get(&index)?;
            prefix.entries.push(entry.clone());
            prefix.through = index;
        }
        Some(prefix)
    }
}

fn logical_last_index(view: &LogicalLogView) -> LogIndex {
    let snapshot_index = view
        .snapshot
        .as_ref()
        .map_or(LogIndex::ZERO, |snapshot| snapshot.index);
    view.entries
        .keys()
        .next_back()
        .copied()
        .unwrap_or(snapshot_index)
        .max(snapshot_index)
}
