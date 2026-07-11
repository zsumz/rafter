use std::collections::{BTreeMap, BTreeSet};

use rafter::{LogIndex, Message, NodeId, Term};

use crate::{Cluster, Envelope};

use super::super::catalog;

mod types;

pub(crate) use types::{LogPrefixWitness, LogicalLogView, LogicalLogViolation};

#[derive(Clone, Debug, Default, Hash)]
pub(crate) struct LogicalLogHistory {
    pub(crate) leader_logs_by_term: BTreeMap<(NodeId, Term), LogicalLogView>,
    pub(crate) prefixes_by_index_term: BTreeMap<(LogIndex, Term), LogPrefixWitness>,
    pub(crate) violations: BTreeSet<LogicalLogViolation>,
}

impl LogicalLogHistory {
    pub(super) fn observe_cluster(&mut self, cluster: &Cluster) {
        let views = cluster
            .nodes
            .keys()
            .copied()
            .map(|node_id| (node_id, LogicalLogView::from_cluster(cluster, node_id)))
            .collect::<BTreeMap<_, _>>();

        for (node_id, view) in &views {
            self.observe_prefixes(*node_id, view);
        }

        for (node_id, view) in views {
            let node = cluster
                .nodes
                .get(&node_id)
                .expect("observed node must exist");
            if node.role() != rafter::Role::Leader {
                continue;
            }
            let key = (node_id, node.current_term());
            if let Some(previous) = self.leader_logs_by_term.get(&key) {
                if !self.log_extends(previous, &view) {
                    self.violations.insert(LogicalLogViolation {
                        invariant: catalog::LG_01_LEADER_APPEND_ONLY,
                        message: format!(
                            "{node_id} leader term {} rewrote or deleted its own log",
                            node.current_term()
                        ),
                    });
                }
            }
            self.leader_logs_by_term.insert(key, view);
        }
    }

    fn observe_prefixes(&mut self, node_id: NodeId, view: &LogicalLogView) {
        for (index, entry) in &view.entries {
            if let Some(prefix) = self.prefix_from_view(view, *index) {
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

    fn log_extends(&self, previous: &LogicalLogView, current: &LogicalLogView) -> bool {
        for (index, entry) in &previous.entries {
            if current.entry_at(*index) == Some(entry) {
                continue;
            }
            if current.snapshot_covers(*index) {
                let Some(previous_prefix) = self.prefix_from_view(previous, *index) else {
                    return false;
                };
                let Some(current_prefix) = self.prefix_from_view(current, *index) else {
                    return false;
                };
                if previous_prefix == current_prefix {
                    continue;
                }
            }
            return false;
        }
        true
    }

    pub(super) fn record_append_entries_delivery(
        &mut self,
        before: &Cluster,
        after: &Cluster,
        delivered: Option<&Envelope>,
        emitted: &[Envelope],
    ) {
        let Some(envelope) = delivered else {
            return;
        };
        let Message::AppendEntries(request) = &envelope.message else {
            return;
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
            return;
        };
        if !response.success {
            return;
        }

        let before_view = LogicalLogView::from_cluster(before, envelope.to);
        let after_view = LogicalLogView::from_cluster(after, envelope.to);
        if before_view.term_at(request.prev_log_index) != Some(request.prev_log_term) {
            self.violations.insert(LogicalLogViolation {
                invariant: catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE,
                message: format!(
                    "{} accepted AppendEntries from {} without matching prev ({}, term {})",
                    envelope.to, envelope.from, request.prev_log_index, request.prev_log_term
                ),
            });
        }

        let expected_match_index =
            LogIndex(request.prev_log_index.0 + request.entries.len() as u64);
        if response.match_index != expected_match_index {
            self.violations.insert(LogicalLogViolation {
                invariant: catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE,
                message: format!(
                    "{} reported match index {} for append ending at {}",
                    envelope.to, response.match_index, expected_match_index
                ),
            });
        }

        for (offset, entry) in request.entries.iter().enumerate() {
            let index = LogIndex(request.prev_log_index.0 + offset as u64 + 1);
            if after_view.entry_at(index) == Some(entry) {
                continue;
            }
            self.violations.insert(LogicalLogViolation {
                invariant: catalog::LG_02_TRUTHFUL_APPEND_ENTRIES_ACCEPTANCE,
                message: format!(
                    "{} acknowledged AppendEntries without storing leader entry at index {}",
                    envelope.to, index
                ),
            });
            break;
        }
    }

    pub(super) fn prefix_from_view(
        &self,
        view: &LogicalLogView,
        index: LogIndex,
    ) -> Option<LogPrefixWitness> {
        if index == LogIndex::ZERO {
            return Some(LogPrefixWitness::default());
        }

        let mut prefix = match view.snapshot {
            Some(snapshot) if snapshot.index > LogIndex::ZERO => self
                .prefixes_by_index_term
                .get(&(snapshot.index, snapshot.term))
                .cloned()?,
            _ => LogPrefixWitness::default(),
        };

        if index < prefix.through {
            return None;
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
