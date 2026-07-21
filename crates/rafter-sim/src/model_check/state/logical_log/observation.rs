//! Whole-cluster logical-log observation and retained-prefix indexing.

use std::collections::BTreeMap;

use rafter::{LogIndex, NodeId, Role, Term};

use crate::Cluster;

use super::super::super::catalog;
use super::super::super::observations::{Observation, ObservationSet};
use super::{
    comparison::logical_last_index, LogPrefixWitness, LogicalLogHistory, LogicalLogViolation,
};

impl LogicalLogHistory {
    pub(in crate::model_check::state) fn observe_cluster(
        &mut self,
        cluster: &Cluster,
    ) -> ObservationSet {
        let mut observations = ObservationSet::default();
        let views = cluster
            .nodes
            .keys()
            .copied()
            .map(|node_id| {
                let view = super::LogicalLogView::from_cluster(cluster, node_id);
                (node_id, self.attach_snapshot_prefix(node_id, view))
            })
            .collect::<BTreeMap<_, _>>();

        for (node_id, view) in &views {
            self.observe_prefixes(*node_id, view);
        }

        for (node_id, view) in &views {
            let Some(node) = cluster.nodes.get(node_id) else {
                continue;
            };
            if node.role() != Role::Leader {
                continue;
            }
            let key = (*node_id, node.current_term());
            if let Some(previous) = self.leader_logs_by_term.get(&key) {
                match self.log_extends(previous, view) {
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

    fn observe_prefixes(&mut self, node_id: NodeId, view: &super::LogicalLogView) {
        let prefix = match view.snapshot.as_ref() {
            Some(snapshot) if snapshot.index > LogIndex::ZERO => {
                let Some(prefix) = snapshot.prefix.as_deref() else {
                    return;
                };
                prefix.clone()
            }
            Some(_) | None => LogPrefixWitness::default(),
        };
        self.observe_entries(node_id, view, prefix);
    }

    fn observe_entries(
        &mut self,
        node_id: NodeId,
        view: &super::LogicalLogView,
        mut prefix: LogPrefixWitness,
    ) {
        for (index, entry) in &view.entries {
            if let Some(previous) = self.prefixes_by_index_term.get(&(*index, entry.term)) {
                if previous.matches_extension(&prefix, *index, entry) {
                    prefix = previous.clone();
                    continue;
                }
            }
            let Some(candidate) = prefix.extend(*index, entry.clone()) else {
                return;
            };
            prefix = self.reconcile_prefix(node_id, *index, entry.term, candidate);
        }
    }

    pub(super) fn insert_prefix(
        &mut self,
        node_id: NodeId,
        index: LogIndex,
        term: Term,
        prefix: LogPrefixWitness,
    ) {
        self.reconcile_prefix(node_id, index, term, prefix);
    }

    fn reconcile_prefix(
        &mut self,
        node_id: NodeId,
        index: LogIndex,
        term: Term,
        prefix: LogPrefixWitness,
    ) -> LogPrefixWitness {
        let key = (index, term);
        if let Some(previous) = self.prefixes_by_index_term.get(&key) {
            if previous != &prefix {
                self.violations.insert(LogicalLogViolation {
                    invariant: catalog::LG_03_LOG_MATCHING,
                    message: format!(
                        "{node_id} observed a different prefix for log entry ({index}, term {term})"
                    ),
                });
                return prefix;
            }
            return previous.clone();
        }
        self.prefixes_by_index_term.insert(key, prefix.clone());
        prefix
    }
}

#[cfg(test)]
mod tests;
