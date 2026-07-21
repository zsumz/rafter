//! Logical-prefix reconstruction and append-only comparisons.

use rafter::LogIndex;

use crate::Cluster;

use super::{LogPrefixWitness, LogicalLogHistory, LogicalLogView};

impl LogicalLogHistory {
    pub(super) fn log_extends(
        &self,
        previous: &LogicalLogView,
        current: &LogicalLogView,
    ) -> Option<bool> {
        let previous_last = logical_last_index(previous);
        if logical_last_index(current) < previous_last {
            return Some(false);
        }
        let previous_prefix = self.prefix_from_view(previous, previous_last)?;
        let current_prefix = self.prefix_from_view(current, previous_last)?;
        Some(previous_prefix == current_prefix)
    }

    #[cfg(test)]
    pub(crate) fn last_view(&self, node_id: rafter::NodeId) -> Option<&LogicalLogView> {
        self.last_views_by_node.get(&node_id)
    }

    pub(crate) fn observed_view(
        &self,
        cluster: &Cluster,
        node_id: rafter::NodeId,
    ) -> LogicalLogView {
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
        Self::default().log_extends(previous, current)
    }

    pub(in crate::model_check) fn prefix_from_view(
        &self,
        view: &LogicalLogView,
        index: LogIndex,
    ) -> Option<LogPrefixWitness> {
        if index == LogIndex::ZERO {
            return Some(LogPrefixWitness::default());
        }

        let mut prefix = match view.snapshot.as_ref() {
            Some(snapshot) if snapshot.index > LogIndex::ZERO => {
                let prefix = snapshot.prefix.as_deref()?;
                if index <= prefix.through() {
                    return prefix.slice_through(index);
                }
                prefix.clone()
            }
            _ => LogPrefixWitness::default(),
        };

        if index < prefix.through() {
            return None;
        }
        let start = prefix.through().0.checked_add(1)?;
        for raw_index in start..=index.0 {
            let entry_index = LogIndex(raw_index);
            let entry = view.entries.get(&entry_index)?;
            if let Some(canonical) = self
                .prefixes_by_index_term
                .get(&(entry_index, entry.term))
                .filter(|canonical| canonical.matches_extension(&prefix, entry_index, entry))
            {
                prefix = canonical.clone();
            } else {
                prefix = prefix.extend(entry_index, entry.clone())?;
            }
        }
        Some(prefix)
    }
}

pub(super) fn logical_last_index(view: &LogicalLogView) -> LogIndex {
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
