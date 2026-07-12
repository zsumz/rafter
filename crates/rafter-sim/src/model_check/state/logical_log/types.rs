use std::collections::BTreeMap;

use rafter::{BootstrapState, LogEntry, LogIndex, SnapshotTransferId, Term};

use crate::Cluster;
use rafter::NodeId;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct LogicalLogView {
    pub(crate) snapshot: Option<LogicalSnapshotBoundary>,
    pub(crate) entries: BTreeMap<LogIndex, LogEntry>,
}

impl LogicalLogView {
    pub(crate) fn from_cluster(cluster: &Cluster, node_id: NodeId) -> Self {
        let bootstrap = cluster.bootstrap_state(node_id);
        Self::from_bootstrap(bootstrap)
    }

    fn from_bootstrap(bootstrap: BootstrapState) -> Self {
        let snapshot = bootstrap.snapshot.map(|snapshot| {
            let transfer_id = snapshot.transfer_id();
            LogicalSnapshotBoundary {
                transfer_id,
                index: snapshot.metadata.last_included_index,
                term: snapshot.metadata.last_included_term,
                prefix: None,
            }
        });
        let entries = bootstrap
            .log
            .into_iter()
            .map(|entry| {
                (
                    entry.index,
                    LogEntry {
                        term: entry.term,
                        kind: entry.kind,
                    },
                )
            })
            .collect();
        Self { snapshot, entries }
    }

    pub(crate) fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        self.entries.get(&index)
    }

    pub(crate) fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == LogIndex::ZERO && self.snapshot.is_none() {
            return Some(Term::default());
        }
        if let Some(snapshot) = self.snapshot.as_ref() {
            if index == snapshot.index {
                return Some(snapshot.term);
            }
            if index < snapshot.index {
                return None;
            }
        }
        self.entries.get(&index).map(|entry| entry.term)
    }

    #[cfg(test)]
    pub(crate) fn snapshot_only(
        transfer_id: SnapshotTransferId,
        index: LogIndex,
        term: Term,
        prefix: Option<LogPrefixWitness>,
    ) -> Self {
        Self {
            snapshot: Some(LogicalSnapshotBoundary {
                transfer_id,
                index,
                term,
                prefix: prefix.map(Box::new),
            }),
            entries: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct LogicalSnapshotBoundary {
    pub(crate) transfer_id: SnapshotTransferId,
    pub(crate) index: LogIndex,
    pub(crate) term: Term,
    pub(crate) prefix: Option<Box<LogPrefixWitness>>,
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct LogPrefixWitness {
    pub(crate) through: LogIndex,
    pub(crate) entries: Vec<LogEntry>,
}

impl LogPrefixWitness {
    pub(crate) fn slice_through(&self, index: LogIndex) -> Option<Self> {
        if index > self.through {
            return None;
        }
        let len = usize::try_from(index.0).ok()?;
        if len > self.entries.len() {
            return None;
        }
        Some(Self {
            through: index,
            entries: self.entries[..len].to_vec(),
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct LogicalLogViolation {
    pub(crate) invariant: &'static str,
    pub(crate) message: String,
}
