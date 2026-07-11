use std::collections::BTreeMap;

use rafter::{BootstrapState, LogEntry, LogIndex, Term};

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
        let snapshot = bootstrap.snapshot.map(|snapshot| LogicalSnapshotBoundary {
            index: snapshot.metadata.last_included_index,
            term: snapshot.metadata.last_included_term,
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
        if let Some(snapshot) = self.snapshot {
            if index == snapshot.index {
                return Some(snapshot.term);
            }
            if index < snapshot.index {
                return None;
            }
        }
        self.entries.get(&index).map(|entry| entry.term)
    }

    pub(crate) fn snapshot_covers(&self, index: LogIndex) -> bool {
        self.snapshot
            .is_some_and(|snapshot| snapshot.index >= index && index > LogIndex::ZERO)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct LogicalSnapshotBoundary {
    pub(crate) index: LogIndex,
    pub(crate) term: Term,
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct LogPrefixWitness {
    pub(crate) through: LogIndex,
    pub(crate) entries: Vec<LogEntry>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct LogicalLogViolation {
    pub(crate) invariant: &'static str,
    pub(crate) message: String,
}
