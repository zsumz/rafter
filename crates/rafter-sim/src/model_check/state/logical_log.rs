//! Immutable logical-log evidence retained across model transitions.
//!
//! The history records enough information to detect temporal log violations
//! without changing protocol behavior. Prefix witnesses share immutable entry
//! storage; their equality and hashes still describe only the visible prefix.

use std::collections::{BTreeMap, BTreeSet};

use rafter::{LogIndex, NodeId, SnapshotTransferId, Term};

mod append;
mod comparison;
mod observation;
mod snapshot;
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
