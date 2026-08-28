//! Typed reasons the simulator cannot extend a node's execution history.
//!
//! Each variant names a missing instrumentation input — no cursor, an
//! unretained log range, an unavailable snapshot payload or reference — so a
//! coverage gap is reported as a gap instead of passing as a clean run. These
//! are harness diagnostics, never protocol failures.

use std::fmt;

use rafter::{LogIndex, NodeId};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum ExecutionInstrumentationError {
    CursorUnavailable {
        node_id: NodeId,
    },
    RetainedLogGap {
        node_id: NodeId,
        first_index: LogIndex,
        applied_through: LogIndex,
        available_entries: usize,
    },
    SnapshotPayloadUnavailable {
        node_id: NodeId,
        snapshot_index: LogIndex,
    },
    SnapshotReferenceUnavailable {
        node_id: NodeId,
        snapshot_index: LogIndex,
    },
    InitialReferenceUnavailable {
        node_id: NodeId,
    },
}

impl fmt::Display for ExecutionInstrumentationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CursorUnavailable { node_id } => {
                write!(formatter, "{node_id} has no execution-history cursor")
            }
            Self::RetainedLogGap {
                node_id,
                first_index,
                applied_through,
                available_entries,
            } => write!(
                formatter,
                "{node_id} applied through {applied_through} without retaining every execution-history entry from {first_index} ({available_entries} available)"
            ),
            Self::SnapshotPayloadUnavailable {
                node_id,
                snapshot_index,
            } => write!(
                formatter,
                "{node_id} cannot resume execution history at snapshot index {snapshot_index}: snapshot payload is missing"
            ),
            Self::SnapshotReferenceUnavailable {
                node_id,
                snapshot_index,
            } => write!(
                formatter,
                "{node_id} cannot resume execution history at snapshot index {snapshot_index}: snapshot reference state is missing"
            ),
            Self::InitialReferenceUnavailable { node_id } => write!(
                formatter,
                "{node_id} cannot resume execution history: initial reference state is missing"
            ),
        }
    }
}
