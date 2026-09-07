//! Fail-closed detection of missing execution-recorder inputs.
//!
//! The AP-02 recorder needs a cursor, a reference state, and enough retained
//! log to cross every newly applied index. Each missing input is reported as
//! an explicit instrumentation error rather than a silently skipped witness.

use rafter::{LogIndex, NodeId};

use crate::Cluster;

use super::ExecutionInstrumentationError;

impl Cluster {
    pub(crate) fn execution_instrumentation_errors(&self) -> Vec<ExecutionInstrumentationError> {
        self.execution_instrumentation_errors_with_log_len(|node_id, first_index| {
            self.log_entries_from(node_id, first_index).len()
        })
    }

    pub(super) fn execution_instrumentation_errors_with_log_len(
        &self,
        retained_log_len: impl Fn(NodeId, LogIndex) -> usize,
    ) -> Vec<ExecutionInstrumentationError> {
        self.nodes
            .keys()
            .filter_map(|node_id| {
                self.execution_instrumentation_error_with_log_len(*node_id, &retained_log_len)
            })
            .collect()
    }

    pub(super) fn execution_instrumentation_error(
        &self,
        node_id: NodeId,
    ) -> Option<ExecutionInstrumentationError> {
        self.execution_instrumentation_error_with_log_len(node_id, &|node_id, first_index| {
            self.log_entries_from(node_id, first_index).len()
        })
    }

    fn execution_instrumentation_error_with_log_len(
        &self,
        node_id: NodeId,
        retained_log_len: &impl Fn(NodeId, LogIndex) -> usize,
    ) -> Option<ExecutionInstrumentationError> {
        let cursor = self
            .execution_cursors
            .get(&node_id)
            .ok_or(ExecutionInstrumentationError::CursorUnavailable { node_id });
        let cursor = match cursor {
            Ok(cursor) => cursor,
            Err(error) => return Some(error),
        };
        let application_epoch = self.application_epoch(node_id);
        let snapshot = self.node(node_id).snapshot();
        let snapshot_boundary = snapshot.map_or(LogIndex::ZERO, |snapshot| {
            snapshot.metadata.last_included_index
        });
        if let Some(snapshot) = snapshot {
            if self
                .snapshot_reference_membership(node_id, snapshot)
                .is_none()
            {
                return Some(
                    ExecutionInstrumentationError::SnapshotReferenceUnavailable {
                        node_id,
                        snapshot_index: snapshot_boundary,
                    },
                );
            }
        }
        let needs_refresh = cursor.application_epoch != application_epoch
            || cursor.applied_through < snapshot_boundary;

        let applied_from = if needs_refresh {
            if let Some(snapshot) = snapshot {
                let snapshot_index = snapshot.metadata.last_included_index;
                if self.snapshot_payload(node_id, snapshot).is_none() {
                    return Some(ExecutionInstrumentationError::SnapshotPayloadUnavailable {
                        node_id,
                        snapshot_index,
                    });
                }
                snapshot_index
            } else {
                if self.initial_reference_state(node_id).is_none() {
                    return Some(ExecutionInstrumentationError::InitialReferenceUnavailable {
                        node_id,
                    });
                }
                LogIndex::ZERO
            }
        } else {
            cursor.applied_through
        };

        let applied_through = self.local_applied_index(node_id);
        if applied_through <= applied_from {
            return None;
        }
        let first_index = applied_from.next();
        let available_entries = retained_log_len(node_id, first_index);
        let required = applied_through.0 - applied_from.0;
        if u64::try_from(available_entries).is_ok_and(|available| available >= required) {
            return None;
        }
        Some(ExecutionInstrumentationError::RetainedLogGap {
            node_id,
            first_index,
            applied_through,
            available_entries,
        })
    }
}
