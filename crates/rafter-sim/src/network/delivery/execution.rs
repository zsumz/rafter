//! Execution-cursor maintenance for the AP-02 reference application.
//!
//! The cursor tracks the exact reference state each node has folded in. A
//! snapshot install or an application-epoch change rebases it onto the new
//! boundary; ordinary applies advance it entry by entry.

use rafter::{CommittedConfiguration, LogEntryKind, LogIndex, NodeId, RaftSnapshot};

use crate::{Cluster, ExecutedLogEntry, ExecutionCursor, ReferenceState};

impl Cluster {
    #[cfg(test)]
    pub(crate) fn rewind_execution_cursor_for_fixture(&mut self, node_id: NodeId) {
        let state = self.initial_reference_state(node_id).unwrap_or_else(|| {
            std::panic::panic_any("fixture node has an initial reference state")
        });
        self.execution_cursors.insert(
            node_id,
            ExecutionCursor {
                application_epoch: self.application_epoch(node_id),
                applied_through: LogIndex::ZERO,
                state,
            },
        );
    }

    pub(super) fn refresh_execution_epoch(&mut self, node_id: NodeId) {
        let application_epoch = self.application_epoch(node_id);
        if self.execution_instrumentation_error(node_id).is_some() {
            return;
        }
        let Some(cursor) = self.execution_cursors.get(&node_id) else {
            return;
        };
        let snapshot_boundary = self
            .node(node_id)
            .snapshot()
            .map_or(LogIndex::ZERO, |snapshot| {
                snapshot.metadata.last_included_index
            });
        if cursor.application_epoch == application_epoch
            && cursor.applied_through >= snapshot_boundary
        {
            return;
        }

        let (applied_through, state) = if let Some(snapshot) = self.node(node_id).snapshot() {
            let Some(payload) = self
                .snapshot_payload(node_id, snapshot)
                .map(ToOwned::to_owned)
            else {
                return;
            };
            let Some(state) = self.snapshot_reference_state(node_id, snapshot, payload) else {
                return;
            };
            (snapshot.metadata.last_included_index, state)
        } else {
            let Some(state) = self.initial_reference_state(node_id) else {
                return;
            };
            (LogIndex::ZERO, state)
        };
        self.execution_cursors.insert(
            node_id,
            ExecutionCursor {
                application_epoch,
                applied_through,
                state,
            },
        );
    }

    pub(super) fn reset_execution_cursor_to_snapshot(
        &mut self,
        node_id: NodeId,
        snapshot: &RaftSnapshot,
        payload: Vec<u8>,
    ) {
        let Some(state) = self.snapshot_reference_state(node_id, snapshot, payload) else {
            return;
        };
        self.execution_cursors.insert(
            node_id,
            ExecutionCursor {
                application_epoch: self.application_epoch(node_id),
                applied_through: snapshot.metadata.last_included_index,
                state,
            },
        );
    }

    pub(super) fn initial_reference_state(&self, node_id: NodeId) -> Option<ReferenceState> {
        self.initial_reference_states.get(&node_id).cloned()
    }

    fn snapshot_reference_state(
        &self,
        node_id: NodeId,
        snapshot: &RaftSnapshot,
        payload: Vec<u8>,
    ) -> Option<ReferenceState> {
        let committed_membership = self.snapshot_reference_membership(node_id, snapshot)?;
        Some(ReferenceState {
            application_value: payload.into(),
            committed_membership,
            committed_configuration: snapshot.metadata.committed_configuration_state(),
        })
    }

    pub(super) fn snapshot_reference_membership(
        &self,
        node_id: NodeId,
        snapshot: &RaftSnapshot,
    ) -> Option<rafter::MembershipConfig> {
        snapshot
            .metadata
            .committed_membership()
            .cloned()
            .or_else(|| {
                self.initial_reference_state(node_id)
                    .map(|state| state.committed_membership)
            })
    }

    pub(super) fn apply_reference_transition(
        prior: &ReferenceState,
        entry: &ExecutedLogEntry,
        emitted_application_payload: Option<&rafter::SharedPayload>,
    ) -> ReferenceState {
        let mut result = prior.clone();
        match &entry.kind {
            LogEntryKind::Application(_) => {
                let Some(payload) = emitted_application_payload else {
                    return result;
                };
                result.application_value.clone_from(payload);
            }
            LogEntryKind::Configuration(configuration) => {
                result.committed_membership = configuration.membership_config();
                result.committed_configuration = Some(CommittedConfiguration {
                    index: entry.index,
                    config_id: configuration.config_id(),
                });
            }
            LogEntryKind::Noop => {}
        }
        result
    }
}
