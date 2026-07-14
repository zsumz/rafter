use rafter::{CommittedConfiguration, Input, LogEntryKind, LogIndex, NodeId, Output, RaftSnapshot};

use super::{Envelope, QueuedEnvelope};
use crate::records::{ProposalRejected, RecordedOutputs, TransferRejected};
use crate::{
    Applied, Cluster, ExecutedLogEntry, ExecutionCursor, ExecutionWitness, ReadGranted,
    ReadTerminalOutput, ReferenceState, SnapshotInstalled,
};

#[path = "delivery_error.rs"]
mod error;
#[path = "delivery_proposal.rs"]
mod proposal;

pub(crate) use error::ExecutionInstrumentationError;
use proposal::local_proposal_event;

impl Cluster {
    /// Returns the currently queued message envelopes in network order.
    pub fn pending(&self) -> impl Iterator<Item = &Envelope> {
        self.network.iter().map(|queued| &queued.envelope)
    }

    /// Delivers every message that is ready at the current simulator tick.
    pub fn deliver_all(&mut self) {
        while let Some(position) = self.ready_position_matching(|_| true) {
            let Some(queued) = self.network.remove(position) else {
                break;
            };
            self.deliver(queued.envelope);
        }
    }

    /// Delivers one ready message matching `predicate`.
    pub fn deliver_one_matching(&mut self, mut predicate: impl FnMut(&Envelope) -> bool) -> bool {
        let Some(position) = self.ready_position_matching(&mut predicate) else {
            return false;
        };
        let Some(queued) = self.network.remove(position) else {
            return false;
        };
        self.deliver(queued.envelope);
        true
    }

    /// Delivers one message directly, bypassing the queued network.
    pub fn deliver_message(&mut self, from: NodeId, to: NodeId, message: rafter::Message) {
        self.deliver(Envelope { from, to, message });
    }

    /// Queues one message for normal simulated delivery.
    pub fn queue_message(&mut self, from: NodeId, to: NodeId, message: rafter::Message) {
        self.enqueue(Envelope { from, to, message });
    }

    /// Delivers all ready messages matching `predicate`.
    pub fn deliver_matching(&mut self, mut predicate: impl FnMut(&Envelope) -> bool) -> usize {
        let mut delivered = 0;

        while let Some(position) = self.ready_position_matching(&mut predicate) {
            let Some(queued) = self.network.remove(position) else {
                break;
            };
            delivered += 1;
            self.deliver(queued.envelope);
        }

        delivered
    }

    /// Delivers one random ready message and returns its envelope.
    pub fn deliver_random_ready(&mut self) -> Option<Envelope> {
        let position = self.random_ready_position()?;
        let queued = self.network.remove(position)?;
        let delivered = queued.envelope.clone();
        self.deliver(queued.envelope);
        Some(delivered)
    }

    pub(crate) fn random_ready_position(&mut self) -> Option<usize> {
        let positions: Vec<_> = self
            .network
            .iter()
            .enumerate()
            .filter_map(|(position, queued)| {
                (queued.ready_at <= self.clock.now()).then_some(position)
            })
            .collect();
        (!positions.is_empty()).then(|| positions[self.rng.index(positions.len())])
    }

    pub(crate) fn pending_envelope_at(&self, position: usize) -> Option<&Envelope> {
        self.network.get(position).map(|queued| &queued.envelope)
    }

    pub(crate) fn record_outputs(&mut self, from: NodeId, outputs: Vec<Output>) -> Vec<Envelope> {
        self.record_outputs_observed(from, outputs).emitted
    }

    pub(crate) fn record_outputs_observed(
        &mut self,
        from: NodeId,
        outputs: Vec<Output>,
    ) -> RecordedOutputs {
        let mut emitted = Vec::new();
        let mut local_proposals = Vec::new();
        for output in outputs {
            if let Some(event) = local_proposal_event(from, &output) {
                local_proposals.push(event);
            }
            self.record_output(from, output, &mut emitted);
        }
        self.record_execution_history(from);
        RecordedOutputs {
            emitted,
            local_proposals,
        }
    }

    fn record_output(&mut self, from: NodeId, output: Output, emitted: &mut Vec<Envelope>) {
        match output {
            Output::Apply { index, payload, .. } => self.record_apply(from, index, payload),
            Output::ApplySnapshot { snapshot } => self.record_snapshot_apply(from, &snapshot),
            Output::SendSnapshotChunk { to, chunk } => {
                self.record_snapshot_send(from, to, &chunk, emitted);
            }
            Output::StageSnapshotChunk { chunk } => self.stage_snapshot_chunk(from, chunk),
            Output::ReadIndexGranted {
                read_id,
                read_index,
            } => {
                let operation_id = self.pending_read_operation_id(from, read_id.0);
                self.record_read_output_correlation(from, read_id.0, operation_id, "grant");
                let application_epoch = self.application_epoch(from);
                self.read_grants.push(ReadGranted {
                    node_id: from,
                    operation_id,
                    application_epoch,
                    request_id: read_id.0,
                    read_index,
                    local_applied_index: self.local_applied_index(from),
                });
            }
            Output::ReadIndexRejected { read_id, reason } => {
                let operation_id = self.pending_read_operation_id(from, read_id.0);
                self.record_read_output_correlation(from, read_id.0, operation_id, "rejection");
                self.read_terminal_outputs
                    .push(ReadTerminalOutput::Rejected {
                        node_id: from,
                        operation_id,
                        request_id: read_id.0,
                        reason,
                    });
            }
            Output::ReadIndexCanceled { read_id, reason } => {
                let operation_id = self.pending_read_operation_id(from, read_id.0);
                self.record_read_output_correlation(from, read_id.0, operation_id, "cancellation");
                self.read_terminal_outputs
                    .push(ReadTerminalOutput::Canceled {
                        node_id: from,
                        operation_id,
                        request_id: read_id.0,
                        reason,
                    });
            }
            Output::RejectProposal {
                proposal_id,
                reason: _,
            } => self.proposal_rejections.push(ProposalRejected {
                node_id: from,
                proposal_id,
            }),
            Output::LeadershipTransferRejected { target, reason: _ } => {
                self.transfer_rejections.push(TransferRejected {
                    node_id: from,
                    target,
                });
            }
            Output::LocalProposalAppended { .. } | Output::LocalProposalDropped { .. } => {}
            Output::Send { to, message } => {
                let envelope = Envelope { from, to, message };
                emitted.push(envelope.clone());
                self.enqueue(envelope);
            }
        }
    }

    fn pending_read_operation_id(&self, node_id: NodeId, request_id: u64) -> Option<u64> {
        self.read_registrations
            .iter()
            .filter(|registration| {
                registration.node_id == node_id && registration.request_id == request_id
            })
            .map(|registration| registration.operation_id)
            .find(|operation_id| {
                !self.retired_read_operations.contains(operation_id)
                    && !self
                        .read_grants
                        .iter()
                        .any(|grant| grant.operation_id == Some(*operation_id))
                    && !self
                        .read_terminal_outputs
                        .iter()
                        .any(|terminal| terminal.operation_id() == Some(*operation_id))
            })
    }

    pub(crate) fn retire_pending_reads(&mut self, node_id: NodeId) {
        let pending = self
            .read_registrations
            .iter()
            .filter(|registration| registration.node_id == node_id)
            .map(|registration| registration.operation_id)
            .filter(|operation_id| {
                !self.retired_read_operations.contains(operation_id)
                    && !self
                        .read_grants
                        .iter()
                        .any(|grant| grant.operation_id == Some(*operation_id))
                    && !self
                        .read_terminal_outputs
                        .iter()
                        .any(|terminal| terminal.operation_id() == Some(*operation_id))
            })
            .collect::<Vec<_>>();
        self.retired_read_operations.extend(pending);
    }

    fn record_read_output_correlation(
        &mut self,
        node_id: NodeId,
        request_id: u64,
        operation_id: Option<u64>,
        output: &str,
    ) {
        if operation_id.is_none() {
            self.read_output_correlation_errors.insert(format!(
                "{node_id} emitted read {output} for uncorrelated request {request_id}"
            ));
        }
    }

    fn record_apply(&mut self, from: NodeId, index: LogIndex, payload: rafter::SharedPayload) {
        let commit_index_at_emit = self.commit_index(from);
        self.record_durable_applied(from, index);
        let application_epoch = self.application_epoch(from);
        self.applied.push(Applied {
            node_id: from,
            application_epoch,
            commit_index_at_emit,
            index,
            payload,
        });
    }

    fn record_snapshot_apply(&mut self, from: NodeId, snapshot: &RaftSnapshot) {
        let commit_index_at_emit = self.commit_index(from);
        self.record_durable_applied(from, snapshot.metadata.last_included_index);
        let payload = self.take_installed_snapshot_payload(from, snapshot);
        self.reset_execution_cursor_to_snapshot(from, snapshot, payload.clone());
        let application_epoch = self.application_epoch(from);
        self.snapshot_installs.push(SnapshotInstalled {
            node_id: from,
            application_epoch,
            commit_index_at_emit,
            last_included_index: snapshot.metadata.last_included_index,
            last_included_term: snapshot.metadata.last_included_term,
            committed_membership: snapshot.metadata.committed_membership().cloned(),
            payload,
            applied_records_before_install: self.applied.len(),
        });
    }

    fn record_snapshot_send(
        &mut self,
        from: NodeId,
        to: NodeId,
        chunk: &rafter::SnapshotChunkSend,
        emitted: &mut Vec<Envelope>,
    ) {
        let resolved = self
            .snapshot_sources
            .get(&from)
            .and_then(|source| chunk.resolve(source));
        if let Some(message) = resolved {
            let envelope = Envelope {
                from,
                to,
                message: rafter::Message::InstallSnapshotChunk(message),
            };
            emitted.push(envelope.clone());
            self.enqueue(envelope);
        }
    }

    pub(crate) fn deliver_observed(&mut self, envelope: Envelope) -> RecordedOutputs {
        self.record_delivered_acknowledgement(&envelope);
        let to = envelope.to;
        let outputs = self.node_mut(to).step(Input::Message {
            from: envelope.from,
            message: envelope.message,
        });
        self.record_outputs_observed(to, outputs)
    }

    pub(crate) fn deliver(&mut self, envelope: Envelope) -> Vec<Envelope> {
        self.deliver_observed(envelope).emitted
    }

    /// A delivered success acknowledgement raises the sender's durable-loss
    /// floor: from this moment a leader has counted the entries through the
    /// acknowledged index, so a legal lossy restart must keep them.
    fn record_delivered_acknowledgement(&mut self, envelope: &Envelope) {
        let acknowledged = match &envelope.message {
            rafter::Message::AppendEntriesResponse(response) if response.success => {
                Some((response.follower_id, response.match_index))
            }
            rafter::Message::InstallSnapshotResponse(response) if response.success => {
                Some((response.follower_id, response.last_included_index))
            }
            _ => None,
        };
        if let Some((node_id, index)) = acknowledged {
            let floor = self.delivered_ack_floor.entry(node_id).or_default();
            *floor = (*floor).max(index);
        }
    }

    fn record_durable_applied(&mut self, node_id: NodeId, index: LogIndex) {
        let floor = self.durable_applied.entry(node_id).or_default();
        *floor = (*floor).max(index);
    }

    fn record_execution_history(&mut self, node_id: NodeId) {
        self.refresh_execution_epoch(node_id);

        if self.execution_instrumentation_error(node_id).is_some() {
            return;
        }
        let Some(cursor) = self.execution_cursors.get(&node_id).cloned() else {
            return;
        };
        let applied_through = self.local_applied_index(node_id);
        if applied_through <= cursor.applied_through {
            return;
        }

        let first_index = cursor.applied_through.next();
        let entries = self.log_entries_from(node_id, first_index);
        let required = applied_through.0 - cursor.applied_through.0;
        let Ok(required_entries) = usize::try_from(required) else {
            return;
        };

        let application_epoch = self.application_epoch(node_id);
        let commit_index_at_emit = self.commit_index(node_id);
        let mut state = cursor.state;
        for (index, entry) in
            (first_index.0..=applied_through.0).zip(entries.into_iter().take(required_entries))
        {
            let executed = ExecutedLogEntry {
                index: LogIndex(index),
                term: entry.term,
                kind: entry.kind,
            };
            let emitted_application_payload =
                matches!(&executed.kind, LogEntryKind::Application(_))
                    .then(|| {
                        self.applied
                            .iter()
                            .find(|applied| {
                                applied.node_id == node_id
                                    && applied.application_epoch == application_epoch
                                    && applied.index == executed.index
                            })
                            .map(|applied| applied.payload.clone())
                    })
                    .flatten();
            let prior_state = state;
            let resulting_state = Self::apply_reference_transition(
                &prior_state,
                &executed,
                emitted_application_payload.as_ref(),
            );
            self.execution_history.push(ExecutionWitness {
                node_id,
                application_epoch,
                commit_index_at_emit,
                entry: executed,
                emitted_application_payload,
                prior_state: prior_state.clone(),
                resulting_state: resulting_state.clone(),
            });
            state = resulting_state;
        }

        self.execution_cursors.insert(
            node_id,
            ExecutionCursor {
                application_epoch,
                applied_through,
                state,
            },
        );
        self.record_durable_applied(node_id, applied_through);
    }

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

    fn refresh_execution_epoch(&mut self, node_id: NodeId) {
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

    fn reset_execution_cursor_to_snapshot(
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

    fn initial_reference_state(&self, node_id: NodeId) -> Option<ReferenceState> {
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

    fn snapshot_reference_membership(
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

    pub(crate) fn execution_instrumentation_errors(&self) -> Vec<ExecutionInstrumentationError> {
        self.execution_instrumentation_errors_with_log_len(|node_id, first_index| {
            self.log_entries_from(node_id, first_index).len()
        })
    }

    fn execution_instrumentation_errors_with_log_len(
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

    fn execution_instrumentation_error(
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

    fn apply_reference_transition(
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

    fn enqueue(&mut self, envelope: Envelope) {
        if self.blocked_pairs.contains(&(envelope.from, envelope.to)) {
            return;
        }
        self.network.push_back(QueuedEnvelope {
            ready_at: self.clock.now(),
            envelope,
        });
    }

    fn ready_position_matching(
        &self,
        mut predicate: impl FnMut(&Envelope) -> bool,
    ) -> Option<usize> {
        self.network
            .iter()
            .position(|queued| queued.ready_at <= self.clock.now() && predicate(&queued.envelope))
    }
}

#[cfg(test)]
#[path = "delivery_read_tests.rs"]
mod read_tests;

#[cfg(test)]
#[path = "delivery_execution_tests.rs"]
mod tests;
