use rafter::{CommittedConfiguration, Input, LogEntryKind, LogIndex, NodeId, Output, RaftSnapshot};

use super::{Envelope, QueuedEnvelope};
use crate::{
    Applied, Cluster, ExecutedLogEntry, ExecutionCursor, ExecutionWitness, ReadGranted,
    ReferenceState, SnapshotInstalled,
};

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
        let mut emitted = Vec::new();
        for output in outputs {
            match output {
                Output::Apply { index, payload, .. } => {
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
                Output::ApplySnapshot { snapshot } => {
                    self.record_durable_applied(from, snapshot.metadata.last_included_index);
                    // The kernel emits the descriptor only; the content is
                    // the staged transfer completed earlier in this batch.
                    let payload = self.take_installed_snapshot_payload(from, &snapshot);
                    self.reset_execution_cursor_to_snapshot(from, &snapshot, payload.clone());
                    let application_epoch = self.application_epoch(from);
                    self.snapshot_installs.push(SnapshotInstalled {
                        node_id: from,
                        application_epoch,
                        last_included_index: snapshot.metadata.last_included_index,
                        last_included_term: snapshot.metadata.last_included_term,
                        committed_membership: snapshot.metadata.committed_membership().cloned(),
                        payload,
                        applied_records_before_install: self.applied.len(),
                    });
                }
                Output::SendSnapshotChunk { to, chunk } => {
                    // Resolve the byte-free directive against the sending
                    // node's snapshot store and route the materialized wire
                    // message through the normal network path, so drop,
                    // delay, and duplicate faults apply to snapshot chunks
                    // too. An unresolvable directive is a lost message.
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
                Output::StageSnapshotChunk { chunk } => {
                    self.stage_snapshot_chunk(from, chunk);
                }
                Output::ReadIndexGranted {
                    read_id,
                    read_index,
                } => {
                    let application_epoch = self.application_epoch(from);
                    self.read_grants.push(ReadGranted {
                        node_id: from,
                        application_epoch,
                        request_id: read_id.0,
                        read_index,
                        local_applied_index: self.local_applied_index(from),
                    });
                }
                Output::LocalProposalAppended { .. }
                | Output::LocalProposalDropped { .. }
                | Output::RejectProposal { .. }
                | Output::LeadershipTransferRejected { .. }
                | Output::ReadIndexRejected { .. }
                | Output::ReadIndexCanceled { .. } => {}
                Output::Send { to, message } => {
                    let envelope = Envelope { from, to, message };
                    emitted.push(envelope.clone());
                    self.enqueue(envelope);
                }
            }
        }
        self.record_execution_history(from);
        emitted
    }

    pub(crate) fn deliver(&mut self, envelope: Envelope) -> Vec<Envelope> {
        self.record_delivered_acknowledgement(&envelope);
        let outputs = self.node_mut(envelope.to).step(Input::Message {
            from: envelope.from,
            message: envelope.message,
        });
        self.record_outputs(envelope.to, outputs)
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

        let cursor = self
            .execution_cursors
            .get(&node_id)
            .expect("every simulated node has an execution cursor")
            .clone();
        let applied_through = self.local_applied_index(node_id);
        if applied_through <= cursor.applied_through {
            return;
        }

        let first_index = cursor.applied_through.next();
        let entries = self.log_entries_from(node_id, first_index);
        let required = applied_through.0 - cursor.applied_through.0;
        let required_entries = usize::try_from(required)
            .expect("an in-memory log prefix must fit in the platform address space");
        assert!(
            entries.len() >= required_entries,
            "{node_id} applied through {applied_through} without retaining entries from {first_index}"
        );

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
            let prior_state = state;
            let resulting_state = Self::apply_reference_transition(&prior_state, &executed);
            if matches!(
                executed.kind,
                LogEntryKind::Application(_) | LogEntryKind::Configuration(_)
            ) {
                self.execution_history.push(ExecutionWitness {
                    node_id,
                    application_epoch,
                    commit_index_at_emit,
                    entry: executed,
                    prior_state: prior_state.clone(),
                    resulting_state: resulting_state.clone(),
                });
            }
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
    }

    fn refresh_execution_epoch(&mut self, node_id: NodeId) {
        let application_epoch = self.application_epoch(node_id);
        let cursor = self
            .execution_cursors
            .get(&node_id)
            .expect("every simulated node has an execution cursor");
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

        let (applied_through, state) = self.node(node_id).snapshot().map_or_else(
            || (LogIndex::ZERO, self.initial_reference_state(node_id)),
            |snapshot| {
                let payload = self
                    .snapshot_payload(node_id, snapshot)
                    .expect("restarted snapshot payload remains available")
                    .to_vec();
                (
                    snapshot.metadata.last_included_index,
                    self.snapshot_reference_state(node_id, snapshot, payload),
                )
            },
        );
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
        let state = self.snapshot_reference_state(node_id, snapshot, payload);
        self.execution_cursors.insert(
            node_id,
            ExecutionCursor {
                application_epoch: self.application_epoch(node_id),
                applied_through: snapshot.metadata.last_included_index,
                state,
            },
        );
    }

    fn initial_reference_state(&self, node_id: NodeId) -> ReferenceState {
        self.initial_reference_states
            .get(&node_id)
            .expect("every simulated node has an initial reference state")
            .clone()
    }

    fn snapshot_reference_state(
        &self,
        node_id: NodeId,
        snapshot: &RaftSnapshot,
        payload: Vec<u8>,
    ) -> ReferenceState {
        ReferenceState {
            application_value: payload.into(),
            committed_membership: snapshot
                .metadata
                .committed_membership()
                .cloned()
                .unwrap_or_else(|| self.initial_reference_state(node_id).committed_membership),
            committed_configuration: snapshot.metadata.committed_configuration_state(),
        }
    }

    fn apply_reference_transition(
        prior: &ReferenceState,
        entry: &ExecutedLogEntry,
    ) -> ReferenceState {
        let mut result = prior.clone();
        match &entry.kind {
            LogEntryKind::Application(payload) => {
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
