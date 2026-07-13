use std::fmt;

use rafter::{CommittedConfiguration, Input, LogEntryKind, LogIndex, NodeId, Output, RaftSnapshot};

use super::{Envelope, QueuedEnvelope};
use crate::{
    Applied, Cluster, ExecutedLogEntry, ExecutionCursor, ExecutionWitness, ReadGranted,
    ReferenceState, SnapshotInstalled,
};

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

#[cfg(test)]
mod tests {
    use rafter::{
        ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
        BootstrapLogEntry, BootstrapState, LogIndex, Node, NodeConfig, NodeId, RaftSnapshot,
        RaftSnapshotMetadata, SnapshotGroupId, Term,
    };

    use super::{Cluster, ExecutionCursor, ExecutionInstrumentationError};

    const NODE_ID: NodeId = NodeId(1);

    #[test]
    fn recorder_detects_each_missing_execution_input() {
        let mut cursor = one_node_cluster();
        cursor.execution_cursors.remove(&NODE_ID);
        assert!(!cursor.execution_cursors.contains_key(&NODE_ID));
        cursor.record_execution_history(NODE_ID);

        let mut initial_reference = one_node_cluster();
        initial_reference.application_epochs.insert(NODE_ID, 1);
        initial_reference.initial_reference_states.remove(&NODE_ID);
        assert!(!initial_reference
            .initial_reference_states
            .contains_key(&NODE_ID));
        initial_reference.record_execution_history(NODE_ID);

        let mut snapshot_payload = cluster_with_snapshot(true, false);
        let snapshot = snapshot_payload
            .node(NODE_ID)
            .snapshot()
            .expect("fixture has a snapshot");
        assert!(snapshot_payload
            .snapshot_payload(NODE_ID, snapshot)
            .is_none());
        snapshot_payload.record_execution_history(NODE_ID);

        let mut snapshot_reference = cluster_with_snapshot(false, true);
        snapshot_reference.initial_reference_states.remove(&NODE_ID);
        let snapshot = snapshot_reference
            .node(NODE_ID)
            .snapshot()
            .expect("fixture has a snapshot");
        assert!(snapshot_reference
            .snapshot_reference_membership(NODE_ID, snapshot)
            .is_none());
        snapshot_reference.record_execution_history(NODE_ID);

        let cases = [
            (
                cursor.execution_instrumentation_errors(),
                ExecutionInstrumentationError::CursorUnavailable { node_id: NODE_ID },
            ),
            (
                initial_reference.execution_instrumentation_errors(),
                ExecutionInstrumentationError::InitialReferenceUnavailable { node_id: NODE_ID },
            ),
            (
                snapshot_payload.execution_instrumentation_errors(),
                ExecutionInstrumentationError::SnapshotPayloadUnavailable {
                    node_id: NODE_ID,
                    snapshot_index: LogIndex(2),
                },
            ),
            (
                snapshot_reference.execution_instrumentation_errors(),
                ExecutionInstrumentationError::SnapshotReferenceUnavailable {
                    node_id: NODE_ID,
                    snapshot_index: LogIndex(2),
                },
            ),
        ];

        for (actual, expected) in cases {
            assert_eq!(actual, vec![expected]);
        }
    }

    #[test]
    fn recorder_detects_a_retained_log_gap_from_its_log_lookup() {
        let cluster = cluster_applied_through_two();
        assert!(cluster.execution_instrumentation_errors().is_empty());
        let mut retained_entries = cluster.log_entries_from(NODE_ID, LogIndex(1));
        assert_eq!(retained_entries.len(), 2);
        retained_entries.remove(0);

        let errors = cluster.execution_instrumentation_errors_with_log_len(|_, first_index| {
            assert_eq!(first_index, LogIndex(1));
            retained_entries.len()
        });

        assert_eq!(
            errors,
            vec![ExecutionInstrumentationError::RetainedLogGap {
                node_id: NODE_ID,
                first_index: LogIndex(1),
                applied_through: LogIndex(2),
                available_entries: 1,
            }]
        );
    }

    fn one_node_cluster() -> Cluster {
        Cluster::new(vec![node_config()])
    }

    fn node_config() -> NodeConfig {
        NodeConfig::new(NODE_ID, vec![NodeId(2), NodeId(3)], 3).expect("test node config is valid")
    }

    fn cluster_applied_through_two() -> Cluster {
        let mut cluster = one_node_cluster();
        let bootstrap = BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(2),
            committed_configuration: None,
            snapshot: None,
            log: vec![
                BootstrapLogEntry::application(LogIndex(1), Term(1), b"one".to_vec()),
                BootstrapLogEntry::application(LogIndex(2), Term(1), b"two".to_vec()),
            ],
        };
        let node = Node::from_bootstrap_applied_through(node_config(), bootstrap, LogIndex(2))
            .expect("fixture bootstrap is valid");
        cluster.nodes.insert(NODE_ID, node);
        cluster
    }

    fn cluster_with_snapshot(include_reference_membership: bool, seed_payload: bool) -> Cluster {
        let mut cluster = one_node_cluster();
        let payload = b"snapshot-state".to_vec();
        let metadata = RaftSnapshotMetadata::new(
            SnapshotGroupId::new("execution-recorder").expect("valid snapshot group"),
            NODE_ID,
            LogIndex(2),
            Term(1),
            Term(1),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("register").expect("valid snapshot kind"),
                ApplicationSnapshotVersion::new(1).expect("valid snapshot version"),
            ),
        )
        .expect("valid snapshot metadata");
        let metadata = if include_reference_membership {
            metadata.with_committed_membership(
                cluster.initial_reference_states[&NODE_ID]
                    .committed_membership
                    .clone(),
            )
        } else {
            metadata
        };
        let snapshot = RaftSnapshot::from_payload(metadata, &payload);
        if seed_payload {
            cluster.seed_snapshot_payload(NODE_ID, &snapshot, payload);
        }
        let bootstrap = BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(2),
            committed_configuration: None,
            snapshot: Some(snapshot),
            log: Vec::new(),
        };
        let node = Node::from_bootstrap_applied_through(node_config(), bootstrap, LogIndex(2))
            .expect("snapshot fixture bootstrap is valid");
        cluster.nodes.insert(NODE_ID, node);
        cluster.execution_cursors.insert(
            NODE_ID,
            ExecutionCursor {
                application_epoch: 0,
                applied_through: LogIndex::ZERO,
                state: cluster.initial_reference_states[&NODE_ID].clone(),
            },
        );
        cluster
    }
}
