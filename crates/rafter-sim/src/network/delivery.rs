use rafter::{Input, LogIndex, NodeId, Output};

use super::{Envelope, QueuedEnvelope};
use crate::{Applied, Cluster, ReadGranted, SnapshotInstalled};

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

    pub(crate) fn record_outputs(&mut self, from: NodeId, outputs: Vec<Output>) {
        for output in outputs {
            match output {
                Output::Apply { index, payload, .. } => {
                    self.record_durable_applied(from, index);
                    let application_epoch = self.application_epoch(from);
                    self.applied.push(Applied {
                        node_id: from,
                        application_epoch,
                        index,
                        payload,
                    });
                }
                Output::ApplySnapshot { snapshot } => {
                    self.record_durable_applied(from, snapshot.metadata.last_included_index);
                    // The kernel emits the descriptor only; the content is
                    // the staged transfer completed earlier in this batch.
                    let payload = self.take_installed_snapshot_payload(from, &snapshot);
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
                        self.enqueue(Envelope {
                            from,
                            to,
                            message: rafter::Message::InstallSnapshotChunk(message),
                        });
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
                    self.enqueue(Envelope { from, to, message });
                }
            }
        }
    }

    pub(crate) fn deliver(&mut self, envelope: Envelope) {
        self.record_delivered_acknowledgement(&envelope);
        let outputs = self.node_mut(envelope.to).step(Input::Message {
            from: envelope.from,
            message: envelope.message,
        });
        self.record_outputs(envelope.to, outputs);
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
