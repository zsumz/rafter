use rafter::{Input, NodeId, Output};

use crate::{Applied, Cluster, ReadGranted, SnapshotInstalled};

/// A queued or delivered Raft message with simulator routing metadata.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub message: rafter::Message,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct QueuedEnvelope {
    pub(super) ready_at: crate::SimTick,
    pub(super) envelope: Envelope,
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
        let positions: Vec<_> = self
            .network
            .iter()
            .enumerate()
            .filter_map(|(position, queued)| {
                (queued.ready_at <= self.clock.now()).then_some(position)
            })
            .collect();
        if positions.is_empty() {
            return None;
        }

        let position = positions[self.rng.index(positions.len())];
        let queued = self.network.remove(position)?;
        let delivered = queued.envelope.clone();
        self.deliver(queued.envelope);
        Some(delivered)
    }

    /// Delays all queued messages matching `predicate`.
    pub fn delay_matching(
        &mut self,
        mut predicate: impl FnMut(&Envelope) -> bool,
        delay_ticks: u64,
    ) -> usize {
        let ready_at = self.clock.now().after(delay_ticks);
        let mut delayed = 0;

        for queued in &mut self.network {
            if predicate(&queued.envelope) {
                delayed += 1;
                queued.ready_at = std::cmp::max(queued.ready_at, ready_at);
            }
        }

        delayed
    }

    /// Enqueues one extra copy of every deliverable message matching
    /// `predicate` — the at-least-once delivery fault. Copies keep their
    /// original readiness; messages still held back by a delay are not
    /// duplicated until they become deliverable. Returns the number of
    /// copies enqueued.
    pub fn duplicate_matching(&mut self, mut predicate: impl FnMut(&Envelope) -> bool) -> usize {
        let now = self.clock.now();
        let copies: Vec<QueuedEnvelope> = self
            .network
            .iter()
            .filter(|queued| queued.ready_at <= now && predicate(&queued.envelope))
            .cloned()
            .collect();
        let duplicated = copies.len();
        self.network.extend(copies);
        duplicated
    }

    /// Drops all queued messages matching `predicate`.
    pub fn drop_matching(&mut self, mut predicate: impl FnMut(&Envelope) -> bool) -> usize {
        let queued = self.network.len();
        let mut dropped = 0;

        for _ in 0..queued {
            let Some(queued) = self.network.pop_front() else {
                break;
            };
            if predicate(&queued.envelope) {
                dropped += 1;
            } else {
                self.network.push_back(queued);
            }
        }

        dropped
    }

    pub(super) fn record_outputs(&mut self, from: NodeId, outputs: Vec<Output>) {
        for output in outputs {
            match output {
                Output::Apply { index, payload, .. } => {
                    self.applied.push(Applied {
                        node_id: from,
                        index,
                        payload,
                    });
                }
                Output::ApplySnapshot { snapshot } => {
                    // The kernel emits the descriptor only; the content is
                    // the staged transfer completed earlier in this batch.
                    let payload = self.take_installed_snapshot_payload(from, &snapshot);
                    self.snapshot_installs.push(SnapshotInstalled {
                        node_id: from,
                        last_included_index: snapshot.metadata.last_included_index,
                        last_included_term: snapshot.metadata.last_included_term,
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
                    self.read_grants.push(ReadGranted {
                        node_id: from,
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

    pub(super) fn deliver(&mut self, envelope: Envelope) {
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

    fn enqueue(&mut self, envelope: Envelope) {
        if self.blocked_pairs.contains(&(envelope.from, envelope.to)) {
            return;
        }
        self.network.push_back(QueuedEnvelope {
            ready_at: self.clock.now(),
            envelope,
        });
    }

    /// Installs a sustained bidirectional partition between `a` and `b`:
    /// in-flight traffic between them is dropped and nothing new crosses
    /// until [`Cluster::heal_partitions`]. Returns the envelopes purged.
    pub fn partition_between(&mut self, a: NodeId, b: NodeId) -> usize {
        self.blocked_pairs.insert((a, b));
        self.blocked_pairs.insert((b, a));
        self.purge_blocked()
    }

    /// Partitions `node` away from every other node.
    pub fn partition_isolate(&mut self, node: NodeId) -> usize {
        let others: Vec<NodeId> = self
            .nodes
            .keys()
            .copied()
            .filter(|other| *other != node)
            .collect();
        for other in others {
            self.blocked_pairs.insert((node, other));
            self.blocked_pairs.insert((other, node));
        }
        self.purge_blocked()
    }

    /// Removes every sustained partition; traffic flows again (messages
    /// dropped while partitioned stay dropped, as on a real network).
    pub fn heal_partitions(&mut self) {
        self.blocked_pairs.clear();
    }

    /// Whether a sustained partition currently blocks `from` -> `to`.
    #[must_use]
    pub fn partitioned(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked_pairs.contains(&(from, to))
    }

    fn purge_blocked(&mut self) -> usize {
        let blocked = &self.blocked_pairs;
        let before = self.network.len();
        self.network
            .retain(|queued| !blocked.contains(&(queued.envelope.from, queued.envelope.to)));
        before - self.network.len()
    }

    /// Mutates every queued envelope matching `predicate` in place: field-level
    /// corruption injection. Codec frame checksums reject byte-level damage at
    /// decode; this hook targets direct in-memory kernel resilience. The kernel
    /// must absorb malformed fields without panicking, though byzantine field
    /// values can legitimately break cluster-level guarantees, so this belongs
    /// in targeted scenarios, never in invariant-checked soaks.
    pub fn corrupt_queued_matching(
        &mut self,
        mut predicate: impl FnMut(&Envelope) -> bool,
        mut mutate: impl FnMut(&mut rafter::Message),
    ) -> usize {
        let mut corrupted = 0;
        for queued in &mut self.network {
            if predicate(&queued.envelope) {
                mutate(&mut queued.envelope.message);
                corrupted += 1;
            }
        }
        corrupted
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
