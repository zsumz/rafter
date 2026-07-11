use rafter::NodeId;

use super::{Envelope, QueuedEnvelope};
use crate::Cluster;

impl Cluster {
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
    /// `predicate` - the at-least-once delivery fault. Copies keep their
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
}
