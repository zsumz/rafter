//! Leader-side append and snapshot sending.
//!
//! The send path interprets follower progress and emits messages; response
//! handling owns acknowledgement semantics and commit advancement.

use crate::{AppendEntries, LogIndex, Message, NodeId, SharedEntries};

mod cache;

use super::super::state::ProgressMode;
use super::super::{Node, Output};
use cache::LogBatchCache;

/// Whether one replication nudge may remain silent when no payload can move.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::node) enum ReplicationDemand {
    /// Send only when replication state can make progress.
    ProgressOnly,
    /// Reach the follower even when that requires an empty heartbeat.
    EnsureContact,
}

impl ReplicationDemand {
    const fn requires_message(self) -> bool {
        matches!(self, Self::EnsureContact)
    }
}

#[derive(Clone, Copy)]
struct WindowBudget {
    last_log_index: LogIndex,
    max_batches: usize,
    max_bytes: usize,
}

impl Node {
    pub(in crate::node) fn broadcast_append_entries(&mut self) -> Vec<Output> {
        let mut outputs = Vec::new();
        self.broadcast_append_entries_into(&mut outputs);
        outputs
    }

    pub(in crate::node) fn broadcast_append_entries_into(&mut self, outputs: &mut Vec<Output>) {
        self.leader.heartbeat_sequence += 1;
        self.leader.heartbeat_elapsed = 0;
        self.refresh_leader_progress_index();

        let local_id = self.id();
        let replica_count = self.leader.progress.replica_count();
        outputs.reserve(replica_count.saturating_sub(1));

        let mut batch_cache = LogBatchCache::default();
        for slot in 0..replica_count {
            let Some(follower_id) = self.leader.progress.replica_id_at(slot) else {
                continue;
            };
            if follower_id == local_id {
                continue;
            }

            self.replicate_to_follower_with_cache_fresh(
                follower_id,
                ReplicationDemand::EnsureContact,
                outputs,
                &mut batch_cache,
            );
        }
    }

    /// Sends whatever the follower's progress mode admits.
    ///
    /// [`ReplicationDemand::EnsureContact`] emits an empty heartbeat when the
    /// window is full or no entries are pending. Broadcast rounds, check-quorum,
    /// read barriers, and leadership transfer rely on that contact guarantee.
    pub(in crate::node) fn replicate_to_follower(
        &mut self,
        follower_id: NodeId,
        demand: ReplicationDemand,
        outputs: &mut Vec<Output>,
    ) {
        let mut batch_cache = LogBatchCache::default();
        self.replicate_to_follower_with_cache(follower_id, demand, outputs, &mut batch_cache);
    }

    fn replicate_to_follower_with_cache(
        &mut self,
        follower_id: NodeId,
        demand: ReplicationDemand,
        outputs: &mut Vec<Output>,
        batch_cache: &mut LogBatchCache,
    ) {
        self.refresh_leader_progress_index();
        self.replicate_to_follower_with_cache_fresh(follower_id, demand, outputs, batch_cache);
    }

    fn replicate_to_follower_with_cache_fresh(
        &mut self,
        follower_id: NodeId,
        demand: ReplicationDemand,
        outputs: &mut Vec<Output>,
        batch_cache: &mut LogBatchCache,
    ) {
        if follower_id == self.id() || !self.leader.progress.contains(follower_id) {
            return;
        }

        let snapshot_index = self.snapshot_index();
        let window = WindowBudget {
            last_log_index: self.last_log_index(),
            max_batches: self.config.max_inflight_appends(),
            max_bytes: self.config.max_inflight_bytes(),
        };

        let Some(progress) = self.leader.progress.get_mut(follower_id) else {
            return;
        };
        // A follower behind the snapshot boundary needs the snapshot, not the
        // log. Snapshot mode pauses append pipelining until installation.
        if progress.next_index <= snapshot_index
            && !matches!(progress.mode, ProgressMode::Snapshot { .. })
        {
            progress.mode = ProgressMode::Snapshot { next_offset: 0 };
            progress.inflights.clear();
        }
        let mode = progress.mode.clone();

        match mode {
            ProgressMode::Snapshot { .. } => {
                self.replicate_snapshot_to_follower(follower_id, outputs);
            }
            ProgressMode::Probe { awaiting_response } => {
                self.replicate_probe_to_follower(
                    follower_id,
                    awaiting_response,
                    demand,
                    snapshot_index,
                    outputs,
                    batch_cache,
                );
            }
            ProgressMode::Replicate => {
                self.fill_replication_window(follower_id, demand, window, outputs, batch_cache);
            }
        }
    }

    fn replicate_snapshot_to_follower(&mut self, follower_id: NodeId, outputs: &mut Vec<Output>) {
        // Every nudge re-sends the cursor chunk. A later tick retries lost
        // chunks; acknowledgements advance the cursor in the response path.
        if let Some(snapshot) = self.persistent.snapshot.as_ref() {
            outputs.push(self.install_snapshot_chunk_to(follower_id, snapshot));
        }
    }

    fn replicate_probe_to_follower(
        &mut self,
        follower_id: NodeId,
        awaiting_response: bool,
        demand: ReplicationDemand,
        snapshot_index: LogIndex,
        outputs: &mut Vec<Output>,
        batch_cache: &mut LogBatchCache,
    ) {
        if awaiting_response {
            if demand.requires_message() {
                self.send_empty_append_to_progress_next(follower_id, outputs);
            }
            return;
        }

        let Some(next_index) = self
            .leader
            .progress
            .get(follower_id)
            .map(|progress| progress.next_index.max(snapshot_index.next()))
        else {
            return;
        };
        let entries = self
            .log_batch_from_bounded_cached(
                next_index,
                self.config.max_append_entries_bytes(),
                batch_cache,
            )
            .map_or_else(SharedEntries::default, |batch| batch.entries);

        outputs.push(self.append_entries_message(follower_id, next_index, entries));

        let Some(progress) = self.leader.progress.get_mut(follower_id) else {
            return;
        };
        progress.next_index = next_index;
        progress.mode = ProgressMode::Probe {
            awaiting_response: true,
        };
    }

    fn fill_replication_window(
        &mut self,
        follower_id: NodeId,
        demand: ReplicationDemand,
        window: WindowBudget,
        outputs: &mut Vec<Output>,
        batch_cache: &mut LogBatchCache,
    ) {
        let mut sent = false;
        while let Some(progress) = self.leader.progress.get(follower_id) {
            if progress
                .inflights
                .is_full(window.max_batches, window.max_bytes)
                || progress.next_index > window.last_log_index
            {
                break;
            }

            let next_index = progress.next_index;
            let Some(batch) = self.log_batch_from_bounded_cached(
                next_index,
                self.config.max_append_entries_bytes(),
                batch_cache,
            ) else {
                break;
            };
            let last_sent = batch.last_index;
            let batch_bytes = batch.replication_bytes;

            outputs.push(self.append_entries_message(
                follower_id,
                batch.first_index,
                batch.entries,
            ));

            let Some(progress) = self.leader.progress.get_mut(follower_id) else {
                return;
            };
            progress.inflights.record(last_sent, batch_bytes);
            progress.next_index = last_sent.next();
            sent = true;
        }

        if !sent && demand.requires_message() {
            self.send_empty_append_to_progress_next(follower_id, outputs);
        }
    }

    fn send_empty_append_to_progress_next(
        &mut self,
        follower_id: NodeId,
        outputs: &mut Vec<Output>,
    ) {
        let Some(next_index) = self
            .leader
            .progress
            .get(follower_id)
            .map(|progress| progress.next_index)
        else {
            return;
        };
        outputs.push(self.append_entries_message(
            follower_id,
            next_index,
            SharedEntries::default(),
        ));
    }

    fn append_entries_message(
        &self,
        follower_id: NodeId,
        next_index: LogIndex,
        entries: SharedEntries,
    ) -> Output {
        let prev_log_index = LogIndex(next_index.0.saturating_sub(1));
        Output::Send {
            to: follower_id,
            message: Message::AppendEntries(AppendEntries {
                term: self.current_term(),
                leader_id: self.id(),
                prev_log_index,
                prev_log_term: self.term_at(prev_log_index).unwrap_or_default(),
                entries,
                leader_commit: self.volatile.commit_index,
                sequence: self.leader.heartbeat_sequence,
            }),
        }
    }
}
