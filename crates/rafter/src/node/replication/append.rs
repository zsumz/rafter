use crate::{AppendEntries, LogIndex, Message, NodeId, SharedEntries};

use super::super::log::LogBatch;
use super::super::state::ProgressMode;
use super::super::{Node, Output};

#[derive(Default)]
pub(super) struct LogBatchCache {
    batches: Vec<CachedLogBatch>,
}

struct CachedLogBatch {
    first_index: LogIndex,
    max_replication_bytes: usize,
    batch: LogBatch,
}

#[derive(Clone, Copy)]
struct WindowFill {
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
            let Some(peer) = self.leader.progress.replica_id_at(slot) else {
                continue;
            };
            if peer == local_id {
                continue;
            }
            self.replicate_to_peer_with_cache_fresh(peer, true, outputs, &mut batch_cache);
        }
    }

    /// Sends whatever `peer`'s progress mode admits. With `ensure_message`,
    /// at least one message goes out - an empty heartbeat when the window is
    /// full or nothing is pending - so a broadcast round reaches every
    /// follower; check-quorum and read barriers depend on that.
    pub(in crate::node) fn replicate_to_peer(
        &mut self,
        peer: NodeId,
        ensure_message: bool,
        outputs: &mut Vec<Output>,
    ) {
        let mut batch_cache = LogBatchCache::default();
        self.replicate_to_peer_with_cache(peer, ensure_message, outputs, &mut batch_cache);
    }

    pub(super) fn replicate_to_peer_with_cache(
        &mut self,
        peer: NodeId,
        ensure_message: bool,
        outputs: &mut Vec<Output>,
        batch_cache: &mut LogBatchCache,
    ) {
        self.refresh_leader_progress_index();
        self.replicate_to_peer_with_cache_fresh(peer, ensure_message, outputs, batch_cache);
    }

    fn replicate_to_peer_with_cache_fresh(
        &mut self,
        peer: NodeId,
        ensure_message: bool,
        outputs: &mut Vec<Output>,
        batch_cache: &mut LogBatchCache,
    ) {
        if peer == self.id() || !self.leader.progress.contains(peer) {
            return;
        }

        let snapshot_index = self.snapshot_index();
        let last_log_index = self.last_log_index();
        let max_batches = self.config.max_inflight_appends();
        let max_bytes = self.config.max_inflight_bytes();

        let Some(progress) = self.leader.progress.get_mut(peer) else {
            return;
        };
        // A follower behind the snapshot boundary needs the snapshot, not
        // the log; the transfer pauses append pipelining until it installs.
        if progress.next_index <= snapshot_index
            && !matches!(progress.mode, ProgressMode::Snapshot { .. })
        {
            progress.mode = ProgressMode::Snapshot { next_offset: 0 };
            progress.inflights.clear();
        }
        let mode = progress.mode.clone();

        match mode {
            ProgressMode::Snapshot { .. } => {
                self.replicate_snapshot_to_peer(peer, outputs);
            }
            ProgressMode::Probe { awaiting_response } => {
                self.replicate_probe_to_peer(
                    peer,
                    awaiting_response,
                    ensure_message,
                    snapshot_index,
                    outputs,
                    batch_cache,
                );
            }
            ProgressMode::Replicate => {
                self.replicate_window_to_peer(
                    peer,
                    ensure_message,
                    WindowFill {
                        last_log_index,
                        max_batches,
                        max_bytes,
                    },
                    outputs,
                    batch_cache,
                );
            }
        }
    }

    fn replicate_snapshot_to_peer(&mut self, peer: NodeId, outputs: &mut Vec<Output>) {
        // Every nudge re-sends the cursor chunk: lost chunks are retried by the
        // next tick, and acknowledgements advance the cursor through the
        // snapshot response path.
        if let Some(snapshot) = self.persistent.snapshot.as_ref() {
            outputs.push(self.install_snapshot_chunk_to(peer, snapshot));
        }
    }

    fn replicate_probe_to_peer(
        &mut self,
        peer: NodeId,
        awaiting_response: bool,
        ensure_message: bool,
        snapshot_index: LogIndex,
        outputs: &mut Vec<Output>,
        batch_cache: &mut LogBatchCache,
    ) {
        if awaiting_response {
            if ensure_message {
                self.send_empty_append_to_progress_next(peer, outputs);
            }
            return;
        }

        let Some(next_index) = self
            .leader
            .progress
            .get(peer)
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
        outputs.push(self.append_entries_message(peer, next_index, entries));
        let Some(progress) = self.leader.progress.get_mut(peer) else {
            return;
        };
        progress.next_index = next_index;
        progress.mode = ProgressMode::Probe {
            awaiting_response: true,
        };
    }

    fn replicate_window_to_peer(
        &mut self,
        peer: NodeId,
        ensure_message: bool,
        fill: WindowFill,
        outputs: &mut Vec<Output>,
        batch_cache: &mut LogBatchCache,
    ) {
        let mut sent = false;
        while let Some(progress) = self.leader.progress.get(peer) {
            if progress.inflights.is_full(fill.max_batches, fill.max_bytes)
                || progress.next_index > fill.last_log_index
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
            outputs.push(self.append_entries_message(peer, batch.first_index, batch.entries));
            let Some(progress) = self.leader.progress.get_mut(peer) else {
                return;
            };
            progress.inflights.record(last_sent, batch_bytes);
            progress.next_index = last_sent.next();
            sent = true;
        }
        if !sent && ensure_message {
            self.send_empty_append_to_progress_next(peer, outputs);
        }
    }

    fn send_empty_append_to_progress_next(&mut self, peer: NodeId, outputs: &mut Vec<Output>) {
        let Some(next_index) = self
            .leader
            .progress
            .get(peer)
            .map(|progress| progress.next_index)
        else {
            return;
        };
        outputs.push(self.append_entries_message(peer, next_index, SharedEntries::default()));
    }

    fn log_batch_from_bounded_cached(
        &self,
        first_index: LogIndex,
        max_replication_bytes: usize,
        batch_cache: &mut LogBatchCache,
    ) -> Option<LogBatch> {
        if let Some(cached) = batch_cache.batches.iter().find(|cached| {
            cached.first_index == first_index
                && cached.max_replication_bytes == max_replication_bytes
        }) {
            return Some(cached.batch.clone());
        }

        let batch = self.log_batch_from_bounded(first_index, max_replication_bytes)?;
        batch_cache.batches.push(CachedLogBatch {
            first_index,
            max_replication_bytes,
            batch: batch.clone(),
        });
        Some(batch)
    }

    fn append_entries_message(
        &self,
        peer: NodeId,
        next_index: LogIndex,
        entries: SharedEntries,
    ) -> Output {
        let prev_log_index = LogIndex(next_index.0.saturating_sub(1));
        Output::Send {
            to: peer,
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
