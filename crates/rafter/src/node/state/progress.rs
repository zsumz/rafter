//! Per-replica replication progress and the bounded in-flight window.
//!
//! These types model the leader's view of one replica. Membership-to-slot
//! indexing remains in [`super::membership`].

use std::collections::VecDeque;

use crate::LogIndex;

/// One replica's replication state as the leader sees it (thesis 10.2.1 and
/// the Progress/Inflights discipline).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::node) struct Progress {
    pub match_index: LogIndex,
    pub next_index: LogIndex,
    pub mode: ProgressMode,
    pub inflights: Inflights,
}

impl Progress {
    /// A fresh follower view: nothing matched, probing at `next_index` until
    /// the follower confirms its log position.
    pub(in crate::node) fn probing(next_index: LogIndex) -> Self {
        Self {
            match_index: LogIndex::ZERO,
            next_index,
            mode: ProgressMode::Probe {
                awaiting_response: false,
            },
            inflights: Inflights::default(),
        }
    }

    /// The leader's own progress entry: fully matched, never sent to.
    pub(in crate::node) fn local(last_log_index: LogIndex) -> Self {
        Self {
            match_index: last_log_index,
            next_index: last_log_index.next(),
            mode: ProgressMode::Replicate,
            inflights: Inflights::default(),
        }
    }

    /// Collapses to probing after a rejection: the in-flight window is
    /// forfeited and `next_index` walks back one step, never below the
    /// acknowledged match (stale rejection storms cannot over-rewind) and
    /// never below the snapshot boundary (below it the follower needs the
    /// snapshot, not the log).
    pub(in crate::node) fn collapse_into_probe(&mut self, snapshot_index: LogIndex) {
        let floor = self.match_index.next().max(snapshot_index);
        self.next_index = LogIndex(self.next_index.0.saturating_sub(1)).max(floor);
        self.mode = ProgressMode::Probe {
            awaiting_response: false,
        };
        self.inflights.clear();
    }

    /// Confirms the follower's position after a successful acknowledgement:
    /// replication resumes from the acknowledged match with a clean window.
    pub(in crate::node) fn confirm_replicating(&mut self) {
        if !matches!(self.mode, ProgressMode::Replicate) {
            self.mode = ProgressMode::Replicate;
            self.inflights.clear();
            self.next_index = self.next_index.max(self.match_index.next());
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::node) enum ProgressMode {
    /// One bounded append at a time until the follower's log position is
    /// confirmed; retries while awaiting are empty heartbeats.
    Probe { awaiting_response: bool },
    /// Confirmed position: sends fill the in-flight window and `next_index`
    /// advances optimistically with each send.
    Replicate,
    /// Streaming the current snapshot chunk by chunk; log replication is
    /// paused until the follower installs it.
    Snapshot { next_offset: u64 },
}

/// The window of optimistically sent, unacknowledged append batches for one
/// follower, bounded by batch count and payload bytes.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(in crate::node) struct Inflights {
    batches: VecDeque<InflightBatch>,
    bytes: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct InflightBatch {
    last_index: LogIndex,
    bytes: usize,
}

impl Inflights {
    /// Whether the window admits another batch. One batch is always
    /// admissible regardless of the byte budget, or a batch larger than the
    /// budget could never be sent at all.
    pub(in crate::node) fn is_full(&self, max_batches: usize, max_bytes: usize) -> bool {
        self.batches.len() >= max_batches.max(1)
            || (!self.batches.is_empty() && self.bytes >= max_bytes)
    }

    /// Records one in-flight append batch.
    pub(in crate::node) fn record(&mut self, last_index: LogIndex, bytes: usize) {
        self.batches.push_back(InflightBatch { last_index, bytes });
        self.bytes = self.bytes.saturating_add(bytes);
    }

    /// Releases every batch fully acknowledged by `match_index`.
    pub(in crate::node) fn free_through(&mut self, match_index: LogIndex) {
        while let Some(batch) = self.batches.front() {
            if batch.last_index > match_index {
                break;
            }
            self.bytes = self.bytes.saturating_sub(batch.bytes);
            self.batches.pop_front();
        }
    }

    /// Clears every recorded in-flight batch.
    pub(in crate::node) fn clear(&mut self) {
        self.batches.clear();
        self.bytes = 0;
    }

    /// Test observability: the window's recorded batch count. Only kernel
    /// tests inspect the window directly, so the accessor is test-gated to
    /// keep the library target free of dead code.
    #[cfg(test)]
    pub(in crate::node) fn batch_count(&self) -> usize {
        self.batches.len()
    }

    /// Test observability: the window's recorded payload bytes (test-gated
    /// like [`Inflights::batch_count`]).
    #[cfg(test)]
    pub(in crate::node) fn byte_count(&self) -> usize {
        self.bytes
    }
}
