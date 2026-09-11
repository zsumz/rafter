//! Split legacy trait handles retain one shared atomic WAL coordinator.
use super::{
    codec::Record, state::Shared, DurableReceipt, PersistenceDomain, RaftPersistenceBatch,
    RaftPersistenceBatchError as Error,
};
use crate::{
    BorrowedPersistedRaftLogEntry, PersistedRaftLogEntry, RaftHardState, RaftHardStateStore,
    RaftHardStateStoreWriteError, RaftLogSegment, RaftLogSegmentAppendError,
    RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};
use rafter::LogIndex;
use std::io;

/// Hard-state view of one shared WAL. Successful standalone writes still sync.
#[derive(Debug)]
pub struct WalRaftHardStateStore(pub(super) Shared);
/// Retained-log view of one shared WAL. Every successful mutation is durable.
#[derive(Debug)]
pub struct WalRaftLogSegment(pub(super) Shared);

impl RaftHardStateStore for WalRaftHardStateStore {
    fn persistence_domain(&self) -> Option<PersistenceDomain> {
        Some(
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .domain
                .clone(),
        )
    }
    fn current(&self) -> RaftHardState {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .hard
    }
    fn write_hard_state(
        &mut self,
        hard: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| RaftHardStateStoreWriteError::StoreRequiresReopen)?;
        let path = state.path.clone();
        state
            .publish(Record {
                operation: 0,
                compact: None,
                truncate: None,
                hard: Some(hard),
                entries: Vec::new(),
            })
            .map(|_| ())
            .map_err(|error| match error {
                Error::StoreRequiresReopen => RaftHardStateStoreWriteError::StoreRequiresReopen,
                other => RaftHardStateStoreWriteError::Io {
                    operation: "publish hard state in shared WAL",
                    path,
                    source: io::Error::other(other).into(),
                },
            })
    }
}
impl WalRaftLogSegment {
    /// Number of successful data syncs since this coordinator was opened.
    #[must_use]
    pub fn sync_count(&self) -> u64 {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .syncs
    }
}
impl RaftLogSegment for WalRaftLogSegment {
    fn persist_batch(
        &mut self,
        domain: &PersistenceDomain,
        batch: RaftPersistenceBatch<'_>,
    ) -> Result<Option<DurableReceipt>, Error> {
        let mut state = self.0.lock().map_err(|_| Error::StoreRequiresReopen)?;
        if &state.domain != domain {
            return Err(Error::DomainMismatch);
        }
        let entries = batch
            .entries
            .iter()
            .map(|entry| PersistedRaftLogEntry::from(*entry))
            .collect();
        state
            .publish(Record {
                operation: 0,
                compact: None,
                truncate: batch.truncate_from,
                hard: batch.hard_state,
                entries,
            })
            .map(Some)
    }
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| RaftLogSegmentAppendError::StoreRequiresReopen)?;
        let mut expected = state.next_index();
        for entry in entries {
            if entry.index.0 == u64::MAX {
                return Err(RaftLogSegmentAppendError::IndexAtMaximum);
            }
            if entry.index != expected {
                return Err(RaftLogSegmentAppendError::NonContiguous {
                    expected,
                    actual: entry.index,
                });
            }
            expected = expected.next();
        }
        state
            .publish(Record {
                operation: 0,
                compact: None,
                truncate: None,
                hard: None,
                entries: entries.to_vec(),
            })
            .map(|_| ())
            .map_err(|error| match error {
                Error::StoreRequiresReopen => RaftLogSegmentAppendError::StoreRequiresReopen,
                other => RaftLogSegmentAppendError::Io {
                    operation: "append shared WAL log",
                    source: io::Error::other(other).into(),
                },
            })
    }
    fn append_entries_borrowed<'a, I>(
        &mut self,
        entries: I,
    ) -> Result<(), RaftLogSegmentAppendError>
    where
        I: IntoIterator<Item = BorrowedPersistedRaftLogEntry<'a>>,
    {
        self.append_entries(
            &entries
                .into_iter()
                .map(PersistedRaftLogEntry::from)
                .collect::<Vec<_>>(),
        )
    }
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| RaftLogSegmentTruncateError::StoreRequiresReopen)?;
        if state.poisoned {
            return Err(RaftLogSegmentTruncateError::StoreRequiresReopen);
        }
        if from <= state.compacted {
            return Err(RaftLogSegmentTruncateError::BeforeCompactedPrefix {
                compacted_through: state.compacted,
                actual: from,
            });
        }
        if from > state.next_index() {
            return Err(RaftLogSegmentTruncateError::OutOfBounds {
                next_index: state.next_index(),
                actual: from,
            });
        }
        if from == state.next_index() {
            return Ok(());
        }
        state
            .publish(Record {
                operation: 0,
                compact: None,
                truncate: Some(from),
                hard: None,
                entries: Vec::new(),
            })
            .map(|_| ())
            .map_err(|error| RaftLogSegmentTruncateError::Io {
                operation: "truncate shared WAL log",
                source: io::Error::other(error).into(),
            })
    }
    fn compact_prefix_through(
        &mut self,
        through: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| RaftLogSegmentCompactError::StoreRequiresReopen)?;
        if state.poisoned {
            return Err(RaftLogSegmentCompactError::StoreRequiresReopen);
        }
        if through.0 == u64::MAX {
            return Err(RaftLogSegmentCompactError::ThroughIndexAtMaximum);
        }
        if through <= state.compacted {
            return Ok(());
        }
        state
            .publish(Record {
                operation: 0,
                compact: Some(through),
                truncate: None,
                hard: None,
                entries: Vec::new(),
            })
            .map_err(|error| RaftLogSegmentCompactError::Io {
                operation: "compact shared WAL log",
                source: io::Error::other(error).into(),
            })?;
        let compacted = state.compacted;
        state.reclaim().map_err(|failure| {
            RaftLogSegmentCompactError::CompactedButReclamationFailed {
                compacted_through: compacted,
                operation: failure.operation,
                source: failure.source.into(),
            }
        })
    }
    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .clone()
    }
    fn next_index(&self) -> LogIndex {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .next_index()
    }
    fn compacted_through(&self) -> LogIndex {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .compacted
    }
}
