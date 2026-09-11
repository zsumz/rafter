//! Stop after a successful durable mutation, before any subsequent write.

use super::*;
use std::{cell::RefCell, io, rc::Rc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Mutation {
    HardState,
    Stage,
    Truncate,
    Promote,
    Compact,
    Append,
    Clear,
}

#[derive(Clone, Debug)]
pub(super) struct Cut {
    after: Option<usize>,
    mutations: Rc<RefCell<Vec<Mutation>>>,
}

impl Cut {
    pub(super) fn new(after: Option<usize>) -> Self {
        Self {
            after,
            mutations: Rc::default(),
        }
    }

    pub(super) fn mutations(&self) -> Vec<Mutation> {
        self.mutations.borrow().clone()
    }

    fn record(&self, mutation: Mutation) -> io::Result<()> {
        let mut mutations = self.mutations.borrow_mut();
        mutations.push(mutation);
        if self.after == Some(mutations.len()) {
            Err(io::Error::other("injected crash after durable mutation"))
        } else {
            Ok(())
        }
    }

    pub(super) fn wrap<T>(&self, inner: T) -> Observed<T> {
        Observed {
            inner,
            cut: self.clone(),
        }
    }
}

#[derive(Debug)]
pub(super) struct Observed<T> {
    pub inner: T,
    cut: Cut,
}

impl<H: RaftHardStateStore> RaftHardStateStore for Observed<H> {
    fn current(&self) -> RaftHardState {
        self.inner.current()
    }

    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        self.inner.write_hard_state(state)?;
        self.cut
            .record(Mutation::HardState)
            .map_err(|source| RaftHardStateStoreWriteError::Io {
                operation: "durable crash cut",
                path: PathBuf::from("hard-state"),
                source: source.into(),
            })
    }
}

impl<L: RaftLogSegment> RaftLogSegment for Observed<L> {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        self.inner.append_entries(entries)?;
        self.cut
            .record(Mutation::Append)
            .map_err(|source| RaftLogSegmentAppendError::Io {
                operation: "durable crash cut",
                source: source.into(),
            })
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        self.inner.truncate_suffix(from)?;
        self.cut
            .record(Mutation::Truncate)
            .map_err(|source| RaftLogSegmentTruncateError::Io {
                operation: "durable crash cut",
                source: source.into(),
            })
    }

    fn compact_prefix_through(
        &mut self,
        through: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        self.inner.compact_prefix_through(through)?;
        self.cut
            .record(Mutation::Compact)
            .map_err(|source| RaftLogSegmentCompactError::Io {
                operation: "durable crash cut",
                source: source.into(),
            })
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.inner.replay_entries()
    }

    fn next_index(&self) -> LogIndex {
        self.inner.next_index()
    }

    fn compacted_through(&self) -> LogIndex {
        self.inner.compacted_through()
    }
}

impl<S: RaftSnapshotStore> RaftSnapshotStore for Observed<S> {
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.inner.write_snapshot(snapshot)
    }

    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.inner.write_snapshot_from_source(snapshot, source)
    }

    fn current_snapshot(&self) -> Option<RaftSnapshot> {
        self.inner.current_snapshot()
    }

    fn stage_snapshot_chunk(
        &mut self,
        chunk: &rafter::StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.inner.stage_snapshot_chunk(chunk)?;
        self.cut.record(Mutation::Stage).map_err(snapshot_error)
    }

    fn promote_staged_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.inner.promote_staged_snapshot(snapshot)?;
        self.cut.record(Mutation::Promote).map_err(snapshot_error)
    }

    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError> {
        self.inner.clear_pending_snapshot_transfer()?;
        self.cut.record(Mutation::Clear).map_err(snapshot_error)
    }

    fn current_pending_snapshot_transfer(&self) -> Option<rafter::PendingSnapshotTransfer> {
        self.inner.current_pending_snapshot_transfer()
    }
}

impl<S: SnapshotChunkSource> SnapshotChunkSource for Observed<S> {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        self.inner.snapshot_chunk(request)
    }
}

fn snapshot_error(source: io::Error) -> RaftSnapshotStoreWriteError {
    RaftSnapshotStoreWriteError::Io {
        operation: "durable crash cut",
        path: PathBuf::from("snapshots"),
        source: source.into(),
    }
}
