//! One coordinator owns the durable file, acknowledged views, and poison state.
use super::{
    codec::{self, Record},
    reclamation::{self, Authority},
    DurableReceipt, PersistenceDomain, RaftPersistenceBatchError as Error,
};
use crate::telemetry::{measure, Stage};
use crate::{file_store_ownership::SharedFileStoreOwnership, PersistedRaftLogEntry, RaftHardState};
use rafter::LogIndex;
use std::{
    fs::File,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(super) type Shared = Arc<Mutex<State>>;
#[derive(Debug)]
pub(super) struct State {
    pub file: File,
    pub path: PathBuf,
    pub authority: Authority,
    pub snapshot_directory: PathBuf,
    pub domain: PersistenceDomain,
    pub hard: RaftHardState,
    pub entries: Vec<PersistedRaftLogEntry>,
    pub compacted: LogIndex,
    pub operation: u64,
    pub poisoned: bool,
    pub syncs: u64,
    pub _ownership: SharedFileStoreOwnership,
    #[cfg(test)]
    pub fail_after_write: bool,
}
impl State {
    pub(super) fn next_index(&self) -> LogIndex {
        self.entries
            .last()
            .map_or(self.compacted, |e| e.index)
            .next()
    }
    pub(super) fn receipt(&self) -> DurableReceipt {
        DurableReceipt {
            domain: self.domain.clone(),
            operation: self.operation,
            hard_state: self.hard,
            next_index: self.next_index(),
            compacted_through: self.compacted,
        }
    }
    pub(super) fn validate(&self, record: &Record) -> Result<(), Error> {
        if record.compact.is_some() && (record.truncate.is_some() || !record.entries.is_empty()) {
            return Err(Error::InvalidBatch(
                "compaction must be a separate operation",
            ));
        }
        let mut next = self.next_index();
        if let Some(from) = record.truncate {
            if from <= self.compacted || from > next {
                return Err(Error::InvalidBatch("truncate outside retained suffix"));
            }
            if from <= self.hard.commit_index {
                return Err(Error::InvalidBatch(
                    "cannot truncate durable committed entries",
                ));
            }
            next = from;
        }
        for entry in &record.entries {
            if entry.index != next || entry.index.0 == u64::MAX {
                return Err(Error::InvalidBatch(
                    "append must be contiguous and leave a successor",
                ));
            }
            next = next.next();
        }
        if let Some(through) = record.compact {
            if through.0 == u64::MAX {
                return Err(Error::InvalidBatch("compaction leaves no successor"));
            }
            next = next.max(through.next());
        }
        if let Some(hard) = record.hard {
            if hard.current_term < self.hard.current_term
                || hard.commit_index < self.hard.commit_index
                || (hard.current_term == self.hard.current_term
                    && self.hard.voted_for.is_some()
                    && hard.voted_for != self.hard.voted_for)
            {
                return Err(Error::InvalidBatch("hard-state promises cannot regress"));
            }
            if hard.commit_index >= next {
                return Err(Error::InvalidBatch(
                    "commit exceeds durable log or compacted prefix",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn apply(&mut self, record: Record) {
        if let Some(from) = record.truncate {
            let count = self.entries.partition_point(|entry| entry.index < from);
            self.entries.truncate(count);
        }
        if let Some(through) = record.compact {
            let count = self.entries.partition_point(|entry| entry.index <= through);
            self.entries.drain(..count);
            self.compacted = self.compacted.max(through);
        }
        self.entries.extend(record.entries);
        if let Some(hard) = record.hard {
            self.hard = hard;
        }
        self.operation = record.operation;
    }
    pub(super) fn publish(&mut self, mut record: Record) -> Result<DurableReceipt, Error> {
        if self.poisoned {
            return Err(Error::StoreRequiresReopen);
        }
        self.validate(&record)?;
        if record.entries.is_empty()
            && record.truncate.is_none()
            && record.compact.is_none()
            && record.hard.is_none_or(|hard| hard == self.hard)
        {
            return Ok(self.receipt());
        }
        record.operation = self
            .operation
            .checked_add(1)
            .ok_or(Error::InvalidBatch("publication number exhausted"))?;
        let bytes =
            measure(Stage::BatchEncode, || codec::encode(&record)).map_err(|source| Error::Io {
                operation: "encode Raft WAL batch",
                source: source.into(),
            })?;
        self.poisoned = true;
        measure(Stage::BatchWrite, || self.file.write_all(&bytes)).map_err(|source| Error::Io {
            operation: "append Raft WAL batch",
            source: source.into(),
        })?;
        #[cfg(test)]
        if self.fail_after_write {
            return Err(Error::Io {
                operation: "injected before WAL sync",
                source: std::io::Error::other("injected failure").into(),
            });
        }
        measure(Stage::BatchSync, || self.file.sync_data()).map_err(|source| Error::Io {
            operation: "sync Raft WAL batch",
            source: source.into(),
        })?;
        self.syncs += 1;
        self.apply(record);
        self.poisoned = false;
        Ok(self.receipt())
    }

    pub(super) fn reclaim(&mut self) -> Result<(), reclamation::Failure> {
        self.poisoned = true;
        let snapshot = reclamation::snapshot_reference(&self.snapshot_directory, self.compacted)
            .map_err(|source| reclamation::Failure {
                operation: "bind WAL checkpoint to current snapshot",
                source,
            })?;
        let prepared = reclamation::prepare(self, snapshot)?;
        reclamation::publish_manifest(&self.path, &prepared.manifest)?;
        // Checkpoint, fresh segment header, and manifest temp are three
        // additional successful data-sync calls beyond the compaction record.
        self.syncs += 3;

        let old_file = std::mem::replace(&mut self.file, prepared.segment);
        drop(old_file);
        self.path = prepared.segment_path;
        self.authority = Authority::Generation(prepared.manifest.generation);

        let directory = self.path.parent().ok_or_else(|| reclamation::Failure {
            operation: "resolve WAL cleanup directory",
            source: codec::invalid("opened WAL path has no parent"),
        })?;
        reclamation::cleanup(directory, &self.authority, true)?;
        self.poisoned = false;
        Ok(())
    }
}
