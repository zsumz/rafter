//! One coordinator owns the durable file, acknowledged views, and poison state.
use super::{
    codec::{self, Record},
    reclamation::{self, Authority},
    retirement::EntryDropper,
    DurableReceipt, PersistenceDomain, RaftPersistenceBatchError as Error,
};
use crate::telemetry::{measure, Stage, Timer};
use crate::{file_store_ownership::SharedFileStoreOwnership, PersistedRaftLogEntry, RaftHardState};
use rafter::LogIndex;
#[cfg(test)]
use std::sync::Barrier;
use std::{
    fs::File,
    io::{self, Write},
    num::NonZeroU64,
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[cfg(test)]
#[derive(Debug)]
pub(super) struct ReclamationGate {
    entered: Barrier,
    release: Barrier,
}
#[cfg(test)]
impl ReclamationGate {
    pub(super) fn new() -> Self {
        Self {
            entered: Barrier::new(2),
            release: Barrier::new(2),
        }
    }
    pub(super) fn wait_until_entered(&self) {
        self.entered.wait();
    }
    pub(super) fn release(&self) {
        self.release.wait();
    }
    fn hold(&self) {
        self.entered.wait();
        self.release.wait();
    }
}

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
    pub retired_entries: EntryDropper<PersistedRaftLogEntry>,
    pub compacted: LogIndex,
    pub operation: u64,
    pub poisoned: bool,
    pub syncs: u64,
    pub batch_encode_buffer: Vec<u8>,
    pub reclamation_threshold_bytes: Option<NonZeroU64>,
    pub _ownership: SharedFileStoreOwnership,
    #[cfg(test)]
    pub fail_after_write: bool,
    #[cfg(test)]
    pub reclaim_gate: Option<Arc<ReclamationGate>>,
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
    pub(super) fn reclamation_due(&self) -> io::Result<bool> {
        match self.reclamation_threshold_bytes {
            None => Ok(true),
            Some(threshold) => Ok(self.file.metadata()?.len() >= threshold.get()),
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
        self.apply_record(record, true);
    }
    pub(super) fn apply_replayed(&mut self, record: Record) {
        self.apply_record(record, false);
    }
    fn apply_record(&mut self, record: Record, defer_compacted_drop: bool) {
        if let Some(from) = record.truncate {
            let count = self.entries.partition_point(|entry| entry.index < from);
            self.entries.truncate(count);
        }
        if let Some(through) = record.compact {
            let count = self.entries.partition_point(|entry| entry.index <= through);
            if defer_compacted_drop && count != 0 {
                let retained_suffix = self.entries.split_off(count);
                let retired_prefix = std::mem::replace(&mut self.entries, retained_suffix);
                self.retired_entries.retire(retired_prefix);
            } else {
                self.entries.drain(..count);
            }
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
        let encode_result = measure(Stage::BatchEncode, || {
            codec::encode_reusing(&record, &mut self.batch_encode_buffer)
        });
        if let Err(source) = encode_result {
            codec::clear_encode_buffer(&mut self.batch_encode_buffer);
            return Err(Error::Io {
                operation: "encode Raft WAL batch",
                source: source.into(),
            });
        }
        self.poisoned = true;
        let write_result = measure(Stage::BatchWrite, || {
            self.file.write_all(&self.batch_encode_buffer)
        });
        codec::clear_encode_buffer(&mut self.batch_encode_buffer);
        write_result.map_err(|source| Error::Io {
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
        let _reclamation = Timer::start(Stage::WalReclamation);
        self.poisoned = true;
        #[cfg(test)]
        if let Some(gate) = self.reclaim_gate.take() {
            gate.hold();
        }
        let snapshot = reclamation::snapshot_reference(&self.snapshot_directory, self.compacted)
            .map_err(|source| reclamation::Failure {
                operation: "bind WAL checkpoint to current snapshot",
                source,
            })?;
        let prepared = measure(Stage::WalCheckpointPrepare, || {
            reclamation::prepare(self, snapshot)
        })?;
        measure(Stage::WalManifestPublish, || {
            reclamation::publish_manifest(&self.path, &prepared.manifest)
        })?;
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
        measure(Stage::WalCleanup, || {
            reclamation::cleanup(directory, &self.authority, true)
        })?;
        self.poisoned = false;
        Ok(())
    }
}
