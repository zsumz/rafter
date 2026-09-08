//! Ordered validation and materialization of the retained log.
//!
//! Accepted catch-up entries can precede durable commit publication. Their
//! count does not establish which configurations are committed: retain them
//! all, leaving commit knowledge and proposal serialization to the node.

use crate::{CommittedConfiguration, LogEntry, LogIndex, RaftSnapshotMetadata, Term};

use super::super::error::BootstrapValidationError;
use super::super::state::BootstrapLogEntry;

pub(super) fn validate_log(
    entries: Vec<BootstrapLogEntry>,
    snapshot: Option<&RaftSnapshotMetadata>,
    current_term: Term,
    stored_commit_index: LogIndex,
    stored_committed_configuration: Option<CommittedConfiguration>,
) -> Result<Vec<LogEntry>, BootstrapValidationError> {
    let mut validation = LogValidation::new(
        snapshot,
        current_term,
        stored_commit_index,
        stored_committed_configuration,
    );
    for entry in entries {
        validation.accept(entry)?;
    }
    validation.finish()
}

struct LogValidation<'a> {
    snapshot: Option<&'a RaftSnapshotMetadata>,
    current_term: Term,
    commit_index: LogIndex,
    stored_committed_configuration: Option<CommittedConfiguration>,

    expected_index: LogIndex,
    last_log_index: LogIndex,
    materialized_log: Vec<LogEntry>,
}

impl<'a> LogValidation<'a> {
    fn new(
        snapshot: Option<&'a RaftSnapshotMetadata>,
        current_term: Term,
        stored_commit_index: LogIndex,
        stored_committed_configuration: Option<CommittedConfiguration>,
    ) -> Self {
        let snapshot_index = snapshot_index(snapshot);
        Self {
            snapshot,
            current_term,
            commit_index: stored_commit_index.max(snapshot_index),
            stored_committed_configuration,
            expected_index: snapshot_index.next(),
            last_log_index: snapshot_index,
            materialized_log: Vec::new(),
        }
    }

    fn accept(&mut self, entry: BootstrapLogEntry) -> Result<(), BootstrapValidationError> {
        let snapshot_index = snapshot_index(self.snapshot);
        if entry.index < snapshot_index {
            return Err(BootstrapValidationError::CompactedLogEntry {
                snapshot_index,
                entry_index: entry.index,
            });
        }
        if entry.index == snapshot_index {
            if let Some(snapshot) = self.snapshot {
                validate_boundary_entry(&entry, snapshot)?;
                return super::configuration::validate_boundary_configuration(
                    &entry,
                    snapshot,
                    self.stored_committed_configuration,
                );
            }
        }

        self.validate_retained_entry(&entry)?;

        self.expected_index = entry.index.next();
        self.last_log_index = entry.index;
        self.materialized_log.push(LogEntry {
            term: entry.term,
            kind: entry.kind,
        });
        Ok(())
    }

    fn validate_retained_entry(
        &self,
        entry: &BootstrapLogEntry,
    ) -> Result<(), BootstrapValidationError> {
        if entry.index != self.expected_index {
            return Err(BootstrapValidationError::NonContiguousLog {
                expected: self.expected_index,
                actual: entry.index,
            });
        }
        if entry.term.is_zero() {
            return Err(BootstrapValidationError::ZeroTermLogEntry { index: entry.index });
        }
        if entry.term > self.current_term {
            return Err(BootstrapValidationError::EntryTermAheadOfCurrentTerm {
                index: entry.index,
                entry_term: entry.term,
                current_term: self.current_term,
            });
        }
        entry
            .index
            .0
            .checked_add(1)
            .ok_or(BootstrapValidationError::LogIndexAtMaximum { index: entry.index })?;
        Ok(())
    }

    fn finish(self) -> Result<Vec<LogEntry>, BootstrapValidationError> {
        if self.commit_index > self.last_log_index {
            return Err(BootstrapValidationError::CommitIndexBeyondLog {
                commit_index: self.commit_index,
                last_log_index: self.last_log_index,
            });
        }

        Ok(self.materialized_log)
    }
}

fn validate_boundary_entry(
    entry: &BootstrapLogEntry,
    snapshot: &RaftSnapshotMetadata,
) -> Result<(), BootstrapValidationError> {
    if entry.term != snapshot.last_included_term {
        return Err(BootstrapValidationError::SnapshotBoundaryTermMismatch {
            index: entry.index,
            snapshot_term: snapshot.last_included_term,
            entry_term: entry.term,
        });
    }
    Ok(())
}

fn snapshot_index(snapshot: Option<&RaftSnapshotMetadata>) -> LogIndex {
    snapshot.map_or(LogIndex::ZERO, |snapshot| snapshot.last_included_index)
}
