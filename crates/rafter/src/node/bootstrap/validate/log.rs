//! Ordered validation of the retained log and committed configuration identity.

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
    first_uncommitted_configuration: Option<LogIndex>,
    latest_committed_configuration: Option<CommittedConfiguration>,
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
            first_uncommitted_configuration: None,
            latest_committed_configuration: None,
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
                return validate_boundary_entry(&entry, snapshot);
            }
        }

        self.validate_retained_entry(&entry)?;
        self.record_configuration(&entry)?;

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

    fn record_configuration(
        &mut self,
        entry: &BootstrapLogEntry,
    ) -> Result<(), BootstrapValidationError> {
        let Some(configuration) = entry.kind.configuration_entry() else {
            return Ok(());
        };

        if entry.index <= self.commit_index {
            self.latest_committed_configuration = Some(CommittedConfiguration {
                index: entry.index,
                config_id: configuration.config_id(),
            });
            return Ok(());
        }

        if let Some(first_index) = self.first_uncommitted_configuration {
            return Err(
                BootstrapValidationError::MultipleUncommittedConfigurationEntries {
                    first_index,
                    second_index: entry.index,
                },
            );
        }
        self.first_uncommitted_configuration = Some(entry.index);
        Ok(())
    }

    fn finish(self) -> Result<Vec<LogEntry>, BootstrapValidationError> {
        if self.commit_index > self.last_log_index {
            return Err(BootstrapValidationError::CommitIndexBeyondLog {
                commit_index: self.commit_index,
                last_log_index: self.last_log_index,
            });
        }

        validate_committed_configuration(
            self.stored_committed_configuration,
            self.latest_committed_configuration,
            self.snapshot,
            self.commit_index,
        )?;
        Ok(self.materialized_log)
    }
}

fn validate_committed_configuration(
    stored: Option<CommittedConfiguration>,
    latest_retained: Option<CommittedConfiguration>,
    snapshot: Option<&RaftSnapshotMetadata>,
    commit_index: LogIndex,
) -> Result<(), BootstrapValidationError> {
    let Some(stored) = stored else {
        return Ok(());
    };
    if stored.index > commit_index {
        return Err(
            BootstrapValidationError::CommittedConfigurationAheadOfCommit {
                committed_configuration_index: stored.index,
                commit_index,
            },
        );
    }

    if let Some(latest_retained) = latest_retained {
        return validate_retained_committed_configuration(stored, latest_retained);
    }

    let snapshot_index = snapshot_index(snapshot);
    if stored.index > snapshot_index {
        return Err(BootstrapValidationError::CommittedConfigurationMissing {
            committed_configuration_index: stored.index,
        });
    }
    if snapshot
        .and_then(RaftSnapshotMetadata::committed_membership)
        .is_some()
    {
        return Ok(());
    }
    Err(
        BootstrapValidationError::CompactedCommittedConfigurationWithoutSnapshotMembership {
            committed_configuration_index: stored.index,
        },
    )
}

fn validate_retained_committed_configuration(
    stored: CommittedConfiguration,
    latest_retained: CommittedConfiguration,
) -> Result<(), BootstrapValidationError> {
    if stored.index < latest_retained.index {
        return Err(BootstrapValidationError::CommittedConfigurationNotLatest {
            recorded_index: stored.index,
            latest_index: latest_retained.index,
        });
    }
    if stored.index > latest_retained.index {
        return Err(BootstrapValidationError::CommittedConfigurationMissing {
            committed_configuration_index: stored.index,
        });
    }
    if stored.config_id != latest_retained.config_id {
        return Err(BootstrapValidationError::CommittedConfigurationIdMismatch {
            index: stored.index,
            expected: stored.config_id,
            actual: latest_retained.config_id,
        });
    }
    Ok(())
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
