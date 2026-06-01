use std::{error::Error, fmt};

use crate::{
    CommittedConfiguration, ConfigurationEntry, LogEntry, LogEntryKind, LogIndex, NodeId,
    RaftSnapshot, RaftSnapshotMetadata, Term,
};

use super::NodeConfig;

/// Durable state used to hydrate a node after restart.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct BootstrapState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    /// Durable commit index recorded with the hard state. Recovery treats the
    /// snapshot boundary as a floor, so a crash after snapshot promotion but
    /// before the final hard-state write still boots at the compacted prefix.
    pub commit_index: LogIndex,
    /// Identity of the latest committed configuration entry when it is known
    /// to the durable runtime. Bootstrap verifies it against any retained log
    /// entry it still covers; compacted entries are represented by snapshot
    /// committed-configuration metadata.
    pub committed_configuration: Option<CommittedConfiguration>,
    /// The persisted snapshot descriptor: metadata plus payload length. The
    /// payload itself stays in the application's snapshot store; the kernel
    /// only needs its length to derive the transfer identity and serve
    /// chunk directives.
    pub snapshot: Option<RaftSnapshot>,
    pub log: Vec<BootstrapLogEntry>,
}

/// One durable log entry with its persisted index.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BootstrapLogEntry {
    pub index: LogIndex,
    pub term: Term,
    pub kind: LogEntryKind,
}

impl BootstrapLogEntry {
    /// Builds a persisted application entry.
    #[must_use]
    pub fn application(index: LogIndex, term: Term, payload: Vec<u8>) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::application(payload),
        }
    }

    /// Builds a persisted configuration entry.
    #[must_use]
    pub fn configuration(index: LogIndex, term: Term, configuration: ConfigurationEntry) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::configuration(configuration),
        }
    }

    /// Builds a persisted no-op entry.
    #[must_use]
    pub const fn noop(index: LogIndex, term: Term) -> Self {
        Self {
            index,
            term,
            kind: LogEntryKind::noop(),
        }
    }
}

pub(super) struct BootstrapParts {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub commit_index: LogIndex,
    pub committed_configuration: Option<CommittedConfiguration>,
    pub snapshot: Option<RaftSnapshot>,
    pub log: Vec<LogEntry>,
}

/// Validation error returned when a bootstrap image cannot form a legal node.
///
/// This enum is exhaustive because bootstrap validation is closed over these
/// persisted-state invariants.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootstrapValidationError {
    VoteForNonVoter {
        voted_for: NodeId,
    },
    VoteInZeroTerm {
        voted_for: NodeId,
    },
    /// The declared applied floor lies beyond the persisted log.
    AppliedFloorBeyondLog {
        applied_through: LogIndex,
        last_log_index: LogIndex,
    },
    /// The declared applied floor lies beyond the recovered committed prefix.
    AppliedFloorBeyondCommit {
        applied_through: LogIndex,
        commit_index: LogIndex,
    },
    NonContiguousLog {
        expected: LogIndex,
        actual: LogIndex,
    },
    ZeroTermLogEntry {
        index: LogIndex,
    },
    EntryTermAheadOfCurrentTerm {
        index: LogIndex,
        entry_term: Term,
        current_term: Term,
    },
    SnapshotWriterNonVoter {
        writer_id: NodeId,
    },
    SnapshotHardStateTermAheadOfCurrentTerm {
        snapshot_hard_state_term: Term,
        current_term: Term,
    },
    CompactedLogEntry {
        snapshot_index: LogIndex,
        entry_index: LogIndex,
    },
    SnapshotBoundaryTermMismatch {
        index: LogIndex,
        snapshot_term: Term,
        entry_term: Term,
    },
    MultipleUncommittedConfigurationEntries {
        first_index: LogIndex,
        second_index: LogIndex,
    },
    CommitIndexBeyondLog {
        commit_index: LogIndex,
        last_log_index: LogIndex,
    },
    CommittedConfigurationAheadOfCommit {
        committed_configuration_index: LogIndex,
        commit_index: LogIndex,
    },
    CommittedConfigurationMissing {
        committed_configuration_index: LogIndex,
    },
    CommittedConfigurationIdMismatch {
        index: LogIndex,
        expected: crate::ConfigurationId,
        actual: crate::ConfigurationId,
    },
    CommittedConfigurationNotLatest {
        recorded_index: LogIndex,
        latest_index: LogIndex,
    },
    CompactedCommittedConfigurationWithoutSnapshotMembership {
        committed_configuration_index: LogIndex,
    },
    /// The log reaches the maximum representable index: its successor
    /// cannot exist and index arithmetic on it overflows.
    LogIndexAtMaximum {
        index: LogIndex,
    },
}

impl fmt::Display for BootstrapValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VoteForNonVoter { voted_for } => write!(
                formatter,
                "Raft bootstrap records a vote for {voted_for} which is not a configured voter"
            ),
            Self::VoteInZeroTerm { voted_for } => write!(
                formatter,
                "Raft bootstrap records a vote for {voted_for} in term zero"
            ),
            Self::AppliedFloorBeyondLog {
                applied_through,
                last_log_index,
            } => write!(
                formatter,
                "declared applied floor {applied_through} lies beyond the persisted log end {last_log_index}"
            ),
            Self::AppliedFloorBeyondCommit {
                applied_through,
                commit_index,
            } => write!(
                formatter,
                "declared applied floor {applied_through} lies beyond the recovered commit index {commit_index}"
            ),
            Self::NonContiguousLog { expected, actual } => write!(
                formatter,
                "Raft bootstrap log entry at index {actual} is not contiguous with expected index {expected}"
            ),
            Self::ZeroTermLogEntry { index } => write!(
                formatter,
                "Raft bootstrap log entry at index {index} has term zero"
            ),
            Self::EntryTermAheadOfCurrentTerm {
                index,
                entry_term,
                current_term,
            } => write!(
                formatter,
                "Raft bootstrap log entry at index {index} has term {entry_term} ahead of current term {current_term}"
            ),
            Self::SnapshotWriterNonVoter { writer_id } => write!(
                formatter,
                "Raft bootstrap snapshot writer {writer_id} is not a voter"
            ),
            Self::SnapshotHardStateTermAheadOfCurrentTerm {
                snapshot_hard_state_term,
                current_term,
            } => write!(
                formatter,
                "Raft bootstrap snapshot hard-state term {snapshot_hard_state_term} is ahead of current term {current_term}"
            ),
            Self::CompactedLogEntry {
                snapshot_index,
                entry_index,
            } => write!(
                formatter,
                "Raft bootstrap log entry at index {entry_index} is already compacted by snapshot index {snapshot_index}"
            ),
            Self::SnapshotBoundaryTermMismatch {
                index,
                snapshot_term,
                entry_term,
            } => write!(
                formatter,
                "Raft bootstrap boundary entry at index {index} has term {entry_term} but the snapshot recorded term {snapshot_term}"
            ),
            Self::LogIndexAtMaximum { index } => write!(
                formatter,
                "Raft bootstrap log entry at index {index} is at the maximum representable index"
            ),
            Self::MultipleUncommittedConfigurationEntries {
                first_index,
                second_index,
            } => write!(
                formatter,
                "Raft bootstrap log holds uncommitted configuration entries at indexes {first_index} and {second_index}"
            ),
            Self::CommitIndexBeyondLog { .. }
            | Self::CommittedConfigurationAheadOfCommit { .. }
            | Self::CommittedConfigurationMissing { .. }
            | Self::CommittedConfigurationIdMismatch { .. }
            | Self::CommittedConfigurationNotLatest { .. }
            | Self::CompactedCommittedConfigurationWithoutSnapshotMembership { .. } => {
                self.fmt_committed_state_error(formatter)
            }
        }
    }
}

impl Error for BootstrapValidationError {}

impl BootstrapValidationError {
    fn fmt_committed_state_error(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommitIndexBeyondLog {
                commit_index,
                last_log_index,
            } => write!(
                formatter,
                "Raft bootstrap commit index {commit_index} lies beyond the persisted log end {last_log_index}"
            ),
            Self::CommittedConfigurationAheadOfCommit {
                committed_configuration_index,
                commit_index,
            } => write!(
                formatter,
                "Raft bootstrap committed configuration index {committed_configuration_index} lies beyond the commit index {commit_index}"
            ),
            Self::CommittedConfigurationMissing {
                committed_configuration_index,
            } => write!(
                formatter,
                "Raft bootstrap committed configuration index {committed_configuration_index} does not point at a retained configuration entry"
            ),
            Self::CommittedConfigurationIdMismatch {
                index,
                expected,
                actual,
            } => write!(
                formatter,
                "Raft bootstrap committed configuration at index {index} has id {actual} but hard state recorded {expected}"
            ),
            Self::CommittedConfigurationNotLatest {
                recorded_index,
                latest_index,
            } => write!(
                formatter,
                "Raft bootstrap committed configuration index {recorded_index} is older than latest committed configuration index {latest_index}"
            ),
            Self::CompactedCommittedConfigurationWithoutSnapshotMembership {
                committed_configuration_index,
            } => write!(
                formatter,
                "Raft bootstrap committed configuration index {committed_configuration_index} is compacted but the snapshot records no committed membership"
            ),
            Self::VoteForNonVoter { .. }
            | Self::VoteInZeroTerm { .. }
            | Self::AppliedFloorBeyondLog { .. }
            | Self::AppliedFloorBeyondCommit { .. }
            | Self::NonContiguousLog { .. }
            | Self::ZeroTermLogEntry { .. }
            | Self::EntryTermAheadOfCurrentTerm { .. }
            | Self::SnapshotWriterNonVoter { .. }
            | Self::SnapshotHardStateTermAheadOfCurrentTerm { .. }
            | Self::CompactedLogEntry { .. }
            | Self::SnapshotBoundaryTermMismatch { .. }
            | Self::MultipleUncommittedConfigurationEntries { .. }
            | Self::LogIndexAtMaximum { .. } => unreachable!("caller filters committed-state errors"),
        }
    }
}

impl BootstrapState {
    pub(super) fn into_parts(
        self,
        config: &NodeConfig,
    ) -> Result<BootstrapParts, BootstrapValidationError> {
        self.validate_vote()?;
        self.validate_snapshot(config)?;
        let log = validate_log(
            self.log,
            self.snapshot.as_ref().map(|snapshot| &snapshot.metadata),
            self.current_term,
            self.commit_index,
            self.committed_configuration,
        )?;
        let snapshot_index = self.snapshot.as_ref().map_or(LogIndex::ZERO, |snapshot| {
            snapshot.metadata.last_included_index
        });
        Ok(BootstrapParts {
            current_term: self.current_term,
            voted_for: self.voted_for,
            commit_index: self.commit_index.max(snapshot_index),
            committed_configuration: self.committed_configuration,
            snapshot: self.snapshot,
            log,
        })
    }

    fn validate_vote(&self) -> Result<(), BootstrapValidationError> {
        let Some(voted_for) = self.voted_for else {
            return Ok(());
        };
        if self.current_term.is_zero() {
            return Err(BootstrapValidationError::VoteInZeroTerm { voted_for });
        }
        Ok(())
    }

    fn validate_snapshot(&self, config: &NodeConfig) -> Result<(), BootstrapValidationError> {
        let Some(snapshot) = self.snapshot.as_ref() else {
            return Ok(());
        };
        let metadata = &snapshot.metadata;

        if metadata.hard_state_term > self.current_term {
            return Err(
                BootstrapValidationError::SnapshotHardStateTermAheadOfCurrentTerm {
                    snapshot_hard_state_term: metadata.hard_state_term,
                    current_term: self.current_term,
                },
            );
        }

        let writer_is_voter = metadata.committed_membership().map_or_else(
            || config.voters().any(|voter| voter == metadata.writer_id),
            |membership| membership.contains_voter(metadata.writer_id),
        );
        if writer_is_voter {
            Ok(())
        } else {
            Err(BootstrapValidationError::SnapshotWriterNonVoter {
                writer_id: metadata.writer_id,
            })
        }
    }
}

fn validate_log(
    entries: Vec<BootstrapLogEntry>,
    snapshot: Option<&RaftSnapshotMetadata>,
    current_term: Term,
    stored_commit_index: LogIndex,
    stored_committed_configuration: Option<CommittedConfiguration>,
) -> Result<Vec<LogEntry>, BootstrapValidationError> {
    let snapshot_index = snapshot.map_or(LogIndex::ZERO, |snapshot| snapshot.last_included_index);
    let commit_index = stored_commit_index.max(snapshot_index);
    let mut expected = snapshot_index.next();
    let mut materialized = Vec::new();
    let mut uncommitted_configuration_index = None;
    let mut latest_committed_configuration = None;
    let mut last_log_index = snapshot_index;

    for entry in entries {
        if entry.index < snapshot_index {
            return Err(BootstrapValidationError::CompactedLogEntry {
                snapshot_index,
                entry_index: entry.index,
            });
        }
        if let Some(snapshot) = snapshot {
            if entry.index == snapshot_index {
                validate_boundary_entry(&entry, snapshot)?;
                continue;
            }
        }
        if entry.index != expected {
            return Err(BootstrapValidationError::NonContiguousLog {
                expected,
                actual: entry.index,
            });
        }
        if entry.term.is_zero() {
            return Err(BootstrapValidationError::ZeroTermLogEntry { index: entry.index });
        }
        if entry.term > current_term {
            return Err(BootstrapValidationError::EntryTermAheadOfCurrentTerm {
                index: entry.index,
                entry_term: entry.term,
                current_term,
            });
        }
        expected = LogIndex(
            entry
                .index
                .0
                .checked_add(1)
                .ok_or(BootstrapValidationError::LogIndexAtMaximum { index: entry.index })?,
        );
        last_log_index = entry.index;
        if let Some(configuration) = entry.kind.configuration_entry() {
            if entry.index <= commit_index {
                latest_committed_configuration = Some(CommittedConfiguration {
                    index: entry.index,
                    config_id: configuration.config_id(),
                });
            } else if let Some(first_index) = uncommitted_configuration_index {
                return Err(
                    BootstrapValidationError::MultipleUncommittedConfigurationEntries {
                        first_index,
                        second_index: entry.index,
                    },
                );
            } else {
                uncommitted_configuration_index = Some(entry.index);
            }
        }
        materialized.push(LogEntry {
            term: entry.term,
            kind: entry.kind,
        });
    }
    if commit_index > last_log_index {
        return Err(BootstrapValidationError::CommitIndexBeyondLog {
            commit_index,
            last_log_index,
        });
    }
    validate_committed_configuration(
        stored_committed_configuration,
        latest_committed_configuration,
        snapshot,
        commit_index,
    )?;
    Ok(materialized)
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
        return Ok(());
    }

    let snapshot_index = snapshot.map_or(LogIndex::ZERO, |snapshot| snapshot.last_included_index);
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
