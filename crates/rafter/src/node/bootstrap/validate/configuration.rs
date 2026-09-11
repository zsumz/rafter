//! Reconcile configuration identity without inferring new commitment.

use crate::{CommittedConfiguration, LogEntry, LogIndex, RaftSnapshotMetadata};

use super::super::{error::BootstrapValidationError, state::BootstrapLogEntry};

pub(super) fn recover_configuration(
    stored: Option<CommittedConfiguration>,
    snapshot: Option<&RaftSnapshotMetadata>,
    log: &[LogEntry],
    commit_index: LogIndex,
) -> Result<Option<CommittedConfiguration>, BootstrapValidationError> {
    let snapshot_index = snapshot.map_or(LogIndex::ZERO, |value| value.last_included_index);
    let captured = snapshot.and_then(RaftSnapshotMetadata::committed_configuration_state);
    if let Some(captured) = captured {
        validate_floor(captured, snapshot_index)?;
    }
    if let Some(stored) = stored {
        validate_floor(stored, commit_index)?;
        if stored.index > snapshot_index {
            let retained = usize::try_from(stored.index.0 - snapshot_index.0 - 1)
                .ok()
                .and_then(|offset| log.get(offset))
                .and_then(LogEntry::configuration_entry)
                .map(|entry| CommittedConfiguration {
                    index: stored.index,
                    config_id: entry.config_id(),
                })
                .ok_or_else(|| missing(stored))?;
            validate_identity(stored, retained)?;
        } else {
            if snapshot
                .and_then(RaftSnapshotMetadata::committed_membership)
                .is_none()
            {
                return Err(
                    BootstrapValidationError::CompactedCommittedConfigurationWithoutSnapshotMembership {
                        committed_configuration_index: stored.index,
                    },
                );
            }
            if let Some(captured) = captured {
                // A snapshot explicitly naming an older configuration cannot
                // justify a newer identity in its own covered prefix.
                if stored.index > captured.index {
                    return Err(missing(stored));
                }
                validate_identity(stored, captured)?;
            }
        }
    }

    let retained = log.iter().enumerate().rev().find_map(|(offset, entry)| {
        let index = LogIndex(snapshot_index.0 + offset as u64 + 1);
        (index <= commit_index)
            .then(|| {
                entry
                    .configuration_entry()
                    .map(|entry| CommittedConfiguration {
                        index,
                        config_id: entry.config_id(),
                    })
            })
            .flatten()
    });
    Ok([stored, captured, retained]
        .into_iter()
        .flatten()
        .max_by_key(|state| state.index))
}

pub(super) fn validate_boundary_configuration(
    entry: &BootstrapLogEntry,
    snapshot: &RaftSnapshotMetadata,
    stored: Option<CommittedConfiguration>,
) -> Result<(), BootstrapValidationError> {
    for expected in [stored, snapshot.committed_configuration_state()]
        .into_iter()
        .flatten()
        .filter(|state| state.index == entry.index)
    {
        let actual = entry
            .kind
            .configuration_entry()
            .ok_or_else(|| missing(expected))?;
        validate_identity(
            expected,
            CommittedConfiguration {
                index: entry.index,
                config_id: actual.config_id(),
            },
        )?;
    }
    Ok(())
}

fn validate_floor(
    state: CommittedConfiguration,
    floor: LogIndex,
) -> Result<(), BootstrapValidationError> {
    if state.index > floor {
        return Err(
            BootstrapValidationError::CommittedConfigurationAheadOfCommit {
                committed_configuration_index: state.index,
                commit_index: floor,
            },
        );
    }
    Ok(())
}

fn validate_identity(
    expected: CommittedConfiguration,
    actual: CommittedConfiguration,
) -> Result<(), BootstrapValidationError> {
    if expected.index == actual.index && expected.config_id != actual.config_id {
        return Err(BootstrapValidationError::CommittedConfigurationIdMismatch {
            index: expected.index,
            expected: expected.config_id,
            actual: actual.config_id,
        });
    }
    Ok(())
}

fn missing(state: CommittedConfiguration) -> BootstrapValidationError {
    BootstrapValidationError::CommittedConfigurationMissing {
        committed_configuration_index: state.index,
    }
}
