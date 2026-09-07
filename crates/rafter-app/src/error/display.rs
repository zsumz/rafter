//! How a group error renders, and what it exposes as its source.
//!
//! `Display` states the category alone; `source()` reaches the preserved
//! failure. Keeping the two apart is what lets a chain-aware report print
//! one link per real failure instead of the same text twice.

use std::{error::Error, fmt};

use super::GroupError;

impl<E, R> fmt::Display for GroupError<E, R>
where
    E: fmt::Display,
    R: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GroupError::Runtime(error) => write!(formatter, "Raft runtime failed: {error}"),
            Self::StateMachine { operation, source } => {
                write!(formatter, "state machine {operation} failed: {source}")
            }
            Self::ApplyResultCountMismatch { expected, actual } => write!(
                formatter,
                "state machine returned {actual} apply results for {expected} committed entries"
            ),
            Self::ApplyResultMetadataMismatch {
                expected_index,
                actual_index,
                expected_term,
                actual_term,
                expected_local_proposal_id,
                actual_local_proposal_id,
            } => write!(
                formatter,
                "state machine apply result metadata mismatch: expected index {expected_index}, term {expected_term}, local proposal {expected_local_proposal_id:?}; got index {actual_index}, term {actual_term}, local proposal {actual_local_proposal_id:?}"
            ),
            Self::ApplyEntryAlreadyApplied {
                entry_index,
                app_applied_index,
                group_applied_index,
            } => write!(
                formatter,
                "refusing to apply entry {entry_index} because the app reports applied index {app_applied_index} while the group reports {group_applied_index}"
            ),
            Self::AppliedIndexBehind { required, actual } => write!(
                formatter,
                "state machine applied index {actual} is behind required index {required}"
            ),
            Self::AppliedIndexBelowSnapshotBoundary {
                app_applied_index,
                snapshot_index,
            } => write!(
                formatter,
                "state machine applied index {app_applied_index} is below the snapshot boundary {snapshot_index}, whose covered entries are compacted and can never be applied"
            ),
            Self::SnapshotRestoreRequired {
                app_applied_index,
                snapshot_index,
                entry_index,
            } => write!(
                formatter,
                "refusing to apply committed entry {entry_index} onto a state machine at applied index {app_applied_index}, which is below the snapshot boundary {snapshot_index}: install the snapshot first"
            ),
            Self::MalformedSnapshot { reason } => write!(formatter, "malformed snapshot: {reason}"),
            Self::SnapshotsUnsupported { snapshot_index } => write!(
                formatter,
                "refusing snapshot install at index {snapshot_index}: the state machine declares no application snapshot support"
            ),
            Self::SnapshotSupportMisdeclared { snapshot_index } => write!(
                formatter,
                "state machine declares application snapshot support but refused the install at index {snapshot_index} as unsupported"
            ),
            Self::Poisoned { reason, .. } => write!(formatter, "Raft group is poisoned: {reason}"),
            GroupError::WrongGroup => formatter.write_str("input targets a different Raft group"),
            Self::WrongRecipient { expected, actual } => write!(
                formatter,
                "peer message targets {actual}, but this group is node {expected}"
            ),
            Self::NonMonotonicLocalProposalId {
                local_proposal_id,
                last_seen_local_proposal_id,
            } => write!(
                formatter,
                "local proposal id {local_proposal_id} is not greater than last seen id {last_seen_local_proposal_id}"
            ),
            Self::DuplicateReadId { read_id } => {
                write!(formatter, "read id {read_id} is already pending")
            }
            Self::NonMonotonicReadId {
                read_id,
                last_seen_read_id,
            } => write!(
                formatter,
                "read id {read_id} is not greater than last seen id {last_seen_read_id}"
            ),
            Self::UnsupportedReadConsistency { consistency } => {
                write!(formatter, "unsupported read consistency {consistency:?}")
            }
            Self::UnsupportedOutput { output } => {
                write!(formatter, "unsupported Raft output in app layer: {output}")
            }
        }
    }
}

impl<E, R> Error for GroupError<E, R>
where
    E: Error + 'static,
    R: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            GroupError::Runtime(error) => Some(error),
            Self::StateMachine { source, .. } => Some(&**source),
            // Transparent to the preserved cause: a chain printer walks one
            // link per real failure rather than one per boundary crossed.
            Self::Poisoned { cause, .. } => match cause {
                Some(cause) => Some(cause.as_error()),
                None => None,
            },
            Self::ApplyResultCountMismatch { .. }
            | Self::ApplyResultMetadataMismatch { .. }
            | Self::ApplyEntryAlreadyApplied { .. }
            | Self::AppliedIndexBehind { .. }
            | Self::AppliedIndexBelowSnapshotBoundary { .. }
            | Self::SnapshotRestoreRequired { .. }
            | Self::MalformedSnapshot { .. }
            | Self::SnapshotsUnsupported { .. }
            | Self::SnapshotSupportMisdeclared { .. }
            | GroupError::WrongGroup
            | Self::WrongRecipient { .. }
            | Self::NonMonotonicLocalProposalId { .. }
            | Self::DuplicateReadId { .. }
            | Self::NonMonotonicReadId { .. }
            | Self::UnsupportedReadConsistency { .. }
            | Self::UnsupportedOutput { .. } => None,
        }
    }
}
