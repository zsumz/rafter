#![allow(clippy::wildcard_imports)]

use std::{error::Error, fmt};

use super::*;

/// Error returned while constructing or manually driving an in-memory managed
/// service driver.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ManagedDriverError {
    EmptyCluster,
    MissingPrimary {
        node_id: NodeId,
    },
    MissingNode {
        node_id: NodeId,
    },
    DuplicateNode {
        node_id: NodeId,
    },
    PoisonedGroup {
        node_id: NodeId,
        reason: String,
    },
    NonQuiescentGroup {
        node_id: NodeId,
        pending_proposals: usize,
        reserved_reads: usize,
    },
    LocalProposalIdExhausted {
        node_id: NodeId,
        last_seen_local_proposal_id: LocalProposalId,
    },
    ReadIdExhausted {
        node_id: NodeId,
        last_seen_read_id: ReadId,
    },
    MixedGroups,
    Stalled {
        max_steps: usize,
    },
    ShuttingDown,
    Group {
        message: String,
    },
}

impl fmt::Display for ManagedDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCluster => formatter.write_str("managed driver requires at least one group"),
            Self::MissingPrimary { node_id } => {
                write!(formatter, "managed driver primary node {node_id} is missing")
            }
            Self::MissingNode { node_id } => {
                write!(formatter, "managed driver node {node_id} is missing")
            }
            Self::DuplicateNode { node_id } => {
                write!(formatter, "managed driver has duplicate node {node_id}")
            }
            Self::PoisonedGroup { node_id, reason } => write!(
                formatter,
                "managed driver group for node {node_id} is poisoned: {reason}"
            ),
            Self::NonQuiescentGroup {
                node_id,
                pending_proposals,
                reserved_reads,
            } => write!(
                formatter,
                "managed driver cannot adopt node {node_id}: {pending_proposals} pending proposals and {reserved_reads} reserved reads remain"
            ),
            Self::LocalProposalIdExhausted {
                node_id,
                last_seen_local_proposal_id,
            } => write!(
                formatter,
                "managed driver node {node_id} exhausted local proposal ids after {last_seen_local_proposal_id}"
            ),
            Self::ReadIdExhausted {
                node_id,
                last_seen_read_id,
            } => write!(
                formatter,
                "managed driver node {node_id} exhausted read ids after {last_seen_read_id}"
            ),
            Self::MixedGroups => formatter.write_str("managed driver cannot adopt mixed group ids"),
            Self::Stalled { max_steps } => write!(
                formatter,
                "managed driver made no progress within {max_steps} drive steps"
            ),
            Self::ShuttingDown => formatter.write_str("managed driver is shutting down"),
            Self::Group { message } => write!(formatter, "managed driver group error: {message}"),
        }
    }
}

impl Error for ManagedDriverError {}

#[derive(Debug)]
pub(super) enum ManagedOperationError<E, RE> {
    MissingNode { node_id: NodeId },
    Transport(String),
    ShuttingDown,
    Write(WriteError),
    Read(ReadError),
    Transfer(TransferLeadershipError),
    Group(GroupError<E, RE>),
}

impl<E, RE> ManagedOperationError<E, RE>
where
    E: Debug,
    RE: Debug + fmt::Display,
{
    pub(super) fn into_write_error(self) -> WriteError {
        match self {
            Self::Write(error) => error,
            Self::Read(error) => WriteError::Transport {
                message: error.to_string(),
            },
            Self::Transfer(error) => WriteError::Transport {
                message: error.to_string(),
            },
            Self::MissingNode { node_id } => WriteError::Transport {
                message: format!("missing node {node_id}"),
            },
            Self::Transport(message) => WriteError::Transport { message },
            Self::ShuttingDown => WriteError::ShuttingDown,
            Self::Group(error) => write_error_from_group(error),
        }
    }

    pub(super) fn into_read_error(self) -> ReadError {
        match self {
            Self::Read(error) => error,
            Self::Write(error) => ReadError::Transport {
                message: error.to_string(),
            },
            Self::Transfer(error) => ReadError::Transport {
                message: error.to_string(),
            },
            Self::MissingNode { node_id } => ReadError::Transport {
                message: format!("missing node {node_id}"),
            },
            Self::Transport(message) => ReadError::Transport { message },
            Self::ShuttingDown => ReadError::ShuttingDown,
            Self::Group(error) => read_error_from_group(error),
        }
    }

    pub(super) fn into_transfer_error(self) -> TransferLeadershipError {
        match self {
            Self::Transfer(error) => error,
            Self::Write(error) => TransferLeadershipError::Transport {
                message: error.to_string(),
            },
            Self::Read(error) => TransferLeadershipError::Transport {
                message: error.to_string(),
            },
            Self::MissingNode { node_id } => TransferLeadershipError::Transport {
                message: format!("missing node {node_id}"),
            },
            Self::Transport(message) => TransferLeadershipError::Transport { message },
            Self::ShuttingDown => TransferLeadershipError::ShuttingDown,
            Self::Group(error) => transfer_error_from_group(error),
        }
    }
}

impl<E, RE> From<GroupError<E, RE>> for ManagedOperationError<E, RE> {
    fn from(error: GroupError<E, RE>) -> Self {
        Self::Group(error)
    }
}

impl<E, RE> From<ManagedOperationError<E, RE>> for ManagedDriverError
where
    E: Debug,
    RE: Debug + fmt::Display,
{
    fn from(error: ManagedOperationError<E, RE>) -> Self {
        match error {
            ManagedOperationError::MissingNode { node_id } => Self::MissingNode { node_id },
            ManagedOperationError::Transport(message) => Self::Group { message },
            ManagedOperationError::ShuttingDown => Self::ShuttingDown,
            ManagedOperationError::Write(error) => Self::Group {
                message: error.to_string(),
            },
            ManagedOperationError::Read(error) => Self::Group {
                message: error.to_string(),
            },
            ManagedOperationError::Transfer(error) => Self::Group {
                message: error.to_string(),
            },
            ManagedOperationError::Group(error) => Self::Group {
                message: format!("{error:?}"),
            },
        }
    }
}

pub(super) fn write_error_from_group<E, RE>(error: GroupError<E, RE>) -> WriteError
where
    E: Debug,
    RE: Debug + fmt::Display,
{
    match error {
        GroupError::Poisoned { reason, .. } => WriteError::Poisoned { reason },
        GroupError::NonMonotonicLocalProposalId {
            local_proposal_id,
            last_seen_local_proposal_id,
        } => {
            WriteError::ManagedInvariantViolation {
                message: format!(
                    "managed driver local-ID invariant violation: generated non-monotonic local proposal id {local_proposal_id} after {last_seen_local_proposal_id}"
                ),
            }
        }
        GroupError::StateMachine { operation, source } => match operation {
            StateMachineOperation::ApplyBatch
            | StateMachineOperation::DecodeCommand
            | StateMachineOperation::Read
            | StateMachineOperation::InstallSnapshot => WriteError::ApplyFailed {
                message: format!("{source:?}"),
            },
            StateMachineOperation::AppliedIndex | StateMachineOperation::EncodeCommand => {
                WriteError::Storage {
                    message: format!("{source:?}"),
                }
            }
        },
        GroupError::Runtime(error) => WriteError::Storage {
            message: error.to_string(),
        },
        error => WriteError::Transport {
            message: format!("{error:?}"),
        },
    }
}

pub(super) fn read_error_from_group<E, RE>(error: GroupError<E, RE>) -> ReadError
where
    E: Debug,
    RE: Debug + fmt::Display,
{
    match error {
        GroupError::Poisoned { reason, .. } => ReadError::Poisoned { reason },
        GroupError::DuplicateReadId { read_id } => ReadError::ManagedInvariantViolation {
            message: format!(
                "managed driver local-ID invariant violation: generated duplicate read id {read_id}"
            ),
        },
        GroupError::NonMonotonicReadId {
            read_id,
            last_seen_read_id,
        } => ReadError::ManagedInvariantViolation {
            message: format!(
                "managed driver local-ID invariant violation: generated non-monotonic read id {read_id} after {last_seen_read_id}"
            ),
        },
        GroupError::StateMachine { source, .. } => ReadError::ApplyFailed {
            message: format!("{source:?}"),
        },
        GroupError::Runtime(error) => ReadError::Storage {
            message: error.to_string(),
        },
        GroupError::UnsupportedReadConsistency { consistency } => {
            ReadError::UnsupportedConsistency { consistency }
        }
        error => ReadError::Transport {
            message: format!("{error:?}"),
        },
    }
}

pub(super) fn transfer_error_from_group<E, RE>(error: GroupError<E, RE>) -> TransferLeadershipError
where
    E: Debug,
    RE: Debug + fmt::Display,
{
    match error {
        GroupError::Poisoned { reason, .. } => TransferLeadershipError::Poisoned { reason },
        GroupError::Runtime(error) => TransferLeadershipError::Storage {
            message: error.to_string(),
        },
        error => TransferLeadershipError::Transport {
            message: format!("{error:?}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct MappingRuntimeError;

    impl fmt::Display for MappingRuntimeError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("mapping runtime error")
        }
    }

    #[test]
    fn non_monotonic_local_proposal_id_maps_to_managed_invariant_write_error() {
        assert_eq!(
            write_error_from_group::<String, MappingRuntimeError>(
                GroupError::NonMonotonicLocalProposalId {
                    local_proposal_id: LocalProposalId(7),
                    last_seen_local_proposal_id: LocalProposalId(9),
                },
            ),
            WriteError::ManagedInvariantViolation {
                message: "managed driver local-ID invariant violation: generated non-monotonic local proposal id local-proposal-7 after local-proposal-9".to_owned(),
            }
        );
    }

    #[test]
    fn duplicate_read_id_maps_to_managed_invariant_read_error() {
        assert_eq!(
            read_error_from_group::<String, MappingRuntimeError>(GroupError::DuplicateReadId {
                read_id: ReadId(8),
            }),
            ReadError::ManagedInvariantViolation {
                message:
                    "managed driver local-ID invariant violation: generated duplicate read id read-8"
                        .to_owned(),
            }
        );
    }

    #[test]
    fn non_monotonic_read_id_maps_to_managed_invariant_read_error() {
        assert_eq!(
            read_error_from_group::<String, MappingRuntimeError>(
                GroupError::NonMonotonicReadId {
                    read_id: ReadId(8),
                    last_seen_read_id: ReadId(10),
                },
            ),
            ReadError::ManagedInvariantViolation {
                message:
                    "managed driver local-ID invariant violation: generated non-monotonic read id read-8 after read-10"
                        .to_owned(),
            }
        );
    }

    #[test]
    fn managed_driver_error_is_a_standard_error_with_display_message() {
        let error = ManagedDriverError::MissingNode { node_id: NodeId(9) };
        let standard_error: &(dyn Error + 'static) = &error;

        assert_eq!(
            standard_error.to_string(),
            "managed driver node node-9 is missing"
        );
    }
}
