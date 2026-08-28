#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! What a driver stage carries before a client hears about it.
//!
//! The staged error every driver operation travels in, and the one error object
//! this crate authors rather than preserves: a driver reporting on its own
//! routing. Each staged variant is projected into the write, read, or transfer
//! vocabulary by the caller that knows which surface is waiting, because the
//! same fault means different things to the three of them.

use std::{error::Error, fmt};

use super::*;

/// Internal error carried between driver stages before it reaches a client.
///
/// `WrongGroup` is a driver fact rather than a delivery failure, which is why
/// it is a variant here instead of a synthesized transport error.
#[derive(Debug)]
pub(super) enum ManagedOperationError<E, RE> {
    MissingNode { node_id: NodeId },
    WrongGroup,
    DriveBoundReached { max_steps: usize },
    ShuttingDown,
    Write(WriteError),
    Read(ReadError),
    Transfer(TransferLeadershipError),
    Group(GroupError<E, RE>),
}

/// A driver stage that could not route its own work.
///
/// This is the driver reporting on itself, so it is the one place the service
/// layer authors an error object rather than preserving somebody else's.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DriverRoutingError {
    /// A frame was addressed to a node this driver does not own.
    MissingNode { node_id: NodeId },
    /// The driver stopped routing at its own bound rather than looping forever.
    DriveBoundReached { max_steps: usize },
    /// The driver already holds its configured maximum of unresolved waiters.
    ///
    /// Failing closed rather than growing: the operation was refused before
    /// anything was proposed, so nothing is in flight to be uncertain about.
    PendingWaiterLimit { max_pending_waiters: usize },
}

impl fmt::Display for DriverRoutingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNode { node_id } => {
                write!(formatter, "managed driver node {node_id} is missing")
            }
            Self::DriveBoundReached { max_steps } => write!(
                formatter,
                "managed driver did not drain within {max_steps} drive steps"
            ),
            Self::PendingWaiterLimit {
                max_pending_waiters,
            } => write!(
                formatter,
                "managed driver already holds {max_pending_waiters} unresolved waiters"
            ),
        }
    }
}

impl Error for DriverRoutingError {}

impl<E, RE> ManagedOperationError<E, RE>
where
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    /// Maps a staged error into the write surface.
    ///
    /// `fate` is the fate the driver observed for this write, and it is passed
    /// in rather than inferred: the same fault can occur on either side of the
    /// local append, and only the caller knows which side this one was on.
    pub(super) fn into_write_error(self, fate: WriteFate) -> WriteError {
        match self {
            Self::Write(error) => error,
            Self::Read(error) => WriteError::Transport {
                fate,
                cause: ErrorCause::new(error),
            },
            Self::Transfer(error) => WriteError::Transport {
                fate,
                cause: ErrorCause::new(error),
            },
            Self::MissingNode { node_id } => WriteError::Transport {
                fate,
                cause: ErrorCause::new(DriverRoutingError::MissingNode { node_id }),
            },
            Self::DriveBoundReached { max_steps } => WriteError::Transport {
                fate,
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            Self::WrongGroup => WriteError::WrongGroup,
            Self::ShuttingDown => WriteError::ShuttingDown,
            Self::Group(error) => write_error_from_group(error, fate),
        }
    }

    pub(super) fn into_read_error(self) -> ReadError {
        match self {
            Self::Read(error) => error,
            Self::Write(error) => ReadError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::Transfer(error) => ReadError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::MissingNode { node_id } => ReadError::Transport {
                cause: ErrorCause::new(DriverRoutingError::MissingNode { node_id }),
            },
            Self::DriveBoundReached { max_steps } => ReadError::Transport {
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            Self::WrongGroup => ReadError::WrongGroup,
            Self::ShuttingDown => ReadError::ShuttingDown,
            Self::Group(error) => read_error_from_group(error),
        }
    }

    pub(super) fn into_transfer_error(self) -> TransferLeadershipError {
        match self {
            Self::Transfer(error) => error,
            Self::Write(error) => TransferLeadershipError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::Read(error) => TransferLeadershipError::Transport {
                cause: ErrorCause::new(error),
            },
            Self::MissingNode { node_id } => TransferLeadershipError::Transport {
                cause: ErrorCause::new(DriverRoutingError::MissingNode { node_id }),
            },
            Self::DriveBoundReached { max_steps } => TransferLeadershipError::Transport {
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            Self::WrongGroup => TransferLeadershipError::WrongGroup,
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
    E: Error + Send + Sync + 'static,
    RE: Error + Send + Sync + 'static,
{
    fn from(error: ManagedOperationError<E, RE>) -> Self {
        match error {
            ManagedOperationError::MissingNode { node_id } => Self::MissingNode { node_id },
            ManagedOperationError::DriveBoundReached { max_steps } => Self::Group {
                cause: ErrorCause::new(DriverRoutingError::DriveBoundReached { max_steps }),
            },
            ManagedOperationError::WrongGroup => Self::Group {
                cause: ErrorCause::new(WriteError::WrongGroup),
            },
            ManagedOperationError::ShuttingDown => Self::ShuttingDown,
            ManagedOperationError::Write(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
            ManagedOperationError::Read(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
            ManagedOperationError::Transfer(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
            ManagedOperationError::Group(error) => Self::Group {
                cause: ErrorCause::new(error),
            },
        }
    }
}
