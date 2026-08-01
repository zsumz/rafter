//! Synchronous managed-service transport admission failures.

use std::{error::Error, fmt};

use rafter::NodeId;
use rafter_codec::EncodePeerMessageError;

use crate::{DirectoryError, PeerId, TrafficClass};

use super::BoxError;

/// Synchronous refusal from [`crate::TlsSender`].
#[derive(Debug)]
#[non_exhaustive]
pub enum TlsTransportError {
    /// The sender's directory does not host the envelope's group.
    UnknownGroup,
    /// The group has no stable transport-principal binding for this node.
    UnknownNode {
        /// Unmapped Raft identity.
        node_id: NodeId,
    },
    /// The envelope's sender is not the local transport principal in this group.
    LocalIdentityMismatch {
        /// Raft identity claimed as the sender.
        node_id: NodeId,
    },
    /// The target identity is not currently authorized for this group.
    UnauthorizedPeer {
        /// Refused Raft identity.
        node_id: NodeId,
    },
    /// A committed removal permanently retired the target identity.
    RetiredPeer {
        /// Refused Raft identity.
        node_id: NodeId,
    },
    /// No persistent sender worker exists for the mapped physical peer.
    EndpointUnavailable {
        /// Physical peer without a configured worker.
        peer: PeerId,
    },
    /// One peer's finite queue refused the frame without waiting.
    QueueFull {
        /// Physical peer whose queue was full.
        peer: PeerId,
        /// Traffic class whose capacity was exhausted.
        class: TrafficClass,
        /// Complete frames retained at refusal time.
        frames: usize,
        /// Complete frame bytes retained at refusal time.
        bytes: usize,
    },
    /// Caller-owned canonical group encoding failed.
    GroupEncode {
        /// Original group-codec error.
        source: BoxError,
    },
    /// Canonical group encoding produced no bytes.
    EmptyGroupId,
    /// Canonical group encoding exceeded its finite bound.
    GroupIdTooLarge {
        /// Actual canonical bytes.
        actual: usize,
        /// Maximum accepted bytes.
        maximum: usize,
    },
    /// The inner Rafter message could not be encoded.
    MessageEncode {
        /// Original peer-codec error.
        source: EncodePeerMessageError,
    },
    /// Inner-message length did not fit the frozen frame grammar.
    MessageLengthOverflow,
    /// Complete-frame length arithmetic overflowed.
    FrameLengthOverflow,
    /// The encoded frame exceeded the configured finite bound.
    FrameTooLarge {
        /// Actual complete-frame body bytes.
        actual: usize,
        /// Maximum accepted body bytes.
        maximum: usize,
    },
    /// The envelope sender disagreed with the sender embedded in the message.
    SenderMismatch {
        /// Sender named by the envelope.
        envelope_from: NodeId,
        /// Sender encoded inside the Rafter message.
        message_from: NodeId,
    },
    /// Directory state could not be read or updated safely.
    Directory {
        /// Original directory error.
        source: DirectoryError,
    },
    /// No snapshot directive resolver is installed.
    SnapshotResolverUnavailable,
    /// Shutdown has begun or completed.
    Stopped,
    /// A terminal local failure made further admission unsafe.
    TerminalFailure {
        /// First latched failure, when available.
        message: Option<String>,
    },
    /// A poisoned internal queue made its accounting untrustworthy.
    InternalState,
}

impl fmt::Display for TlsTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownGroup => formatter.write_str("transport does not know this group"),
            Self::UnknownNode { node_id } => {
                write!(formatter, "{node_id} has no transport-principal binding")
            }
            Self::LocalIdentityMismatch { node_id } => write!(
                formatter,
                "outbound envelope sender {node_id} is not this transport's local principal"
            ),
            Self::UnauthorizedPeer { node_id } => {
                write!(formatter, "{node_id} is not authorized for this group")
            }
            Self::RetiredPeer { node_id } => {
                write!(formatter, "{node_id} was retired by a committed removal")
            }
            Self::EndpointUnavailable { peer } => {
                write!(formatter, "peer {peer} has no persistent sender worker")
            }
            Self::QueueFull {
                peer,
                class,
                frames,
                bytes,
            } => write!(
                formatter,
                "peer {peer} {class} queue refused admission at {frames} frames/{bytes} bytes"
            ),
            Self::GroupEncode { source } => write!(formatter, "group encoding failed: {source}"),
            Self::EmptyGroupId => formatter.write_str("canonical group ID must not be empty"),
            Self::GroupIdTooLarge { actual, maximum } => write!(
                formatter,
                "canonical group ID is {actual} bytes, exceeding maximum {maximum}"
            ),
            Self::MessageEncode { source } => {
                write!(formatter, "Rafter peer-message encoding failed: {source}")
            }
            Self::MessageLengthOverflow => {
                formatter.write_str("Rafter peer-message length does not fit u32")
            }
            Self::FrameLengthOverflow => {
                formatter.write_str("peer-frame length arithmetic overflowed")
            }
            Self::FrameTooLarge { actual, maximum } => write!(
                formatter,
                "peer-frame body is {actual} bytes, exceeding maximum {maximum}"
            ),
            Self::SenderMismatch {
                envelope_from,
                message_from,
            } => write!(
                formatter,
                "envelope sender {envelope_from} does not match message sender {message_from}"
            ),
            Self::Directory { source } => write!(formatter, "peer directory failed: {source}"),
            Self::SnapshotResolverUnavailable => {
                formatter.write_str("no snapshot directive resolver is installed")
            }
            Self::Stopped => formatter.write_str("transport is stopping or stopped"),
            Self::TerminalFailure {
                message: Some(message),
            } => {
                write!(formatter, "transport failed terminally: {message}")
            }
            Self::TerminalFailure { message: None } => {
                formatter.write_str("transport failed terminally")
            }
            Self::InternalState => formatter.write_str("transport queue state is poisoned"),
        }
    }
}

impl Error for TlsTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::GroupEncode { source } => Some(source.as_ref()),
            Self::MessageEncode { source } => Some(source),
            Self::Directory { source } => Some(source),
            _ => None,
        }
    }
}
