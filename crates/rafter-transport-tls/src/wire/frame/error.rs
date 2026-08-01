//! Peer-frame construction, configuration, encoding, and decoding errors.

use std::{error::Error, fmt};

use rafter::NodeId;
use rafter_codec::{DecodePeerMessageError, EncodePeerMessageError};

/// Structural refusal while constructing a [`super::PeerFrame`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PeerFrameError {
    /// The inner message sender disagrees with the outer authenticated envelope.
    SenderMismatch {
        /// Sender named by the outer frame.
        envelope_from: NodeId,
        /// Sender encoded inside the Rafter message.
        message_from: NodeId,
    },
}

impl fmt::Display for PeerFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SenderMismatch {
                envelope_from,
                message_from,
            } => write!(
                formatter,
                "peer frame sender {envelope_from} does not match embedded sender \
                 {message_from}"
            ),
        }
    }
}

impl Error for PeerFrameError {}

/// Invalid relationship between a group codec and the transport wire limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PeerFrameCodecConfigError {
    /// A group codec declared that it can encode no bytes.
    ZeroGroupIdBound,
    /// The codec's declared maximum exceeds the transport's accepted maximum.
    GroupIdBoundTooLarge {
        /// Codec-declared maximum.
        codec_maximum: usize,
        /// Transport maximum.
        wire_maximum: usize,
    },
}

impl fmt::Display for PeerFrameCodecConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroGroupIdBound => {
                formatter.write_str("group codec maximum encoded length must be nonzero")
            }
            Self::GroupIdBoundTooLarge {
                codec_maximum,
                wire_maximum,
            } => write!(
                formatter,
                "group codec maximum {codec_maximum} exceeds wire maximum \
                 {wire_maximum}"
            ),
        }
    }
}

impl Error for PeerFrameCodecConfigError {}

/// Failure while encoding one version-1 peer frame.
#[derive(Debug)]
#[non_exhaustive]
pub enum EncodePeerFrameError<E> {
    /// Caller-owned group encoding failed.
    GroupEncode(E),
    /// Canonical group encoding produced no bytes.
    EmptyGroupId,
    /// Canonical group encoding exceeded its declared or transport bound.
    GroupIdTooLarge {
        /// Actual encoded bytes.
        actual: usize,
        /// Maximum accepted bytes.
        maximum: usize,
    },
    /// The inner Rafter message has no valid current wire representation.
    MessageEncode(EncodePeerMessageError),
    /// Inner-message length did not fit its version-1 `u32` field.
    MessageLengthOverflow,
    /// Frame length overflowed local or version-1 arithmetic.
    FrameLengthOverflow,
    /// Complete body exceeded the configured receive bound.
    FrameTooLarge {
        /// Actual frame body bytes.
        actual: usize,
        /// Maximum accepted frame body bytes.
        maximum: usize,
    },
}

impl<E> fmt::Display for EncodePeerFrameError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupEncode(error) => write!(formatter, "group encoding failed: {error}"),
            Self::EmptyGroupId => formatter.write_str("canonical group ID must not be empty"),
            Self::GroupIdTooLarge { actual, maximum } => write!(
                formatter,
                "canonical group ID is {actual} bytes, exceeding maximum {maximum}"
            ),
            Self::MessageEncode(error) => {
                write!(formatter, "Rafter peer-message encoding failed: {error}")
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
        }
    }
}

impl<E> Error for EncodePeerFrameError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::GroupEncode(error) => Some(error),
            Self::MessageEncode(error) => Some(error),
            _ => None,
        }
    }
}

/// Malformed, noncanonical, or oversized version-1 peer frame.
#[derive(Debug)]
#[non_exhaustive]
pub enum DecodePeerFrameError<E> {
    /// Fewer than four bytes were available for the outer length.
    TruncatedLengthPrefix,
    /// A declared version-1 length cannot fit local address space.
    FrameLengthUnsupported(u32),
    /// The declared frame body exceeded the configured receive limit.
    FrameTooLarge {
        /// Declared frame body bytes.
        declared: usize,
        /// Maximum accepted frame body bytes.
        maximum: usize,
    },
    /// The complete frame ended before its declared length.
    TruncatedFrame {
        /// Complete bytes declared, including the prefix.
        declared: usize,
        /// Complete bytes available.
        actual: usize,
    },
    /// Bytes remained after the one declared frame.
    TrailingBytes {
        /// Unconsumed bytes.
        remaining: usize,
    },
    /// A fixed or length-delimited frame field ended early.
    TruncatedBody,
    /// The frame kind is not allocated by version 1.
    UnknownFrameKind(u8),
    /// Connection sequence zero is reserved and invalid.
    ZeroSequence,
    /// The canonical group ID was empty.
    EmptyGroupId,
    /// The group ID exceeded the codec or transport bound.
    GroupIdTooLarge {
        /// Declared or reproduced bytes.
        actual: usize,
        /// Maximum accepted bytes.
        maximum: usize,
    },
    /// The inner-message length cannot fit local address space.
    MessageLengthUnsupported(u32),
    /// The message length did not consume exactly the rest of the body.
    MessageLengthMismatch {
        /// Declared inner-message bytes.
        declared: usize,
        /// Bytes actually remaining in the frame body.
        remaining: usize,
    },
    /// Caller-owned group decoding failed.
    GroupDecode(E),
    /// Re-encoding the decoded caller-owned group failed.
    GroupReencode(E),
    /// Decoding and re-encoding changed the group-route bytes.
    NonCanonicalGroupId,
    /// The inner Rafter peer-message frame was invalid.
    MessageDecode(DecodePeerMessageError),
    /// The outer sender disagreed with the inner message sender.
    SenderMismatch {
        /// Sender named by the outer frame.
        envelope_from: NodeId,
        /// Sender encoded inside the inner message.
        message_from: NodeId,
    },
}

impl<E> fmt::Display for DecodePeerFrameError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TruncatedLengthPrefix => {
                formatter.write_str("peer frame ended inside its length prefix")
            }
            Self::FrameLengthUnsupported(length) => write!(
                formatter,
                "peer-frame body length {length} cannot fit local address space"
            ),
            Self::FrameTooLarge { declared, maximum } => write!(
                formatter,
                "peer-frame body declares {declared} bytes, exceeding maximum {maximum}"
            ),
            Self::TruncatedFrame { declared, actual } => write!(
                formatter,
                "peer frame declares {declared} complete bytes, but only {actual} are available"
            ),
            Self::TrailingBytes { remaining } => {
                write!(formatter, "peer frame has {remaining} trailing bytes")
            }
            Self::TruncatedBody => {
                formatter.write_str("peer frame ended before a body field was complete")
            }
            Self::UnknownFrameKind(kind) => {
                write!(formatter, "peer-frame kind {kind} is not allocated")
            }
            Self::ZeroSequence => formatter.write_str("peer-frame sequence must be nonzero"),
            Self::EmptyGroupId => formatter.write_str("canonical group ID must not be empty"),
            Self::GroupIdTooLarge { actual, maximum } => write!(
                formatter,
                "canonical group ID is {actual} bytes, exceeding maximum {maximum}"
            ),
            Self::MessageLengthUnsupported(length) => write!(
                formatter,
                "inner message length {length} cannot fit local address space"
            ),
            Self::MessageLengthMismatch {
                declared,
                remaining,
            } => write!(
                formatter,
                "inner message declares {declared} bytes, but {remaining} remain"
            ),
            Self::GroupDecode(error) => write!(formatter, "group decoding failed: {error}"),
            Self::GroupReencode(error) => {
                write!(formatter, "group canonical re-encoding failed: {error}")
            }
            Self::NonCanonicalGroupId => {
                formatter.write_str("decoded group ID did not re-encode to the exact routed bytes")
            }
            Self::MessageDecode(error) => {
                write!(formatter, "Rafter peer-message decoding failed: {error}")
            }
            Self::SenderMismatch {
                envelope_from,
                message_from,
            } => write!(
                formatter,
                "peer-frame sender {envelope_from} does not match embedded sender \
                 {message_from}"
            ),
        }
    }
}

impl<E> Error for DecodePeerFrameError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::GroupDecode(error) | Self::GroupReencode(error) => Some(error),
            Self::MessageDecode(error) => Some(error),
            _ => None,
        }
    }
}
