//! Error vocabulary for length-prefixed peer-message frame I/O.
//!
//! This module owns how a framing failure is named, rendered, and chained to
//! its source. It reads and writes nothing itself, and it says nothing about
//! connections: a transport wraps these errors rather than replacing them.

use std::error::Error;
use std::fmt;
use std::io;

use rafter_codec::{DecodePeerMessageError, EncodePeerMessageError};

/// Errors raised while writing a length-prefixed peer-message frame.
#[derive(Debug)]
#[non_exhaustive]
pub enum WriteFrameError {
    /// The peer message could not be encoded.
    Encode(EncodePeerMessageError),
    /// The encoded frame cannot be represented by the four-byte prefix.
    FrameTooLarge {
        /// Encoded payload length in bytes.
        len: usize,
    },
    /// Writing the length prefix or payload failed.
    Io(io::Error),
}

/// Errors raised while reading a length-prefixed peer-message frame.
#[derive(Debug)]
#[non_exhaustive]
pub enum ReadFrameError {
    /// Reading the length prefix or payload failed.
    Io(io::Error),
    /// The declared payload exceeds the caller's receive bound.
    FrameTooLarge {
        /// Declared payload length in bytes.
        len: usize,
        /// Configured maximum payload length in bytes.
        max: usize,
    },
    /// The bounded payload could not be decoded as a peer message.
    Decode(DecodePeerMessageError),
}

impl fmt::Display for WriteFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(error) => write!(formatter, "failed to encode peer message: {error}"),
            Self::FrameTooLarge { len } => {
                write!(
                    formatter,
                    "encoded peer message length {len} exceeds u32::MAX"
                )
            }
            Self::Io(error) => write!(formatter, "failed to write peer message frame: {error}"),
        }
    }
}

impl Error for WriteFrameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::FrameTooLarge { .. } => None,
        }
    }
}

impl fmt::Display for ReadFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "failed to read peer message frame: {error}"),
            Self::FrameTooLarge { len, max } => {
                write!(
                    formatter,
                    "peer message frame length {len} exceeds maximum {max}"
                )
            }
            Self::Decode(error) => write!(formatter, "failed to decode peer message: {error}"),
        }
    }
}

impl Error for ReadFrameError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::FrameTooLarge { .. } => None,
        }
    }
}
