//! Nonzero connection-session and per-connection sequence numbers.

use std::{error::Error, fmt, num::NonZeroU64};

/// A durable monotonically increasing connection epoch.
///
/// Session zero is reserved as "no session has ever been allocated" in the
/// durable state format and is therefore never a valid live session.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionSession(NonZeroU64);

impl ConnectionSession {
    /// First session allocated for a peer with no durable outbound history.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// Creates a nonzero connection session.
    ///
    /// # Errors
    ///
    /// Returns [`ZeroConnectionNumber`] when `value` is zero.
    pub const fn new(value: u64) -> Result<Self, ZeroConnectionNumber> {
        match NonZeroU64::new(value) {
            Some(value) => Ok(Self(value)),
            None => Err(ZeroConnectionNumber::Session),
        }
    }

    /// Creates a session from an already validated nonzero value.
    #[must_use]
    pub const fn from_nonzero(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the encoded integer value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub(crate) const fn checked_next(self) -> Option<Self> {
        match self.get().checked_add(1) {
            Some(value) => match NonZeroU64::new(value) {
                Some(value) => Some(Self(value)),
                None => None,
            },
            None => None,
        }
    }
}

impl fmt::Display for ConnectionSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.get())
    }
}

impl From<NonZeroU64> for ConnectionSession {
    fn from(value: NonZeroU64) -> Self {
        Self::from_nonzero(value)
    }
}

impl From<ConnectionSession> for NonZeroU64 {
    fn from(value: ConnectionSession) -> Self {
        value.0
    }
}

impl TryFrom<u64> for ConnectionSession {
    type Error = ZeroConnectionNumber;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// An exact sequence number within one authenticated connection session.
///
/// Every direction begins at one and accepts each following value exactly
/// once. Sequence state is intentionally not durable: a process restart closes
/// the connection, and the next connection must use a newer durable session.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionSequence(NonZeroU64);

impl ConnectionSequence {
    /// First sequence emitted or accepted on a new directional stream.
    pub const FIRST: Self = Self(NonZeroU64::MIN);

    /// Creates a nonzero connection sequence.
    ///
    /// # Errors
    ///
    /// Returns [`ZeroConnectionNumber`] when `value` is zero.
    pub const fn new(value: u64) -> Result<Self, ZeroConnectionNumber> {
        match NonZeroU64::new(value) {
            Some(value) => Ok(Self(value)),
            None => Err(ZeroConnectionNumber::Sequence),
        }
    }

    /// Creates a sequence from an already validated nonzero value.
    #[must_use]
    pub const fn from_nonzero(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the encoded integer value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    pub(crate) const fn checked_next(self) -> Option<Self> {
        match self.get().checked_add(1) {
            Some(value) => match NonZeroU64::new(value) {
                Some(value) => Some(Self(value)),
                None => None,
            },
            None => None,
        }
    }
}

impl fmt::Display for ConnectionSequence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.get())
    }
}

impl From<NonZeroU64> for ConnectionSequence {
    fn from(value: NonZeroU64) -> Self {
        Self::from_nonzero(value)
    }
}

impl From<ConnectionSequence> for NonZeroU64 {
    fn from(value: ConnectionSequence) -> Self {
        value.0
    }
}

impl TryFrom<u64> for ConnectionSequence {
    type Error = ZeroConnectionNumber;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// A zero value was supplied for a connection number that starts at one.
///
/// This enum is exhaustive over the session and sequence counters carried by
/// transport wire version 1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ZeroConnectionNumber {
    /// Connection session zero is reserved by the durable format.
    Session,
    /// Connection sequence zero is outside the live stream grammar.
    Sequence,
}

impl fmt::Display for ZeroConnectionNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session => formatter.write_str("connection session must be nonzero"),
            Self::Sequence => formatter.write_str("connection sequence must be nonzero"),
        }
    }
}

impl Error for ZeroConnectionNumber {}
