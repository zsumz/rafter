//! Exact in-memory sequence progression for one live connection.

use std::{error::Error, fmt};

use super::ConnectionSequence;

/// Outbound sequence allocator for one directional connection stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboundSequence {
    next: Option<ConnectionSequence>,
}

impl OutboundSequence {
    /// Creates an allocator whose first result is sequence one.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            next: Some(ConnectionSequence::FIRST),
        }
    }

    /// Returns the next exact sequence and advances the allocator.
    ///
    /// # Errors
    ///
    /// Returns [`SequenceExhausted`] after sequence `u64::MAX` was allocated.
    pub fn take_next(&mut self) -> Result<ConnectionSequence, SequenceExhausted> {
        let current = self.next.ok_or(SequenceExhausted)?;
        self.next = current.checked_next();
        Ok(current)
    }

    /// Returns the next sequence without advancing, or `None` after exhaustion.
    #[must_use]
    pub const fn next(self) -> Option<ConnectionSequence> {
        self.next
    }
}

impl Default for OutboundSequence {
    fn default() -> Self {
        Self::new()
    }
}

/// Inbound exact-sequence validator for one directional connection stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InboundSequence {
    expected: Option<ConnectionSequence>,
}

impl InboundSequence {
    /// Creates a validator that initially requires sequence one.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            expected: Some(ConnectionSequence::FIRST),
        }
    }

    /// Accepts exactly the next sequence and advances the validator.
    ///
    /// A duplicate, skipped, or reordered value is refused without changing the
    /// expected value.
    ///
    /// # Errors
    ///
    /// Returns [`SequenceError::Unexpected`] when `actual` is not the next exact
    /// sequence, or [`SequenceError::Exhausted`] after accepting `u64::MAX`.
    pub fn accept(&mut self, actual: ConnectionSequence) -> Result<(), SequenceError> {
        let Some(expected) = self.expected else {
            return Err(SequenceError::Exhausted);
        };
        if actual != expected {
            return Err(SequenceError::Unexpected { expected, actual });
        }
        self.expected = expected.checked_next();
        Ok(())
    }

    /// Returns the exact sequence currently required.
    #[must_use]
    pub const fn expected(self) -> Option<ConnectionSequence> {
        self.expected
    }
}

impl Default for InboundSequence {
    fn default() -> Self {
        Self::new()
    }
}

/// Outbound sequence space was consumed by one live connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SequenceExhausted;

impl fmt::Display for SequenceExhausted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("connection sequence space is exhausted")
    }
}

impl Error for SequenceExhausted {}

/// Inbound connection sequence did not progress exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SequenceError {
    /// A duplicate, skipped, or reordered sequence was observed.
    Unexpected {
        /// Exact sequence required next.
        expected: ConnectionSequence,
        /// Sequence carried by the received frame.
        actual: ConnectionSequence,
    },
    /// Sequence `u64::MAX` was already accepted on this connection.
    Exhausted,
}

impl fmt::Display for SequenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unexpected { expected, actual } => write!(
                formatter,
                "connection sequence {actual} arrived while {expected} was required"
            ),
            Self::Exhausted => formatter.write_str("connection sequence space is exhausted"),
        }
    }
}

impl Error for SequenceError {}

#[cfg(test)]
#[path = "sequence_test.rs"]
mod tests;
