//! The state-machine callback a preserved application error came from.
//!
//! A diagnostic vocabulary rather than a control-flow one: it names which
//! callback failed, so an operator and a metric label can say so while
//! the application's own error travels intact beside it.

use std::fmt;

/// State-machine operation that surfaced an application error.
///
/// This diagnostic vocabulary is `#[non_exhaustive]`: new state-machine
/// callbacks may add operations, and a caller that does not recognize one can
/// still preserve and report the underlying error without reclassifying it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StateMachineOperation {
    /// Reading the durable applied-index marker.
    AppliedIndex,
    /// Encoding an application command for the replicated log.
    EncodeCommand,
    /// Decoding an application command from the replicated log.
    DecodeCommand,
    /// Applying a committed batch to the application state machine.
    ApplyBatch,
    /// Reading application state.
    Read,
    /// Installing application snapshot bytes.
    InstallSnapshot,
}

impl fmt::Display for StateMachineOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AppliedIndex => "applied-index lookup",
            Self::EncodeCommand => "command encoding",
            Self::DecodeCommand => "command decoding",
            Self::ApplyBatch => "batch apply",
            Self::Read => "read",
            Self::InstallSnapshot => "snapshot install",
        })
    }
}
