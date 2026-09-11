//! Refusals preserve the distinction between busy ownership and fatal storage errors.
use crate::RaftRuntimeError;
use std::{error::Error, fmt};

/// Errors from the single-operation persistence driver.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PipelineError {
    /// An outstanding write owns the node; retain the input until completion.
    PersistencePending,
    /// A completion does not match this driver's pending generation/operation.
    UnexpectedCompletion,
    /// The operation counter cannot advance without reusing an identity.
    OperationExhausted,
    /// The synchronous durability fence failed; its runtime remains poisoned.
    Runtime(RaftRuntimeError),
}
impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PersistencePending => f.write_str("Raft persistence is pending"),
            Self::UnexpectedCompletion => f.write_str("unexpected Raft persistence completion"),
            Self::OperationExhausted => {
                f.write_str("Raft persistence operation identity exhausted")
            }
            Self::Runtime(error) => error.fmt(f),
        }
    }
}
impl Error for PipelineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            _ => None,
        }
    }
}
impl From<RaftRuntimeError> for PipelineError {
    fn from(error: RaftRuntimeError) -> Self {
        Self::Runtime(error)
    }
}
