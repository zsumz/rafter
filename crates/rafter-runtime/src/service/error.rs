//! Owned admission refusals and terminal service failures.

use super::{ServiceInput, ServiceState};
use crate::application::{ApplicationSubmitRejection, ApplicationWorkerShutdownError};
use crate::pipelined::{PersistenceWorkerShutdownError, ThreadedPipelineError};
use crate::RaftRuntimeError;
use rafter::LogIndex;
use std::{error::Error, fmt, io};

/// An embedding's command decode or conversion failure.
pub type ServiceAdapterError = Box<dyn Error + Send + Sync>;

/// A lifecycle refusal or fatal error from the durable composition.
#[derive(Debug)]
#[non_exhaustive]
pub enum ServiceError {
    /// Storage recovery failed before admission opened.
    Recovery(RaftRuntimeError),
    /// A consensus or persistence operation failed; recovery is required.
    Persistence(ThreadedPipelineError),
    /// Worker construction failed; reopen the durable stores before retrying.
    Start(io::Error),
    /// Application submission cannot make progress under its configured limits.
    ApplicationAdmission(ApplicationSubmitRejection),
    /// The application worker stopped without a valid completion.
    ApplicationStopped,
    /// Application adapter changed the command's required range.
    AdapterRange,
    /// Committed command conversion failed; recovery is required.
    Adapter(ServiceAdapterError),
    /// Snapshot construction failed; the application must recover before reuse.
    SnapshotBuild(ServiceAdapterError),
    /// Local snapshot validation or publication failed. Boundary validation is
    /// nonfatal; publication/compaction failures fence the service for recovery.
    Snapshot(RaftRuntimeError),
    /// Snapshot metadata does not name the quiescent application floor. Nothing
    /// was published, and the service remains usable.
    SnapshotFloor {
        /// Exact durable application floor the snapshot must represent.
        expected: LogIndex,
        /// Boundary returned by the snapshot builder.
        actual: LogIndex,
    },
    /// Application work or unconsumed completions prevent local maintenance.
    NotQuiescent,
    /// Application floor does not agree with the recovery or snapshot boundary.
    ApplicationFloor {
        /// Maximum recovery floor or exact installed snapshot floor.
        expected: LogIndex,
        /// Floor reported by the application store.
        actual: LogIndex,
    },
    /// The ordered output stream contained a non-increasing application index.
    OutputOrder,
    /// Lifecycle operation was refused in this state, without starting work.
    NotReady(ServiceState),
    /// Joining an idle application thread failed.
    ApplicationShutdown(ApplicationWorkerShutdownError),
    /// Joining the idle persistence thread failed.
    PersistenceShutdown(PersistenceWorkerShutdownError),
    /// A prior fatal failure already fenced this incarnation.
    Failed,
}
impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "durable service: {self:?}")
    }
}
impl Error for ServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Recovery(error) | Self::Snapshot(error) => Some(error),
            Self::Persistence(error) => Some(error),
            Self::Start(error) => Some(error),
            Self::Adapter(error) | Self::SnapshotBuild(error) => Some(error.as_ref()),
            Self::ApplicationShutdown(error) => Some(error),
            Self::PersistenceShutdown(error) => Some(error),
            _ => None,
        }
    }
}

/// Input either remained untouched, or was consumed by a failed durable step.
#[derive(Debug)]
#[non_exhaustive]
pub enum ServiceStepError {
    /// Retry later with the exact returned input. No consensus step ran.
    Unavailable {
        /// Current lifecycle or admission state.
        state: ServiceState,
        /// Unconsumed input.
        input: ServiceInput,
    },
    /// Empty or oversized proposal admission; no consensus step ran.
    ProposalLimit {
        /// Unconsumed input.
        input: ServiceInput,
    },
    /// Input may have changed durable state; do not blindly retry a write.
    Fatal(ServiceError),
}
impl fmt::Display for ServiceStepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "durable service admission: {self:?}")
    }
}
impl Error for ServiceStepError {}
