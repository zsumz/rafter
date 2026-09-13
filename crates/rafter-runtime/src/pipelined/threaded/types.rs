//! Public results and errors for the threaded persistence composition.

use super::super::{PersistenceWorkerStopped, PersistenceWorkerTelemetry, PipelineError};
use rafter::Output;
use std::{error::Error, fmt, io};

/// How one proposal batch crossed its local durability fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ThreadedPersistenceDisposition {
    /// The runtime completed persistence before returning any output.
    Durable,
    /// Eligible replication outputs escaped while one worker operation remains pending.
    Pending,
    /// Worker submission was unavailable, so the same work completed on the owner thread.
    InlineFallback,
}

/// Outputs from one proposal batch and the durability path that produced them.
#[derive(Debug)]
pub struct ThreadedProposalOutputs {
    pub(super) outputs: Vec<Output>,
    pub(super) persistence: ThreadedPersistenceDisposition,
}

impl ThreadedProposalOutputs {
    /// Returns the outputs currently safe to execute.
    #[must_use]
    pub fn outputs(&self) -> &[Output] {
        &self.outputs
    }

    /// Returns whether persistence completed or remains with the worker.
    #[must_use]
    pub const fn persistence(&self) -> ThreadedPersistenceDisposition {
        self.persistence
    }

    /// Consumes the result and returns its safe outputs.
    #[must_use]
    pub fn into_outputs(self) -> Vec<Output> {
        self.outputs
    }
}

/// Dependent outputs released by one accepted worker completion.
#[derive(Debug)]
pub struct ThreadedPersistenceCompletion {
    pub(super) outputs: Vec<Output>,
    pub(super) storage_telemetry: PersistenceWorkerTelemetry,
}

impl ThreadedPersistenceCompletion {
    /// Returns outputs released only after the local persistence operation completed.
    #[must_use]
    pub fn outputs(&self) -> &[Output] {
        &self.outputs
    }

    /// Returns cumulative diagnostics collected on the persistence thread.
    #[must_use]
    pub fn storage_telemetry(&self) -> &[(&'static str, rafter_storage::telemetry::Metric)] {
        &self.storage_telemetry
    }

    /// Consumes the completion and returns its durability-released outputs.
    #[must_use]
    pub fn into_outputs(self) -> Vec<Output> {
        self.outputs
    }

    /// Splits the safe outputs from cumulative worker-thread diagnostics.
    #[must_use]
    pub fn into_parts(self) -> (Vec<Output>, PersistenceWorkerTelemetry) {
        (self.outputs, self.storage_telemetry)
    }
}

/// A runtime or persistence-worker failure from the threaded composition.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ThreadedPipelineError {
    /// The owned pipelined runtime refused or failed an operation.
    Pipeline(PipelineError),
    /// The worker ended without returning the outstanding node.
    WorkerStopped(PersistenceWorkerStopped),
}

impl fmt::Display for ThreadedPipelineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pipeline(error) => error.fmt(formatter),
            Self::WorkerStopped(error) => error.fmt(formatter),
        }
    }
}

impl Error for ThreadedPipelineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Pipeline(error) => Some(error),
            Self::WorkerStopped(error) => Some(error),
        }
    }
}

impl From<PipelineError> for ThreadedPipelineError {
    fn from(error: PipelineError) -> Self {
        Self::Pipeline(error)
    }
}

impl From<PersistenceWorkerStopped> for ThreadedPipelineError {
    fn from(error: PersistenceWorkerStopped) -> Self {
        Self::WorkerStopped(error)
    }
}

/// Thread-start failure that returns ownership of the unopened composition.
pub struct ThreadedPipelineStartError<H, L, S> {
    pub(super) source: io::Error,
    pub(super) node: Box<crate::DurableRaftNode<H, L, S>>,
}

impl<H, L, S> ThreadedPipelineStartError<H, L, S> {
    /// Returns the operating-system thread creation failure.
    #[must_use]
    pub const fn source_error(&self) -> &io::Error {
        &self.source
    }

    /// Returns both the thread creation failure and the unchanged durable node.
    #[must_use]
    pub fn into_parts(self) -> (io::Error, crate::DurableRaftNode<H, L, S>) {
        (self.source, *self.node)
    }
}

impl<H, L, S> fmt::Debug for ThreadedPipelineStartError<H, L, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ThreadedPipelineStartError")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl<H, L, S> fmt::Display for ThreadedPipelineStartError<H, L, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "failed to start Raft persistence worker: {}",
            self.source
        )
    }
}

impl<H, L, S> Error for ThreadedPipelineStartError<H, L, S> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}
