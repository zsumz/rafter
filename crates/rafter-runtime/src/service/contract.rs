//! Admission, observation, and delivered-event contracts.

use crate::application::{
    ApplicationCompletion, ApplicationEntry, ApplicationFailure, ApplicationWorkerOptions,
    DurableApplication,
};
use crate::pipelined::PersistenceWorkerOptions;
use rafter::{ClientProposalInput, Input, Output, RaftSnapshot};

/// One admission attempt; there is no hidden input queue.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServiceInput {
    /// One ordinary input, executed through the synchronous durability fence.
    Consensus(Box<Input>),
    /// A proposal batch eligible for overlapped persistence and replication.
    Proposals(Vec<ClientProposalInput>),
}

impl From<Input> for ServiceInput {
    fn from(input: Input) -> Self {
        Self::Consensus(Box::new(input))
    }
}

/// Current admission and lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServiceState {
    /// Recovery outputs or their application completions remain outstanding.
    Recovering,
    /// Another input may be admitted; application work may still be running.
    Ready,
    /// Persistence or undispatched outputs currently prevent new input.
    Busy,
    /// The embedding owns the application store for snapshot installation.
    InstallingSnapshot,
    /// New input is closed; keep polling until shutdown can finish.
    Draining,
    /// Both workers were joined and the application store was returned.
    Stopped,
    /// A fatal failure requires recovery from durable storage.
    Failed,
}

/// Worker limits and proposal admission limits.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct ServiceOptions {
    /// Limits covering application work through completion consumption.
    pub application: ApplicationWorkerOptions,
    /// Persistence-thread telemetry options.
    pub persistence: PersistenceWorkerOptions,
    /// Maximum proposals in one explicit batch (must be nonzero).
    pub max_proposals: usize,
    /// Maximum total payload bytes in one proposal admission (must be nonzero).
    pub max_proposal_bytes: usize,
}

impl ServiceOptions {
    /// Returns bounded defaults: 64 proposals and 256 KiB per admission.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            application: ApplicationWorkerOptions::new(),
            persistence: PersistenceWorkerOptions::new(),
            max_proposals: 64,
            max_proposal_bytes: 256 * 1024,
        }
    }
}
impl Default for ServiceOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// One event transferred to the embedding, without an extra delivery queue.
///
/// Credits are released when this event is returned. The caller must bound its
/// own retained events, transport queues, client waiters, and authenticated
/// input frames. In particular, raw read IDs and membership inputs retain the
/// kernel's caller obligations; this is not the managed membership driver.
#[non_exhaustive]
pub enum ServiceEvent<T: ApplicationEntry, A: DurableApplication<T>> {
    /// Durability-safe control or peer output; never `Apply` or `ApplySnapshot`.
    /// A `ReadIndexGranted` has additionally crossed the application fence.
    Output(Output),
    /// Both command bytes and the corresponding application floor are durable.
    Applied(ApplicationCompletion<T, A::Outcome>),
    /// Terminal application failure with attempted and unattempted owned work.
    ApplicationFailed(ApplicationFailure<T, A::Error>),
    /// All earlier application completions were delivered. Install this exact
    /// descriptor's content in `application`, then call `resume_snapshot`.
    /// Admission and later output delivery remain closed until then.
    InstallSnapshot {
        /// Durable snapshot descriptor; content is in the runtime store.
        snapshot: RaftSnapshot,
        /// Quiescent application store, transferred exactly once.
        application: A,
    },
}

impl<T: ApplicationEntry, A: DurableApplication<T>> std::fmt::Debug for ServiceEvent<T, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Output(output) => f.debug_tuple("Output").field(output).finish(),
            Self::Applied(done) => f
                .debug_struct("Applied")
                .field("entries", &done.entries().len())
                .finish(),
            Self::ApplicationFailed(failure) => f
                .debug_struct("ApplicationFailed")
                .field("kind", failure.kind())
                .finish_non_exhaustive(),
            Self::InstallSnapshot { snapshot, .. } => f
                .debug_struct("InstallSnapshot")
                .field("snapshot", snapshot)
                .finish_non_exhaustive(),
        }
    }
}
