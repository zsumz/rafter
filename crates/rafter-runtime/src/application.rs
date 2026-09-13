//! Bounded ordered execution for durable application state.
//!
//! Raft durability and application durability are separate fences. The runtime
//! can release an [`rafter::Output::Apply`] only after the consensus state that
//! produced it is durable; a client still must not be acknowledged until the
//! corresponding application update and applied floor are durable. This module
//! supplies the reusable one-owner worker for that second fence.
//!
//! [`crate::application::ApplicationWorker`] accepts only contiguous log indexes, accounts for
//! entries and retained bytes until completions are consumed, drains only work
//! that is already ready, and verifies the application's durable floor after
//! every successful batch. It owns no admission, reply, snapshot, or recovery
//! policy. An embedding must poison its live service on
//! [`crate::application::ApplicationEvent::Failed`] and reopen both Raft and
//! application state from their durable floors.

mod contract;
mod event;
mod submission;
mod worker;

pub use contract::{ApplicationEntry, ApplicationWorkerOptions, DurableApplication};
pub use event::{
    ApplicationCompletion, ApplicationEvent, ApplicationFailure, ApplicationFailureKind,
};
pub use submission::{ApplicationSubmitError, ApplicationSubmitRejection};
pub use worker::{ApplicationWorker, ApplicationWorkerShutdownError, ApplicationWorkerStopped};

#[cfg(test)]
mod tests;
