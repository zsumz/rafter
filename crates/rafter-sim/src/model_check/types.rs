//! The vocabulary model-check results are reported in.
//!
//! Bounds, actions, failures, state summaries, and completion status are the
//! whole public shape of a check, so a caller can grade a run without reaching
//! into exploration internals. These types describe results; they run nothing.

mod bounds;
mod failure;
mod state;
mod summary;
mod trace;

pub use bounds::Bounds;
pub use failure::{Failure, FailureKind};
pub use state::{NodeSummary, StateSummary};
pub use summary::{ExplorationCompletion, Summary};
pub use trace::{Action, EnvelopeIdentity, MessageKind, ProposalId};
