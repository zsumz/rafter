//! Neutral filesystem and test mechanisms for independent reference systems.
//!
//! This crate owns a bounded ordering search over already-parsed operation
//! intervals. Callers retain their event schemas, parsing, state transitions,
//! observations, and diagnostic vocabulary. It also provisions directories with
//! explicit synchronization ordering for durable consumers. The crate is deliberately
//! unpublished and has no dependencies.

#![forbid(unsafe_code)]

pub mod durable_directory;
mod operation;
mod outcome;
pub mod process;
mod search;
mod specification;

pub use operation::{Operation, OperationId};
pub use outcome::{Candidate, CandidateReason, SearchError, SearchFrontier, SearchReport};
pub use search::{search, SearchLimits};
pub use specification::{SequentialSpec, Step};
