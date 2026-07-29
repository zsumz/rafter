//! Neutral test mechanisms proven by multiple reference systems.
//!
//! This crate owns a bounded ordering search over already-parsed operation
//! intervals. Callers retain their event schemas, parsing, state transitions,
//! observations, and diagnostic vocabulary. The crate is deliberately
//! unpublished and has no dependencies.

#![forbid(unsafe_code)]

mod operation;
mod outcome;
mod search;
mod specification;

pub use operation::{Operation, OperationId};
pub use outcome::{Candidate, CandidateReason, SearchError, SearchFrontier, SearchReport};
pub use search::{search, SearchLimits};
pub use specification::{SequentialSpec, Step};
