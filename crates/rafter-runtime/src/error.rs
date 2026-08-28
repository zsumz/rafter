//! Errors returned by the durable runtime.
//!
//! Two types, split by one question: did the in-memory kernel run ahead of the
//! durable medium? [`RaftRuntimeError`] is everything a caller can be told;
//! [`RaftRuntimeFatalError`] is the subset that poisons the runtime, and
//! [`RaftRuntimeFatalError::from_runtime_error`] is the rule that decides.

mod display;
mod fatal;
mod runtime;

pub use fatal::RaftRuntimeFatalError;
pub use runtime::RaftRuntimeError;
