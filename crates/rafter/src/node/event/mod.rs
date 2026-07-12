//! Typed vocabulary at the deterministic node boundary.
//!
//! This module owns node roles, accepted input events, ordered output effects,
//! and rejection reasons. It contains no transition logic: `Node::step` consumes
//! `Input`, protocol modules mutate state, and callers preserve the order of
//! returned `Output` values.

mod input;
mod output;
mod rejection;
mod role;

pub use input::{ClientProposalInput, Input};
pub use output::Output;
pub use rejection::{
    ConfigurationProposalRejection, LeadershipTransferRejection, LocalProposalDropReason,
    ProposalRejection, ReadIndexCancelReason, ReadIndexRejection,
};
pub use role::Role;
