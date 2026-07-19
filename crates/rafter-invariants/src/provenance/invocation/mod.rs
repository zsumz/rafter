//! Invocation identity derived from validated process inputs.

mod environment;
mod identity;

pub(crate) use environment::{digest_environment, environment_matches_digest};
pub(crate) use identity::deterministic_u64;
