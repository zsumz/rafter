//! Invocation identity derived from validated process inputs.

mod environment;

pub(crate) use environment::{digest_environment, environment_matches_digest};
