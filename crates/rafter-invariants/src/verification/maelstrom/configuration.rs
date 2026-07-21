//! Access to the authenticated Maelstrom runner configuration.

use crate::{evidence::ResultBundle, verification::AggregateError};

pub(super) fn value<'a>(bundle: &'a ResultBundle, key: &str) -> Result<&'a str, AggregateError> {
    bundle
        .execution
        .plan
        .contract
        .runners
        .get(&bundle.runner)
        .ok_or_else(|| error(format!("execution plan omitted runner {}", bundle.runner)))?
        .configuration
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| error(format!("Maelstrom configuration omitted {key}")))
}

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}
