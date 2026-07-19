//! Fail-closed evidence-verification errors.

use std::fmt;

#[derive(Debug)]
pub(crate) struct AggregateError(String);

impl AggregateError {
    pub(crate) const fn new(message: String) -> Self {
        Self(message)
    }
}

impl fmt::Display for AggregateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for AggregateError {}
