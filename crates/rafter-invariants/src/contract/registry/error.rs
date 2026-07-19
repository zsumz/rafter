//! Parse and I/O failures at the registry authoring boundary.

use std::fmt;

/// Error reading or strictly parsing an invariant registry document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryParseError(pub(crate) String);

impl fmt::Display for RegistryParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RegistryParseError {}
