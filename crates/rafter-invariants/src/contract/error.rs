//! Legacy contract-loading error shared by public catalog and registry APIs.

use std::fmt;

use super::registry::RegistryParseError;

/// Error reading or validating the invariant catalog and profile manifest.
#[derive(Debug)]
pub struct CatalogError(pub(crate) String);

impl fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CatalogError {}

impl From<RegistryParseError> for CatalogError {
    fn from(error: RegistryParseError) -> Self {
        Self(error.to_string())
    }
}
