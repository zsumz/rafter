//! Strict profile-manifest loading.

use std::{fs, path::Path};

use super::ProfileManifest;
use crate::contract::error::CatalogError;

impl ProfileManifest {
    /// Loads explicit PR, nightly, and weekly evidence policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or is not valid strict
    /// profile-manifest JSON.
    pub fn load(path: &Path) -> Result<Self, CatalogError> {
        let source = fs::read_to_string(path)
            .map_err(|error| CatalogError(format!("read {}: {error}", path.display())))?;
        serde_json::from_str(&source)
            .map_err(|error| CatalogError(format!("parse {}: {error}", path.display())))
    }
}
