//! File loading and strict parser entry points for registry documents.

use std::{fs, path::Path};

use super::{parse::parse_registry_document, RegistryDocument, RegistryParseError};
use crate::contract::error::CatalogError;

impl RegistryDocument {
    /// Parses the complete registry using the canonical strict Rust parser.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported schema versions, unknown fields,
    /// malformed syntax, missing fields, or invalid typed values.
    pub fn parse(source: &str) -> Result<Self, CatalogError> {
        Self::parse_strict(source).map_err(Into::into)
    }

    /// Parses with the registry parser's domain-specific error type.
    ///
    /// # Errors
    ///
    /// Returns a registry syntax error before catalog normalization begins.
    pub fn parse_strict(source: &str) -> Result<Self, RegistryParseError> {
        parse_registry_document(source)
    }

    /// Loads and strictly parses a registry file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or does not satisfy the
    /// complete registry schema.
    pub fn load(path: &Path) -> Result<Self, CatalogError> {
        Self::load_strict(path).map_err(Into::into)
    }

    /// Loads with the registry parser's domain-specific error type.
    ///
    /// # Errors
    ///
    /// Returns a registry I/O or syntax error without catalog normalization.
    pub fn load_strict(path: &Path) -> Result<Self, RegistryParseError> {
        let source = fs::read_to_string(path)
            .map_err(|error| RegistryParseError(format!("read {}: {error}", path.display())))?;
        Self::parse_strict(&source)
    }
}
