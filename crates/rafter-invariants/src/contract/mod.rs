//! Reviewed registry, catalog, profile, identity, and schema contracts.

pub(crate) mod catalog;
mod error;
mod identity;
pub(crate) mod profile;
pub(crate) mod registry;
pub(crate) mod schema;

pub use identity::{SimulatorIdentity, TestIdentity};
