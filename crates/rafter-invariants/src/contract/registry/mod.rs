//! Strict authoring model, parser, and canonical rendering for the registry.

mod error;
mod load;
mod model;
mod parse;
mod render;

pub use error::RegistryParseError;
pub use model::{
    RegistryClause, RegistryCounts, RegistryDocument, RegistryEvidence, RegistryInvariant,
    REGISTRY_SCHEMA_VERSION,
};
pub use render::render_registry_markdown;
