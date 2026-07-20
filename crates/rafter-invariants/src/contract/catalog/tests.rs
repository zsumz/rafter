//! Scenarios: catalog normalization is explicit and rejects relational drift.

use std::path::PathBuf;

use super::Catalog;
use crate::RegistryDocument;

mod policy;

#[test]
fn registry_to_catalog_conversion_is_explicit_and_deterministic() {
    let registry = load_registry();
    let catalog = Catalog::try_from(registry.clone()).expect("normalize registry");
    let repeated = Catalog::try_from(registry).expect("normalize registry again");
    assert_eq!(catalog.ids, repeated.ids);
    assert_eq!(catalog.clauses, repeated.clauses);
    assert_eq!(catalog.evidence, repeated.evidence);
}

#[test]
fn relational_catalog_defects_are_not_parser_errors() {
    let mut registry = load_registry();
    registry.invariants[1].id = registry.invariants[0].id.clone();
    let error = Catalog::try_from(registry).expect_err("duplicate IDs fail normalization");
    assert_eq!(error.to_string(), "registry invariant IDs must be unique");
}

pub(super) fn load_registry() -> RegistryDocument {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    RegistryDocument::load(&root.join("verification/raft-invariants.yaml"))
        .expect("strictly parse registry")
}
