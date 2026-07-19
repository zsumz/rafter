//! Scenarios: canonical registry rendering is deterministic and checked in.

use std::{fs, path::PathBuf};

use super::render_registry_markdown;
use crate::RegistryDocument;

#[test]
fn checked_in_document_matches_the_canonical_renderer() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let registry = RegistryDocument::load(&root.join("verification/raft-invariants.yaml"))
        .expect("parse registry");
    let checked_in =
        fs::read_to_string(root.join("docs/raft-invariants.md")).expect("read rendered document");

    assert_eq!(checked_in, render_registry_markdown(&registry));
}

#[test]
fn rendering_is_deterministic() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let registry = RegistryDocument::load(&root.join("verification/raft-invariants.yaml"))
        .expect("parse registry");

    assert_eq!(
        render_registry_markdown(&registry),
        render_registry_markdown(&registry)
    );
}
