use std::collections::BTreeSet;

use crate::catalog::{CatalogError, ClauseDescriptor, EvidenceDescriptor, InvariantDescriptor};

mod document;
mod evidence;
mod fields;
mod simulator;
mod syntax;
mod top_level;

pub(crate) use document::parse_registry_document;

pub(super) fn parse_registry_schema_version(source: &str) -> Result<u32, CatalogError> {
    Ok(parse_registry_document(source)?.schema_version)
}

pub(super) fn parse_invariants(
    source: &str,
) -> Result<(Vec<InvariantDescriptor>, BTreeSet<String>), CatalogError> {
    let document = parse_registry_document(source)?;
    let canonical_ids = document
        .invariants
        .iter()
        .filter(|invariant| invariant.tier == "canonical")
        .map(|invariant| invariant.id.clone())
        .collect();
    Ok((document.invariant_descriptors(), canonical_ids))
}

pub(super) fn parse_clauses(source: &str) -> Result<Vec<ClauseDescriptor>, CatalogError> {
    Ok(parse_registry_document(source)?.clause_descriptors())
}

pub(super) fn parse_evidence(source: &str) -> Result<Vec<EvidenceDescriptor>, CatalogError> {
    Ok(parse_registry_document(source)?.evidence_descriptors())
}

#[cfg(test)]
mod registry_parse_test_fixtures;

#[cfg(test)]
mod tests;
