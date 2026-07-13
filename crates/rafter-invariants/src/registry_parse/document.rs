use std::collections::BTreeMap;

use crate::catalog::CatalogError;
use crate::registry::{
    RegistryClause, RegistryCounts, RegistryDocument, RegistryEvidence, RegistryInvariant,
    REGISTRY_SCHEMA_VERSION,
};

use super::evidence::parse_evidence_record;
use super::syntax::{ensure_unique_record_ids, parse_bool, parse_section_records, required_field};
use super::top_level::{parse_top_level, parse_u32, required_count, required_top_level};

pub(crate) fn parse_registry_document(source: &str) -> Result<RegistryDocument, CatalogError> {
    let parsed_top_level = parse_top_level(source)?;
    let schema_version = parse_u32(required_top_level(
        &parsed_top_level.metadata,
        "schema_version",
    )?)?;
    if schema_version != REGISTRY_SCHEMA_VERSION {
        return Err(CatalogError(format!(
            "unsupported registry schema {schema_version}"
        )));
    }

    let invariant_records = parse_section_records(source, "invariants:")?;
    if invariant_records.is_empty() {
        return Err(CatalogError(
            "registry contains no invariant IDs".to_owned(),
        ));
    }
    ensure_unique_record_ids("invariant", &invariant_records)?;
    let invariants = invariant_records
        .into_iter()
        .enumerate()
        .map(|(index, record)| parse_registry_invariant(index, &record))
        .collect::<Result<Vec<_>, _>>()?;

    let clause_records = parse_section_records(source, "clauses:")?;
    ensure_unique_record_ids("clause", &clause_records)?;
    let clauses = clause_records
        .into_iter()
        .enumerate()
        .map(|(index, record)| parse_registry_clause(index, &record))
        .collect::<Result<Vec<_>, _>>()?;

    let evidence = parse_section_records(source, "evidence:")?
        .into_iter()
        .enumerate()
        .map(|(index, record)| parse_registry_evidence(index, &record))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(RegistryDocument {
        schema_version,
        repository: required_top_level(&parsed_top_level.metadata, "repository")?.to_owned(),
        catalog_origin_ref: required_top_level(&parsed_top_level.metadata, "catalog_origin_ref")?
            .to_owned(),
        catalog_working_ref: required_top_level(&parsed_top_level.metadata, "catalog_working_ref")?
            .to_owned(),
        audit_date: required_top_level(&parsed_top_level.metadata, "audit_date")?.to_owned(),
        document: required_top_level(&parsed_top_level.metadata, "document")?.to_owned(),
        scope: required_top_level(&parsed_top_level.metadata, "scope")?.to_owned(),
        counts: RegistryCounts {
            canonical_raft_safety_properties: required_count(
                &parsed_top_level.counts,
                "canonical_raft_safety_properties",
            )?,
            tla_predicates_now: required_count(&parsed_top_level.counts, "tla_predicates_now")?,
            well_formedness_meta_invariants: required_count(
                &parsed_top_level.counts,
                "well_formedness_meta_invariants",
            )?,
            semantic_safety_invariants: required_count(
                &parsed_top_level.counts,
                "semantic_safety_invariants",
            )?,
            liveness_obligations: required_count(&parsed_top_level.counts, "liveness_obligations")?,
            total_entries: required_count(&parsed_top_level.counts, "total_entries")?,
        },
        evidence,
        clauses,
        invariants,
    })
}

fn parse_registry_invariant(
    index: usize,
    record: &BTreeMap<String, String>,
) -> Result<RegistryInvariant, CatalogError> {
    let required = required_field("invariant", index, record);
    let current_coverage = ["tla", "simulator", "tests", "maelstrom"]
        .into_iter()
        .map(|layer| {
            let field = format!("current_coverage.{layer}");
            Ok((layer.to_owned(), required(&field)?))
        })
        .collect::<Result<_, CatalogError>>()?;
    Ok(RegistryInvariant {
        id: required("id")?,
        kind: required("kind")?,
        family: required("family")?,
        tier: required("tier")?,
        priority: required("priority")?,
        title: required("title")?,
        statement: required("statement")?,
        scope: required("scope")?,
        assumptions: required("assumptions")?,
        current_coverage,
        action_class: required("action_class")?,
        next_action: required("next_action")?,
    })
}

fn parse_registry_clause(
    index: usize,
    record: &BTreeMap<String, String>,
) -> Result<RegistryClause, CatalogError> {
    let required = required_field("clause", index, record);
    Ok(RegistryClause {
        id: required("id")?,
        invariant_id: required("invariant_id")?,
        statement: required("statement")?,
        scope: required("scope")?,
        assumptions: required("assumptions")?,
        required: parse_bool(&required("required")?)?,
    })
}

fn parse_registry_evidence(
    index: usize,
    record: &BTreeMap<String, String>,
) -> Result<RegistryEvidence, CatalogError> {
    let descriptors = parse_evidence_record(index, record)?;
    let Some(descriptor) = descriptors.first() else {
        return Err(CatalogError(format!(
            "evidence record {} has no clause bindings",
            index + 1
        )));
    };
    Ok(RegistryEvidence {
        id: descriptor.invariant_id.clone(),
        clauses: descriptors
            .iter()
            .map(|descriptor| descriptor.clause_id.clone())
            .collect(),
        layer: descriptor.layer.clone(),
        strength: descriptor.strength.clone(),
        path: descriptor.path.clone(),
        symbol: descriptor.symbol.clone(),
        atomic_group: descriptor.atomic_group.clone(),
        negative_fixture: descriptor.negative_fixture.clone(),
        negative_fixture_path: record.get("negative_fixture_path").cloned(),
        negative_fixture_detector: record.get("negative_fixture_detector").cloned(),
        negative_fixture_exemption: record.get("negative_fixture_exemption").cloned(),
        test: descriptor.test.clone(),
        simulator: descriptor.simulator.clone(),
    })
}
