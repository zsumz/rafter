//! Registry-to-catalog normalization and cross-record validation.

use std::{collections::BTreeSet, path::Path};

use super::{Catalog, CatalogError, ClauseDescriptor, EvidenceDescriptor, InvariantDescriptor};
use crate::contract::registry::{RegistryDocument, REGISTRY_SCHEMA_VERSION};

impl Catalog {
    /// Loads and normalizes the reviewed registry.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry cannot be read, parsed, or normalized
    /// into a unique and internally consistent executable catalog.
    pub fn load(path: &Path) -> Result<Self, CatalogError> {
        RegistryDocument::load(path)?.try_into()
    }
}

impl TryFrom<RegistryDocument> for Catalog {
    type Error = CatalogError;

    fn try_from(registry: RegistryDocument) -> Result<Self, Self::Error> {
        if registry.schema_version != REGISTRY_SCHEMA_VERSION {
            return Err(CatalogError(format!(
                "unsupported registry schema {}",
                registry.schema_version
            )));
        }
        let invariants = registry
            .invariants
            .iter()
            .map(|invariant| InvariantDescriptor {
                id: invariant.id.clone(),
                statement: invariant.statement.clone(),
                scope: invariant.scope.clone(),
                assumptions: invariant.assumptions.clone(),
            })
            .collect::<Vec<_>>();
        let canonical_ids = registry
            .invariants
            .iter()
            .filter(|invariant| invariant.tier == "canonical")
            .map(|invariant| invariant.id.clone())
            .collect();
        let ids = invariants
            .iter()
            .map(|invariant| invariant.id.clone())
            .collect::<Vec<_>>();
        let unique_ids = ids.iter().collect::<BTreeSet<_>>();
        if unique_ids.len() != ids.len() {
            return Err(CatalogError(
                "registry invariant IDs must be unique".to_owned(),
            ));
        }
        super::policy::validate(&registry.invariants, &registry.evidence)?;

        let clauses = registry
            .clauses
            .iter()
            .map(|clause| ClauseDescriptor {
                invariant_id: clause.invariant_id.clone(),
                clause_id: clause.id.clone(),
                statement: clause.statement.clone(),
                scope: clause.scope.clone(),
                assumptions: clause.assumptions.clone(),
                required: clause.required,
            })
            .collect::<Vec<_>>();
        validate_clauses(&ids, &unique_ids, &clauses)?;

        let evidence = registry
            .evidence
            .iter()
            .flat_map(|evidence| {
                evidence.clauses.iter().map(|clause_id| EvidenceDescriptor {
                    invariant_id: evidence.id.clone(),
                    clause_id: clause_id.clone(),
                    layer: evidence.layer.clone(),
                    strength: evidence.strength.clone(),
                    path: evidence.path.clone(),
                    symbol: evidence.symbol.clone(),
                    persistence_evidence: evidence.persistence_evidence,
                    atomic_group: evidence.atomic_group.clone(),
                    negative_fixture: evidence.negative_fixture.clone(),
                    negative_fixture_path: evidence.negative_fixture_path.clone(),
                    negative_fixture_detector: evidence.negative_fixture_detector.clone(),
                    negative_fixture_detector_path: evidence.negative_fixture_detector_path.clone(),
                    test: evidence.test.clone(),
                    simulator: evidence.simulator.clone(),
                })
            })
            .collect::<Vec<_>>();
        validate_evidence(&clauses, &evidence)?;

        Ok(Self {
            ids,
            invariants,
            canonical_ids,
            clauses,
            evidence,
        })
    }
}

fn validate_clauses(
    ids: &[String],
    unique_ids: &BTreeSet<&String>,
    clauses: &[ClauseDescriptor],
) -> Result<(), CatalogError> {
    let clause_ids = clauses
        .iter()
        .map(|clause| clause.clause_id.as_str())
        .collect::<BTreeSet<_>>();
    if clause_ids.len() != clauses.len() {
        return Err(CatalogError(
            "registry clause IDs must be globally unique".to_owned(),
        ));
    }
    for clause in clauses {
        if !unique_ids.contains(&clause.invariant_id) {
            return Err(CatalogError(format!(
                "clause {} refers to unknown invariant {}",
                clause.clause_id, clause.invariant_id
            )));
        }
    }
    for invariant_id in ids {
        if !clauses
            .iter()
            .any(|clause| clause.invariant_id == *invariant_id && clause.required)
        {
            return Err(CatalogError(format!(
                "invariant {invariant_id} has no required normative clauses"
            )));
        }
    }
    Ok(())
}

fn validate_evidence(
    clauses: &[ClauseDescriptor],
    evidence: &[EvidenceDescriptor],
) -> Result<(), CatalogError> {
    for descriptor in evidence {
        let Some(clause) = clauses
            .iter()
            .find(|clause| clause.clause_id == descriptor.clause_id)
        else {
            return Err(CatalogError(format!(
                "evidence for {} refers to unknown clause {}",
                descriptor.invariant_id, descriptor.clause_id
            )));
        };
        if clause.invariant_id != descriptor.invariant_id {
            return Err(CatalogError(format!(
                "evidence parent {} does not own clause {}",
                descriptor.invariant_id, descriptor.clause_id
            )));
        }
    }
    let evidence_ids = evidence
        .iter()
        .map(EvidenceDescriptor::evidence_id)
        .collect::<BTreeSet<_>>();
    if evidence_ids.len() != evidence.len() {
        return Err(CatalogError(
            "registry evidence declarations must have unique identities".to_owned(),
        ));
    }
    Ok(())
}
