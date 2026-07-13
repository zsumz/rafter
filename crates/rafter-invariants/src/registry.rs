use std::{collections::BTreeMap, fs, path::Path};

use crate::{
    catalog::{
        CatalogError, ClauseDescriptor, EvidenceDescriptor, InvariantDescriptor, SimulatorIdentity,
        TestIdentity,
    },
    registry_parse::parse_registry_document,
};

/// Registry schema understood by this version of the deterministic verifier.
pub const REGISTRY_SCHEMA_VERSION: u32 = 3;

/// The complete, strictly parsed Raft invariant registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryDocument {
    pub schema_version: u32,
    pub repository: String,
    pub catalog_origin_ref: String,
    pub catalog_working_ref: String,
    pub audit_date: String,
    pub document: String,
    pub scope: String,
    pub counts: RegistryCounts,
    pub evidence: Vec<RegistryEvidence>,
    pub clauses: Vec<RegistryClause>,
    pub invariants: Vec<RegistryInvariant>,
}

/// Reviewed catalog counts declared by the registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryCounts {
    pub canonical_raft_safety_properties: usize,
    pub tla_predicates_now: usize,
    pub well_formedness_meta_invariants: usize,
    pub semantic_safety_invariants: usize,
    pub liveness_obligations: usize,
    pub total_entries: usize,
}

/// One fully documented parent invariant, including its layer coverage labels.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryInvariant {
    pub id: String,
    pub kind: String,
    pub family: String,
    pub tier: String,
    pub priority: String,
    pub title: String,
    pub statement: String,
    pub scope: String,
    pub assumptions: String,
    pub current_coverage: BTreeMap<String, String>,
    pub action_class: String,
    pub next_action: String,
}

/// One atomic normative clause owned by a parent invariant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryClause {
    pub id: String,
    pub invariant_id: String,
    pub statement: String,
    pub scope: String,
    pub assumptions: String,
    pub required: bool,
}

/// One registry evidence row before clause expansion for aggregation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryEvidence {
    pub id: String,
    pub clauses: Vec<String>,
    pub layer: String,
    pub strength: String,
    pub path: String,
    pub symbol: String,
    pub atomic_group: Option<String>,
    pub negative_fixture: Option<String>,
    pub negative_fixture_path: Option<String>,
    pub negative_fixture_detector: Option<String>,
    pub negative_fixture_exemption: Option<String>,
    pub test: Option<TestIdentity>,
    pub simulator: Option<SimulatorIdentity>,
}

impl RegistryDocument {
    /// Parses the complete registry using the canonical strict Rust parser.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported schema versions, unknown fields,
    /// malformed syntax, missing fields, or invalid typed values.
    pub fn parse(source: &str) -> Result<Self, CatalogError> {
        parse_registry_document(source)
    }

    /// Loads and strictly parses a registry file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or does not satisfy the
    /// complete registry schema.
    pub fn load(path: &Path) -> Result<Self, CatalogError> {
        let source = fs::read_to_string(path)
            .map_err(|error| CatalogError(format!("read {}: {error}", path.display())))?;
        Self::parse(&source)
    }

    pub(crate) fn invariant_descriptors(&self) -> Vec<InvariantDescriptor> {
        self.invariants
            .iter()
            .map(|invariant| InvariantDescriptor {
                id: invariant.id.clone(),
                statement: invariant.statement.clone(),
                scope: invariant.scope.clone(),
                assumptions: invariant.assumptions.clone(),
            })
            .collect()
    }

    pub(crate) fn clause_descriptors(&self) -> Vec<ClauseDescriptor> {
        self.clauses
            .iter()
            .map(|clause| ClauseDescriptor {
                invariant_id: clause.invariant_id.clone(),
                clause_id: clause.id.clone(),
                statement: clause.statement.clone(),
                scope: clause.scope.clone(),
                assumptions: clause.assumptions.clone(),
                required: clause.required,
            })
            .collect()
    }

    pub(crate) fn evidence_descriptors(&self) -> Vec<EvidenceDescriptor> {
        self.evidence
            .iter()
            .flat_map(|evidence| {
                evidence.clauses.iter().map(|clause_id| EvidenceDescriptor {
                    invariant_id: evidence.id.clone(),
                    clause_id: clause_id.clone(),
                    layer: evidence.layer.clone(),
                    strength: evidence.strength.clone(),
                    path: evidence.path.clone(),
                    symbol: evidence.symbol.clone(),
                    atomic_group: evidence.atomic_group.clone(),
                    negative_fixture: evidence.negative_fixture.clone(),
                    test: evidence.test.clone(),
                    simulator: evidence.simulator.clone(),
                })
            })
            .collect()
    }
}
