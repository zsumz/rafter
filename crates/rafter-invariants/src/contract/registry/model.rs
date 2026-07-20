//! Authoring model for the reviewed invariant registry.

use std::collections::BTreeMap;

use crate::contract::{SimulatorIdentity, TestIdentity};

/// Registry schema understood by this version of the deterministic verifier.
pub const REGISTRY_SCHEMA_VERSION: u32 = 4;

/// Runtime or storage behavior exercised by persistence evidence.
///
/// This enum is exhaustive so registry consumers must handle every reviewed
/// persistence-evidence category explicitly.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PersistenceEvidenceKind {
    /// Reconstructs state after an explicit crash or reopen boundary.
    CrashReopen,
    /// Injects a durable-operation failure and checks fail-closed behavior.
    FailureInjection,
}

impl PersistenceEvidenceKind {
    /// Returns the stable registry wire name.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::CrashReopen => "crash_reopen",
            Self::FailureInjection => "failure_injection",
        }
    }
}

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
    pub persistence_evidence: Option<PersistenceEvidenceKind>,
    pub atomic_group: Option<String>,
    pub negative_fixture: Option<String>,
    pub negative_fixture_path: Option<String>,
    pub negative_fixture_detector: Option<String>,
    pub negative_fixture_exemption: Option<String>,
    pub test: Option<TestIdentity>,
    pub simulator: Option<SimulatorIdentity>,
}
