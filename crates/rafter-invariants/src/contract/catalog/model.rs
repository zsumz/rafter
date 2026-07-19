//! Normalized invariant, clause, and evidence descriptors.

use std::collections::{BTreeMap, BTreeSet};

use crate::contract::{profile::ProfileContract, SimulatorIdentity, TestIdentity};

/// Reviewed invariant IDs and their declared executable evidence.
#[derive(Clone, Debug)]
pub struct Catalog {
    pub ids: Vec<String>,
    pub invariants: Vec<InvariantDescriptor>,
    pub canonical_ids: BTreeSet<String>,
    pub clauses: Vec<ClauseDescriptor>,
    pub evidence: Vec<EvidenceDescriptor>,
}

/// One reviewed parent invariant and its documented verification boundary.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InvariantDescriptor {
    pub id: String,
    pub statement: String,
    pub scope: String,
    pub assumptions: String,
}

/// One stable, atomic normative obligation owned by a parent invariant.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ClauseDescriptor {
    pub invariant_id: String,
    pub clause_id: String,
    pub statement: String,
    pub scope: String,
    pub assumptions: String,
    pub required: bool,
}

/// One direct or end-to-end evidence declaration from the registry.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct EvidenceDescriptor {
    pub invariant_id: String,
    pub clause_id: String,
    pub layer: String,
    pub strength: String,
    pub path: String,
    pub symbol: String,
    pub atomic_group: Option<String>,
    pub negative_fixture: Option<String>,
    pub negative_fixture_path: Option<String>,
    pub negative_fixture_detector: Option<String>,
    pub test: Option<TestIdentity>,
    pub simulator: Option<SimulatorIdentity>,
}

impl EvidenceDescriptor {
    /// Returns the stable aggregate key for this evidence declaration.
    #[must_use]
    pub fn evidence_id(&self) -> String {
        let base = format!(
            "{}/{}/{}/{}/{}#{}",
            self.invariant_id, self.clause_id, self.layer, self.strength, self.path, self.symbol
        );
        let grouped = self
            .atomic_group
            .as_ref()
            .map_or(base.clone(), |group| format!("{base}@atomic={group}"));
        self.negative_fixture
            .as_ref()
            .map_or(grouped.clone(), |fixture| format!("{grouped}@{fixture}"))
    }
}

impl Catalog {
    /// Returns the ordered normative clauses owned by one parent invariant.
    #[must_use]
    pub fn clauses_for(&self, invariant_id: &str) -> Vec<ClauseDescriptor> {
        self.clauses
            .iter()
            .filter(|clause| clause.invariant_id == invariant_id)
            .cloned()
            .collect()
    }

    /// Selects and deduplicates registry evidence required by a profile.
    #[must_use]
    pub fn required_evidence(
        &self,
        contract: &ProfileContract,
    ) -> BTreeMap<String, Vec<EvidenceDescriptor>> {
        let layers = contract.required_layers.iter().collect::<BTreeSet<_>>();
        let strengths = contract.required_strengths.iter().collect::<BTreeSet<_>>();
        let mut required = self
            .ids
            .iter()
            .cloned()
            .map(|id| (id, Vec::new()))
            .collect::<BTreeMap<_, _>>();
        let mut deduplicated = BTreeSet::new();
        for evidence in &self.evidence {
            if !layers.contains(&evidence.layer) || !strengths.contains(&evidence.strength) {
                continue;
            }
            if deduplicated.insert(evidence.clone()) {
                required
                    .entry(evidence.invariant_id.clone())
                    .or_default()
                    .push(evidence.clone());
            }
        }
        required
    }
}
