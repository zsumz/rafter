use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::{
    registry::{RegistryDocument, REGISTRY_SCHEMA_VERSION},
    types::SimulatorLivenessContract,
};

mod liveness;
mod liveness_validation;
mod runner_contract;
mod simulator_contract;

pub(crate) use liveness::{
    derive_liveness_binding, execution_contract_digest, expected_execution_contract,
    liveness_contract_digest, liveness_reports_digest,
};
pub(crate) use simulator_contract::{
    per_check_observation_key, per_check_protocol_states_key, per_check_verifier_states_key,
    SimulatorRunnerConfiguration, SimulatorStateFloors,
};

#[cfg(test)]
pub(crate) mod liveness_report_tests;

const PROFILE_SCHEMA_VERSION: u32 = 5;

#[derive(Clone, Debug)]
/// Reviewed invariant IDs and their declared executable evidence.
pub struct Catalog {
    pub ids: Vec<String>,
    pub invariants: Vec<InvariantDescriptor>,
    pub canonical_ids: BTreeSet<String>,
    pub clauses: Vec<ClauseDescriptor>,
    pub evidence: Vec<EvidenceDescriptor>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// One reviewed parent invariant and its documented verification boundary.
pub struct InvariantDescriptor {
    pub id: String,
    pub statement: String,
    pub scope: String,
    pub assumptions: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// One stable, atomic normative obligation owned by a parent invariant.
pub struct ClauseDescriptor {
    pub invariant_id: String,
    pub clause_id: String,
    pub statement: String,
    pub scope: String,
    pub assumptions: String,
    pub required: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// One direct or end-to-end evidence declaration from the registry.
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// Exact Cargo target and libtest identity for tests-layer evidence.
pub struct TestIdentity {
    pub package: String,
    pub target_kind: String,
    pub target: String,
    pub test_name: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// Exact simulator legs, coverage floors, and detector qualification.
pub struct SimulatorIdentity {
    pub checks: Vec<String>,
    pub required_observation: String,
    pub minimum_observation: usize,
    pub minimum_protocol_states: Option<usize>,
    pub minimum_verifier_states: Option<usize>,
    pub minimum_runs_per_check: Option<usize>,
    pub minimum_steps: Option<usize>,
    pub liveness_report: Option<SimulatorLivenessContract>,
    pub negative_test: Option<TestIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LivenessReportErrorKind {
    Missing,
    Malformed,
}

#[derive(Debug)]
pub(crate) struct LivenessReportError {
    pub kind: LivenessReportErrorKind,
    pub message: String,
}

impl TestIdentity {
    /// Returns the stable check identity required in a tests-layer receipt.
    #[must_use]
    pub fn check_id(&self) -> String {
        format!(
            "tests/{}/{}/{}#{}",
            self.package, self.target_kind, self.target, self.test_name
        )
    }
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Explicit invariant IDs and evidence policies for every scheduled profile.
pub struct ProfileManifest {
    pub schema_version: u32,
    pub reviewed_ids: Vec<String>,
    pub profiles: BTreeMap<String, ProfileContract>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Evidence-selection and independent-layer policy for one profile.
pub struct ProfileContract {
    pub description: String,
    pub evidence_policy: String,
    pub clause_policy: String,
    pub required_clause_strength: String,
    pub required_layers: Vec<String>,
    pub required_strengths: Vec<String>,
    pub canonical_minimum_independent_layers: usize,
    pub runners: BTreeMap<String, RunnerContract>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Required deterministic producer identity and bounds for one layer.
pub struct RunnerContract {
    pub producer: String,
    /// Human-facing command that reproduces this runner; actual argv is
    /// recorded separately in each execution receipt.
    pub command: Vec<String>,
    pub configuration: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub simulator_checks: BTreeMap<String, SimulatorCheckContract>,
    pub minimum_observed_checks: usize,
    pub require_peak_rss: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Profile-owned exploration and semantic floors for one simulator check.
pub struct SimulatorCheckContract {
    pub minimum_protocol_states: u64,
    pub minimum_verifier_states: u64,
    pub required_observations: Vec<String>,
}

#[derive(Debug)]
/// Error reading or validating the invariant catalog and profile manifest.
pub struct CatalogError(pub(super) String);

impl fmt::Display for CatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CatalogError {}

impl Catalog {
    /// Loads the invariant IDs and executable evidence declarations.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry cannot be read or any evidence
    /// declaration is missing a field required by the aggregate contract.
    pub fn load(path: &Path) -> Result<Self, CatalogError> {
        let registry = RegistryDocument::load(path)?;
        if registry.schema_version != REGISTRY_SCHEMA_VERSION {
            return Err(CatalogError(format!(
                "unsupported registry schema {}",
                registry.schema_version
            )));
        }
        let invariants = registry.invariant_descriptors();
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
        let clauses = registry.clause_descriptors();
        let clause_ids = clauses
            .iter()
            .map(|clause| clause.clause_id.as_str())
            .collect::<BTreeSet<_>>();
        if clause_ids.len() != clauses.len() {
            return Err(CatalogError(
                "registry clause IDs must be globally unique".to_owned(),
            ));
        }
        for clause in &clauses {
            if !unique_ids.contains(&clause.invariant_id) {
                return Err(CatalogError(format!(
                    "clause {} refers to unknown invariant {}",
                    clause.clause_id, clause.invariant_id
                )));
            }
        }
        for invariant_id in &ids {
            if !clauses
                .iter()
                .any(|clause| clause.invariant_id == *invariant_id && clause.required)
            {
                return Err(CatalogError(format!(
                    "invariant {invariant_id} has no required normative clauses"
                )));
            }
        }
        let evidence = registry.evidence_descriptors();
        for descriptor in &evidence {
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
        Ok(Self {
            ids,
            invariants,
            canonical_ids,
            clauses,
            evidence,
        })
    }

    #[must_use]
    /// Returns the ordered normative clauses owned by one parent invariant.
    pub fn clauses_for(&self, invariant_id: &str) -> Vec<ClauseDescriptor> {
        self.clauses
            .iter()
            .filter(|clause| clause.invariant_id == invariant_id)
            .cloned()
            .collect()
    }

    #[must_use]
    /// Selects and deduplicates registry evidence required by a profile.
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

impl ProfileManifest {
    /// Loads explicit PR, nightly, and weekly evidence policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or is not valid strict
    /// profile-manifest JSON.
    pub fn load(path: &Path) -> Result<Self, CatalogError> {
        let source = fs::read_to_string(path)
            .map_err(|error| CatalogError(format!("read {}: {error}", path.display())))?;
        serde_json::from_str(&source)
            .map_err(|error| CatalogError(format!("parse {}: {error}", path.display())))
    }

    /// Checks the profile manifest against the reviewed registry.
    ///
    /// # Errors
    ///
    /// Returns an error unless the manifest and registry contain exactly the
    /// same 44 IDs and all required profiles have supported nonempty policy.
    pub fn validate(&self, catalog: &Catalog) -> Result<(), CatalogError> {
        if self.schema_version != PROFILE_SCHEMA_VERSION {
            return Err(CatalogError(format!(
                "unsupported profile manifest schema {}",
                self.schema_version
            )));
        }
        if catalog.ids.len() != 44 {
            return Err(CatalogError(format!(
                "registry must contain exactly 44 invariants, found {}",
                catalog.ids.len()
            )));
        }
        let catalog_ids = catalog.ids.iter().collect::<BTreeSet<_>>();
        let reviewed_ids = self.reviewed_ids.iter().collect::<BTreeSet<_>>();
        if self.reviewed_ids.len() != 44 || reviewed_ids.len() != 44 || reviewed_ids != catalog_ids
        {
            return Err(CatalogError(
                "reviewed_ids must contain exactly the registry's 44 unique IDs".to_owned(),
            ));
        }
        for profile in ["pr", "nightly", "weekly"] {
            let Some(contract) = self.profiles.get(profile) else {
                return Err(CatalogError(format!("missing required profile {profile}")));
            };
            if contract.evidence_policy != "all_matching_registry_evidence" {
                return Err(CatalogError(format!(
                    "profile {profile} has unsupported evidence policy {}",
                    contract.evidence_policy
                )));
            }
            if contract.clause_policy != "all_required_clauses"
                || contract.required_clause_strength != "direct"
            {
                return Err(CatalogError(format!(
                    "profile {profile} must require direct evidence for all normative clauses"
                )));
            }
            if contract.description.trim().is_empty()
                || contract.required_layers.is_empty()
                || contract.required_strengths.is_empty()
                || contract
                    .required_layers
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != contract.required_layers.len()
                || contract
                    .required_strengths
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != contract.required_strengths.len()
                || contract.canonical_minimum_independent_layers < 2
                || contract.runners.keys().collect::<BTreeSet<_>>()
                    != contract.required_layers.iter().collect::<BTreeSet<_>>()
            {
                return Err(CatalogError(format!(
                    "profile {profile} must document nonempty evidence requirements"
                )));
            }
            for (layer, runner) in &contract.runners {
                if runner.producer.trim().is_empty()
                    || runner.command.is_empty()
                    || runner.configuration.is_empty()
                    || runner.minimum_observed_checks == 0
                {
                    return Err(CatalogError(format!(
                        "profile {profile} runner {layer} has an incomplete execution contract"
                    )));
                }
                validate_simulator_runner(profile, layer, runner, catalog)?;
                runner_contract::validate_runner(profile, layer, runner).map_err(|error| {
                    CatalogError(format!(
                        "profile {profile} runner {layer} has an invalid typed contract: {error}"
                    ))
                })?;
            }
        }
        Ok(())
    }
}

fn validate_simulator_runner(
    profile: &str,
    layer: &str,
    runner: &RunnerContract,
    catalog: &Catalog,
) -> Result<(), CatalogError> {
    if layer == "simulator" {
        let configuration = runner.simulator_configuration().map_err(|error| {
            CatalogError(format!(
                "profile {profile} runner simulator has an invalid typed contract: {error}"
            ))
        })?;
        configuration.validate_profile(profile).map_err(|error| {
            CatalogError(format!(
                "profile {profile} runner simulator has an invalid typed contract: {error}"
            ))
        })?;
    }
    simulator_contract::validate_check_contracts(profile, layer, &runner.simulator_checks, catalog)
        .map_err(|error| {
            CatalogError(format!(
                "profile {profile} runner {layer} has an invalid simulator check contract: {error}"
            ))
        })
}

impl RunnerContract {
    pub(crate) fn simulator_configuration(
        &self,
    ) -> Result<SimulatorRunnerConfiguration, serde_json::Error> {
        serde_json::from_value(serde_json::to_value(&self.configuration)?)
    }
}
