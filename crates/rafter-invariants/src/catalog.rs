use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::Path,
};

use serde::Deserialize;

use crate::registry_parse::{parse_evidence, parse_invariants};

#[derive(Clone, Debug)]
/// Reviewed invariant IDs and their declared executable evidence.
pub struct Catalog {
    pub ids: Vec<String>,
    pub canonical_ids: BTreeSet<String>,
    pub evidence: Vec<EvidenceDescriptor>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// One direct or end-to-end evidence declaration from the registry.
pub struct EvidenceDescriptor {
    pub invariant_id: String,
    pub layer: String,
    pub strength: String,
    pub path: String,
    pub symbol: String,
    pub negative_fixture: Option<String>,
    pub test: Option<TestIdentity>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
/// Exact Cargo target and libtest identity for tests-layer evidence.
pub struct TestIdentity {
    pub package: String,
    pub target_kind: String,
    pub target: String,
    pub test_name: String,
}

impl EvidenceDescriptor {
    /// Returns the stable aggregate key for this evidence declaration.
    #[must_use]
    pub fn evidence_id(&self) -> String {
        let base = format!(
            "{}/{}/{}/{}#{}",
            self.invariant_id, self.layer, self.strength, self.path, self.symbol
        );
        self.negative_fixture
            .as_ref()
            .map_or(base.clone(), |fixture| format!("{base}@{fixture}"))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Explicit invariant IDs and evidence policies for every scheduled profile.
pub struct ProfileManifest {
    pub schema_version: u32,
    pub reviewed_ids: Vec<String>,
    pub profiles: BTreeMap<String, ProfileContract>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Evidence-selection and independent-layer policy for one profile.
pub struct ProfileContract {
    pub description: String,
    pub evidence_policy: String,
    pub required_layers: Vec<String>,
    pub required_strengths: Vec<String>,
    pub canonical_minimum_independent_layers: usize,
    pub runners: BTreeMap<String, RunnerContract>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
/// Required deterministic producer identity and bounds for one layer.
pub struct RunnerContract {
    pub producer: String,
    pub command: Vec<String>,
    pub configuration: BTreeMap<String, String>,
    pub minimum_observed_checks: usize,
    pub require_peak_rss: bool,
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
        let source = fs::read_to_string(path)
            .map_err(|error| CatalogError(format!("read {}: {error}", path.display())))?;
        let (ids, canonical_ids) = parse_invariants(&source)?;
        let evidence = parse_evidence(&source)?;
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
            canonical_ids,
            evidence,
        })
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
        if self.schema_version != 1 {
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
        if reviewed_ids.len() != 44 || reviewed_ids != catalog_ids {
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
            if contract.description.trim().is_empty()
                || contract.required_layers.is_empty()
                || contract.required_strengths.is_empty()
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
            }
        }
        Ok(())
    }
}
