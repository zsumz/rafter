use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::Path,
};

use serde::Deserialize;

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
}

impl EvidenceDescriptor {
    /// Returns the stable aggregate key for this evidence declaration.
    #[must_use]
    pub fn evidence_id(&self) -> String {
        format!(
            "{}/{}/{}#{}",
            self.invariant_id, self.layer, self.path, self.symbol
        )
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
}

#[derive(Debug)]
/// Error reading or validating the invariant catalog and profile manifest.
pub struct CatalogError(String);

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
            {
                return Err(CatalogError(format!(
                    "profile {profile} must document nonempty evidence requirements"
                )));
            }
        }
        Ok(())
    }
}

fn parse_invariants(source: &str) -> Result<(Vec<String>, BTreeSet<String>), CatalogError> {
    let records = parse_section_records(source, "invariants:");
    let ids = records
        .iter()
        .filter_map(|record| record.get("id").cloned())
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Err(CatalogError(
            "registry contains no invariant IDs".to_owned(),
        ));
    }
    let canonical_ids = records
        .iter()
        .filter(|record| record.get("tier").is_some_and(|tier| tier == "canonical"))
        .filter_map(|record| record.get("id").cloned())
        .collect();
    Ok((ids, canonical_ids))
}

fn parse_evidence(source: &str) -> Result<Vec<EvidenceDescriptor>, CatalogError> {
    parse_section_records(source, "evidence:")
        .into_iter()
        .enumerate()
        .map(|(index, record)| {
            let required = |field: &str| {
                record.get(field).cloned().ok_or_else(|| {
                    CatalogError(format!(
                        "evidence record {} is missing required field {field}",
                        index + 1
                    ))
                })
            };
            Ok(EvidenceDescriptor {
                invariant_id: required("id")?,
                layer: required("layer")?,
                strength: required("strength")?,
                path: required("path")?,
                symbol: required("symbol")?,
            })
        })
        .collect()
}

fn parse_section_records(source: &str, section: &str) -> Vec<BTreeMap<String, String>> {
    let mut records = Vec::new();
    let mut current = None::<BTreeMap<String, String>>;
    let mut active = false;
    for raw_line in source.lines() {
        let indent = raw_line.chars().take_while(|ch| *ch == ' ').count();
        let line = raw_line.trim();
        if indent == 0 {
            if active {
                if let Some(record) = current.take() {
                    records.push(record);
                }
            }
            active = line == section;
            continue;
        }
        if !active || line.is_empty() || line.starts_with('#') {
            continue;
        }
        if indent == 2 && line.starts_with("- id: ") {
            if let Some(record) = current.take() {
                records.push(record);
            }
            let mut record = BTreeMap::new();
            record.insert("id".to_owned(), yaml_value(&line[6..]));
            current = Some(record);
        } else if indent == 4 {
            if let Some((key, value)) = line.split_once(": ") {
                if let Some(record) = current.as_mut() {
                    record.insert(key.to_owned(), yaml_value(value));
                }
            }
        }
    }
    if active {
        if let Some(record) = current {
            records.push(record);
        }
    }
    records
}

fn yaml_value(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
        value[1..value.len() - 1].replace("\\\"", "\"")
    } else {
        value.to_owned()
    }
}
