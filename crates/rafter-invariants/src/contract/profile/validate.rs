//! Cross-profile validation against the normalized catalog.

use std::collections::BTreeSet;

use super::{model::PROFILE_SCHEMA_VERSION, ProfileManifest, RunnerContract};
use crate::contract::{catalog::Catalog, error::CatalogError};

impl ProfileManifest {
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
        if self
            .profiles
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            != BTreeSet::from(["pr", "nightly", "weekly"])
        {
            return Err(CatalogError(
                "profile manifest must contain exactly pr, nightly, and weekly".to_owned(),
            ));
        }
        for profile in ["pr", "nightly", "weekly"] {
            self.validate_profile(profile, catalog)?;
        }
        Ok(())
    }

    fn validate_profile(&self, profile: &str, catalog: &Catalog) -> Result<(), CatalogError> {
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
            || has_duplicates(&contract.required_layers)
            || has_duplicates(&contract.required_strengths)
            || !exact_policy_set(&contract.required_layers, required_layers(profile))
            || !exact_policy_set(&contract.required_strengths, &["direct", "e2e"])
            || contract.canonical_minimum_independent_layers != 2
            || contract.runners.keys().collect::<BTreeSet<_>>()
                != contract.required_layers.iter().collect::<BTreeSet<_>>()
        {
            return Err(CatalogError(format!(
                "profile {profile} must document nonempty evidence requirements"
            )));
        }
        for (layer, runner) in &contract.runners {
            validate_runner(profile, layer, runner, catalog)?;
        }
        Ok(())
    }
}

fn required_layers(profile: &str) -> &'static [&'static str] {
    match profile {
        "pr" => &["tests", "simulator", "tla"],
        "nightly" | "weekly" => &["tests", "simulator", "tla", "maelstrom"],
        _ => &[],
    }
}

fn exact_policy_set(observed: &[String], expected: &[&str]) -> bool {
    observed.len() == expected.len()
        && observed.iter().map(String::as_str).collect::<BTreeSet<_>>()
            == expected.iter().copied().collect::<BTreeSet<_>>()
}

fn has_duplicates(values: &[String]) -> bool {
    values.iter().collect::<BTreeSet<_>>().len() != values.len()
}

fn validate_runner(
    profile: &str,
    layer: &str,
    runner: &RunnerContract,
    catalog: &Catalog,
) -> Result<(), CatalogError> {
    if runner.producer.trim().is_empty()
        || runner.command.is_empty()
        || runner.configuration.is_empty()
        || runner.minimum_observed_checks == 0
    {
        return Err(CatalogError(format!(
            "profile {profile} runner {layer} has an incomplete execution contract"
        )));
    }
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
    super::simulator::validate_check_contracts(profile, layer, &runner.simulator_checks, catalog)
        .map_err(|error| {
        CatalogError(format!(
            "profile {profile} runner {layer} has an invalid simulator check contract: {error}"
        ))
    })?;
    super::runner_contract::validate_runner(profile, layer, runner).map_err(|error| {
        CatalogError(format!(
            "profile {profile} runner {layer} has an invalid typed contract: {error}"
        ))
    })
}
