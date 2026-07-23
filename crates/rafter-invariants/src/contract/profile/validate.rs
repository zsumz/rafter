//! Cross-profile validation against the normalized catalog.

use std::collections::BTreeSet;

use super::{
    model::PROFILE_SCHEMA_VERSION, ClausePolicy, EvidenceLayer, EvidencePolicy, EvidenceStrength,
    ProfileManifest, RequiredClauseStrength, RunnerContract,
};
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
        if self
            .verifiers
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            != BTreeSet::from(["pr", "nightly", "weekly"])
        {
            return Err(CatalogError(
                "verifier contracts must contain exactly pr, nightly, and weekly".to_owned(),
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
        if contract.evidence_policy != EvidencePolicy::AllMatchingRegistryEvidence {
            return Err(CatalogError(format!(
                "profile {profile} has unsupported evidence policy {}",
                contract.evidence_policy
            )));
        }
        if contract.clause_policy != ClausePolicy::AllRequiredClauses
            || contract.required_clause_strength != RequiredClauseStrength::Direct
        {
            return Err(CatalogError(format!(
                "profile {profile} must require direct evidence for all normative clauses"
            )));
        }
        if contract.description.trim().is_empty()
            || has_duplicates(&contract.required_layers)
            || has_duplicates(&contract.required_strengths)
            || !exact_policy_set(&contract.required_layers, required_layers(profile))
            || !exact_policy_set(
                &contract.required_strengths,
                &[EvidenceStrength::Direct, EvidenceStrength::E2e],
            )
            || contract.canonical_minimum_independent_layers != 2
            || contract
                .runners
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                != contract
                    .required_layers
                    .iter()
                    .map(|layer| layer.as_str())
                    .collect::<BTreeSet<_>>()
        {
            return Err(CatalogError(format!(
                "profile {profile} must document nonempty evidence requirements"
            )));
        }
        for (layer, runner) in &contract.runners {
            validate_runner(profile, layer, runner, catalog)?;
        }
        validate_verifier(
            profile,
            self.verifiers.get(profile).ok_or_else(|| {
                CatalogError(format!("missing verifier contract for profile {profile}"))
            })?,
        )?;
        Ok(())
    }
}

fn validate_verifier(
    profile: &str,
    verifier: &super::VerifierContract,
) -> Result<(), CatalogError> {
    let replay = &verifier.detector_replay;
    let valid = replay.required_inventory_sha256
        == super::replay::REVIEWED_DETECTOR_REPLAY_INVENTORY_SHA256
        && replay.required_registry_packages == 248
        && replay.maximum_registry_archive_bytes == 268_435_456
        && replay.maximum_registry_expanded_bytes == 2_147_483_648
        && replay.maximum_registry_entries == 250_000
        && replay.required_unique_fixtures == 77
        && replay.required_evidence_bindings == 79
        && replay.required_targets == 2
        && replay.compile_timeout_seconds == 600
        && replay.fixture_timeout_seconds == 30
        && replay.total_timeout_seconds
            == super::replay::REVIEWED_DETECTOR_REPLAY_TOTAL_TIMEOUT_SECONDS;
    if !valid {
        return Err(CatalogError(format!(
            "profile {profile} has an unsupported detector replay contract"
        )));
    }
    Ok(())
}

fn required_layers(profile: &str) -> &'static [EvidenceLayer] {
    match profile {
        "pr" => &[
            EvidenceLayer::Tests,
            EvidenceLayer::Simulator,
            EvidenceLayer::Tla,
        ],
        "nightly" | "weekly" => &[
            EvidenceLayer::Tests,
            EvidenceLayer::Simulator,
            EvidenceLayer::Tla,
            EvidenceLayer::Maelstrom,
        ],
        _ => &[],
    }
}

fn exact_policy_set<T: Ord>(observed: &[T], expected: &[T]) -> bool {
    observed.len() == expected.len()
        && observed.iter().collect::<BTreeSet<_>>() == expected.iter().collect::<BTreeSet<_>>()
}

fn has_duplicates<T: Ord>(values: &[T]) -> bool {
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
