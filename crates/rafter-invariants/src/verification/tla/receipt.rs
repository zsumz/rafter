//! Structural validation of TLA+ runner receipts.

use std::collections::{BTreeMap, BTreeSet};

use crate::contract::{catalog::EvidenceDescriptor, profile::RunnerContract};
use crate::evidence::format::tla::checkpoint::{
    CONTRACT_KIND, INVENTORY_KIND, RECOVERED_CONTRACT_KIND, RECOVERED_INVENTORY_KIND,
    RECOVERY_REPORT_KIND,
};
use crate::evidence::format::tla::{
    detector_config_kind, detector_log_kind, detector_observation, DETECTOR_PROBES,
    MUTATION_SUITE_ARTIFACT_KIND, REGISTERED_PREDICATES, REQUIRED_MODEL_TRANSITIONS,
};
use crate::evidence::{
    CheckCompletion, CheckReceipt, ContinuationOutcome, EvidenceStatus, PrimaryCompletionPolicy,
    ResultBundle,
};

use super::continuation;

pub(crate) fn validate(
    bundle: &ResultBundle,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
    contract: &RunnerContract,
) -> Result<(), &'static str> {
    let required = expected
        .iter()
        .filter(|(_, descriptor)| descriptor.layer == "tla")
        .map(|(evidence_id, descriptor)| (evidence_id.clone(), descriptor.symbol.clone()))
        .collect::<BTreeMap<_, _>>();
    let predicate_count = required.values().collect::<BTreeSet<_>>().len();
    if predicate_count != 8 || bundle.execution.checks.len() != 1 {
        return Err(
            "TLA receipt must contain one check covering eight model predicates and every clause binding",
        );
    }
    let check = &bundle.execution.checks[0];
    let config = contract
        .configuration
        .get("config")
        .ok_or("TLA profile omitted config")?;
    if check.check_id != format!("tla/{config}#Spec")
        || check.evidence_ids.iter().collect::<BTreeSet<_>>()
            != required.keys().collect::<BTreeSet<_>>()
    {
        return Err("TLA check identity or evidence fanout does not match the registry");
    }
    if !bundle.execution.source.tools.contains_key("java") {
        return Err("TLA receipt lacks Java executable provenance");
    }
    let policy = continuation::pinned_policy(bundle, contract)?;
    match check.completion {
        CheckCompletion::FrontierExhausted => {
            validate_pass(bundle, check, &required, contract, policy)?;
        }
        CheckCompletion::Counterexample => validate_counterexample(bundle, &required)?,
        CheckCompletion::CoverageNotReached
        | CheckCompletion::BudgetExhausted
        | CheckCompletion::Timeout => {
            if bundle
                .results
                .iter()
                .any(|result| result.status != EvidenceStatus::Incomplete)
            {
                return Err("incomplete TLA check must leave every predicate incomplete");
            }
        }
        CheckCompletion::HarnessError => {
            if bundle
                .results
                .iter()
                .any(|result| result.status != EvidenceStatus::Error)
            {
                return Err("TLA harness error must mark every predicate as errored");
            }
        }
        CheckCompletion::Completed => {
            return Err("TLA check cannot use generic completed status");
        }
    }
    Ok(())
}

fn validate_counterexample(
    bundle: &ResultBundle,
    required: &BTreeMap<String, String>,
) -> Result<(), &'static str> {
    let failed_symbols = bundle
        .results
        .iter()
        .filter(|result| result.status == EvidenceStatus::Fail)
        .filter_map(|result| required.get(&result.evidence_id))
        .collect::<BTreeSet<_>>();
    if failed_symbols.len() != 1
        || bundle.results.iter().any(|result| {
            !matches!(
                result.status,
                EvidenceStatus::Fail | EvidenceStatus::Incomplete
            )
        })
    {
        return Err(
            "TLA counterexample must fail every binding of one predicate and leave the rest incomplete",
        );
    }
    let failed_symbol = *failed_symbols
        .iter()
        .next()
        .ok_or("TLA counterexample omitted its failed predicate")?;
    if bundle.results.iter().any(|result| {
        required.get(&result.evidence_id).is_some_and(|symbol| {
            (symbol == failed_symbol) != (result.status == EvidenceStatus::Fail)
        })
    }) {
        return Err(
            "TLA counterexample did not fail exactly one predicate's complete clause fanout",
        );
    }
    Ok(())
}

fn validate_pass(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    required: &BTreeMap<String, String>,
    contract: &RunnerContract,
    policy: PrimaryCompletionPolicy,
) -> Result<(), &'static str> {
    if bundle
        .results
        .iter()
        .any(|result| result.status != EvidenceStatus::Pass)
    {
        return Err("frontier-exhausted TLA check must pass every predicate clause binding");
    }
    let minimum_generated = contract.configuration["minimum_generated_states"]
        .parse::<u64>()
        .map_err(|_| "TLA generated-state floor is invalid")?;
    let minimum_distinct = contract.configuration["minimum_distinct_states"]
        .parse::<u64>()
        .map_err(|_| "TLA distinct-state floor is invalid")?;
    // A continuation that elapsed publishes its progress frame instead of a
    // terminal one. That shape is only admissible under a reporting policy;
    // under a gating policy an elapsed run never reaches this function.
    let elapsed = check
        .tla_continuation
        .ok_or("TLA receipt omitted its continuation binding")?
        .outcome
        == ContinuationOutcome::BudgetElapsedFrontierOpen;
    if elapsed && policy.gates() {
        return Err("a gating TLA continuation cannot pass with an open frontier");
    }
    let mut expected_observations = continuation::expected_counter_frame(elapsed);
    expected_observations.extend(
        REGISTERED_PREDICATES
            .iter()
            .filter_map(|predicate| detector_observation(predicate)),
    );
    expected_observations.extend(
        REQUIRED_MODEL_TRANSITIONS
            .iter()
            .map(|transition| format!("transition_covered:{transition}")),
    );
    expected_observations.extend(required.values().map(|symbol| format!("checked:{symbol}")));
    expected_observations.extend(super::obligation::expected_observations(contract));
    let checkpoint_enabled = contract.configuration.contains_key("checkpoint_minutes");
    if checkpoint_enabled {
        expected_observations.extend([
            "checkpoint_enabled".to_owned(),
            "checkpoint_candidate_present".to_owned(),
            "checkpoint_compatible".to_owned(),
            "checkpoint_recovery_attempted".to_owned(),
        ]);
    }
    if check.observations.keys().cloned().collect::<BTreeSet<_>>() != expected_observations
        || observed(check, "configured_invariants") != 9
        || observed(check, "tool_pin_verified") != 1
        || observed(check, "trace_sample_passed") != 1
        || REGISTERED_PREDICATES.iter().any(|predicate| {
            detector_observation(predicate)
                .is_none_or(|observation| observed(check, &observation) != 1)
        })
        || REQUIRED_MODEL_TRANSITIONS
            .iter()
            .any(|transition| observed(check, &format!("transition_covered:{transition}")) != 1)
        || !continuation::counters_are_consistent(
            check,
            policy,
            elapsed,
            minimum_generated,
            minimum_distinct,
        )
        || required
            .values()
            .any(|symbol| observed(check, &format!("checked:{symbol}")) != 1)
        || !super::obligation::floors_cleared(contract, &|name| observed(check, name))
    {
        return Err("passing TLA receipt lacks exact terminal frames or configured coverage");
    }
    if checkpoint_enabled
        && (observed(check, "checkpoint_enabled") != 1
            || observed(check, "checkpoint_compatible") != 1
            || observed(check, "checkpoint_candidate_present") > 1
            || observed(check, "checkpoint_recovery_attempted") > 1
            || observed(check, "checkpoint_recovery_attempted")
                > observed(check, "checkpoint_candidate_present"))
    {
        return Err("passing checkpointed TLA receipt lacks compatible recovery metadata");
    }
    let required_artifacts = required_proof_artifact_kinds(
        checkpoint_enabled,
        observed(check, "checkpoint_candidate_present") == 1,
        contract,
    );
    let actual_artifacts = check
        .artifacts
        .iter()
        .map(|artifact| artifact.kind.clone())
        .collect::<BTreeSet<_>>();
    if actual_artifacts != required_artifacts || actual_artifacts.len() != check.artifacts.len() {
        return Err("passing TLA receipt lacks the exact unique proof artifact set");
    }
    Ok(())
}

fn required_proof_artifact_kinds(
    checkpoint_enabled: bool,
    checkpoint_candidate_present: bool,
    contract: &RunnerContract,
) -> BTreeSet<String> {
    let mut kinds = [
        "tla-log",
        "tla-trace-log",
        "tla-tool",
        "tla-spec",
        "tla-trace-spec",
        "tla-detector-spec",
        "tla-runner",
        "tla-tool-asset-id",
        "tla-tool-checksums",
        "tla-config",
        "tla-trace-config",
        "tla-detector-config",
        MUTATION_SUITE_ARTIFACT_KIND,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    for probe in DETECTOR_PROBES {
        kinds.insert(detector_log_kind(probe).unwrap_or_else(|| {
            format!(
                "invalid-tla-detector-log:{}:{}",
                probe.predicate, probe.mode
            )
        }));
        kinds.insert(detector_config_kind(probe).unwrap_or_else(|| {
            format!(
                "invalid-tla-detector-config:{}:{}",
                probe.predicate, probe.mode
            )
        }));
    }
    kinds.extend(super::obligation::artifact_kinds(contract));
    if checkpoint_enabled {
        kinds.extend([
            CONTRACT_KIND.to_owned(),
            INVENTORY_KIND.to_owned(),
            RECOVERY_REPORT_KIND.to_owned(),
        ]);
        if checkpoint_candidate_present {
            kinds.extend([
                RECOVERED_CONTRACT_KIND.to_owned(),
                RECOVERED_INVENTORY_KIND.to_owned(),
            ]);
        }
    }
    kinds
}

fn observed(check: &CheckReceipt, name: &str) -> u64 {
    check.observations.get(name).copied().unwrap_or_default()
}

#[cfg(test)]
#[path = "tests/receipt.rs"]
mod tests;
