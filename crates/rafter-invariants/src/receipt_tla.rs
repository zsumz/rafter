use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::RunnerContract;
use crate::{CheckCompletion, EvidenceDescriptor, EvidenceStatus, ResultBundle};

pub(super) fn validate(
    bundle: &ResultBundle,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
    contract: &RunnerContract,
) -> Result<(), &'static str> {
    let required = expected
        .iter()
        .filter(|(_, descriptor)| descriptor.layer == "tla")
        .map(|(evidence_id, descriptor)| (evidence_id.clone(), descriptor.symbol.clone()))
        .collect::<BTreeMap<_, _>>();
    if required.len() != 8 || bundle.execution.checks.len() != 1 {
        return Err("TLA receipt must contain one check covering eight registry predicates");
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
    match check.completion {
        CheckCompletion::FrontierExhausted => {
            validate_pass(bundle, check, &required, contract)?;
        }
        CheckCompletion::Counterexample => validate_counterexample(bundle)?,
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

fn validate_counterexample(bundle: &ResultBundle) -> Result<(), &'static str> {
    let statuses = bundle
        .results
        .iter()
        .map(|result| result.status)
        .collect::<Vec<_>>();
    if statuses
        .iter()
        .filter(|status| **status == EvidenceStatus::Fail)
        .count()
        != 1
        || statuses
            .iter()
            .any(|status| !matches!(status, EvidenceStatus::Fail | EvidenceStatus::Incomplete))
    {
        return Err("TLA counterexample must fail one predicate and leave the rest incomplete");
    }
    Ok(())
}

fn validate_pass(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    required: &BTreeMap<String, String>,
    contract: &RunnerContract,
) -> Result<(), &'static str> {
    if bundle
        .results
        .iter()
        .any(|result| result.status != EvidenceStatus::Pass)
    {
        return Err("frontier-exhausted TLA check must pass all eight predicates");
    }
    let minimum = contract.configuration["minimum_distinct_states"]
        .parse::<u64>()
        .map_err(|_| "TLA state floor is invalid")?;
    let mut expected_observations = BTreeSet::from([
        "configured_invariants".to_owned(),
        "tool_pin_verified".to_owned(),
        "trace_sample_passed".to_owned(),
        "detector_negative_passed".to_owned(),
        "generated_states".to_owned(),
        "distinct_states".to_owned(),
        "states_left_on_queue".to_owned(),
        "search_depth".to_owned(),
    ]);
    expected_observations.extend(required.values().map(|symbol| format!("checked:{symbol}")));
    if check.observations.keys().cloned().collect::<BTreeSet<_>>() != expected_observations
        || observed(check, "configured_invariants") != 9
        || observed(check, "tool_pin_verified") != 1
        || observed(check, "trace_sample_passed") != 1
        || observed(check, "detector_negative_passed") != 1
        || observed(check, "distinct_states") < minimum
        || observed(check, "generated_states") < observed(check, "distinct_states")
        || observed(check, "states_left_on_queue") != 0
        || observed(check, "search_depth") == 0
        || required
            .values()
            .any(|symbol| observed(check, &format!("checked:{symbol}")) != 1)
    {
        return Err("passing TLA receipt lacks exact terminal frames or configured coverage");
    }
    let required_artifacts = BTreeSet::from([
        "tla-log",
        "tla-trace-log",
        "tla-detector-log",
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
    ]);
    let actual_artifacts = check
        .artifacts
        .iter()
        .map(|artifact| artifact.kind.as_str())
        .collect::<BTreeSet<_>>();
    if actual_artifacts != required_artifacts || actual_artifacts.len() != check.artifacts.len() {
        return Err("passing TLA receipt lacks the exact unique proof artifact set");
    }
    Ok(())
}

fn observed(check: &crate::CheckReceipt, name: &str) -> u64 {
    check.observations.get(name).copied().unwrap_or_default()
}
