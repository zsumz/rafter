//! Fail-closed validation and collection of runner execution receipts.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use crate::{
    contract::profile::{ProfileContract, RunnerContract},
    verification::IntakeDefect,
    ArtifactRef, CheckCompletion, CheckReceipt, EvidenceDescriptor, EvidenceResult, EvidenceStatus,
    FailureClassification, ResultBundle,
};

#[cfg(test)]
mod fixtures;
pub(crate) use crate::verification::process_invocation_matches_source;
#[cfg(test)]
pub(crate) use crate::verification::process_launchers_match_runtime;
#[cfg(test)]
pub(crate) use fixtures::{
    launchers as fixture_launchers, process_runtime as fixture_process_runtime,
};

pub(crate) fn collect_results(
    bundles: &[ResultBundle],
    expected: &BTreeMap<String, &EvidenceDescriptor>,
    contract: &ProfileContract,
    profile: &str,
    source_ref: &str,
) -> (
    BTreeMap<String, EvidenceResult>,
    Vec<IntakeDefect>,
    Vec<ArtifactRef>,
) {
    let mut accepted = BTreeMap::<String, EvidenceResult>::new();
    let mut ambiguous = BTreeSet::new();
    let mut defects = Vec::new();
    let mut artifacts = BTreeSet::new();
    for bundle in bundles {
        if bundle.schema_version != crate::evidence::RESULT_SCHEMA_VERSION {
            defects.push(IntakeDefect::malformed(format!(
                "runner {} used unsupported result schema {}",
                bundle.runner, bundle.schema_version
            )));
            continue;
        }
        if bundle.profile != profile {
            defects.push(IntakeDefect::stale(format!(
                "runner {} reported profile {} instead of {profile}",
                bundle.runner, bundle.profile
            )));
            continue;
        }
        if bundle.source_ref != source_ref {
            defects.push(IntakeDefect::stale(format!(
                "runner {} evidence is stale: source {} != {source_ref}",
                bundle.runner, bundle.source_ref
            )));
            continue;
        }
        let Some(runner_contract) = contract.runners.get(&bundle.runner) else {
            defects.push(IntakeDefect::unverifiable(format!(
                "unknown runner {} for profile {profile}",
                bundle.runner
            )));
            continue;
        };
        if let Err(message) = validate_execution(bundle, contract, runner_contract, expected) {
            defects.push(IntakeDefect::unverifiable(format!(
                "runner {}: {message}",
                bundle.runner
            )));
            continue;
        }
        artifacts.extend(bundle.execution.artifacts.iter().cloned());
        collect_bundle_results(
            bundle,
            expected,
            &mut accepted,
            &mut ambiguous,
            &mut defects,
        );
    }
    (accepted, defects, artifacts.into_iter().collect())
}

fn collect_bundle_results(
    bundle: &ResultBundle,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
    accepted: &mut BTreeMap<String, EvidenceResult>,
    ambiguous: &mut BTreeSet<String>,
    defects: &mut Vec<IntakeDefect>,
) {
    for result in &bundle.results {
        let Some(descriptor) = expected.get(&result.evidence_id) else {
            defects.push(IntakeDefect::malformed(format!(
                "runner {} reported unknown evidence {}",
                bundle.runner, result.evidence_id
            )));
            continue;
        };
        if result.invariant_id != descriptor.invariant_id || bundle.runner != descriptor.layer {
            defects.push(IntakeDefect::malformed(format!(
                "evidence {} identity does not match registry invariant/layer",
                result.evidence_id
            )));
            continue;
        }
        if let Err(message) = validate_result(result) {
            defects.push(IntakeDefect::malformed(format!(
                "evidence {}: {message}",
                result.evidence_id
            )));
            continue;
        }
        if ambiguous.contains(&result.evidence_id) {
            continue;
        }
        if accepted.contains_key(&result.evidence_id) {
            accepted.remove(&result.evidence_id);
            ambiguous.insert(result.evidence_id.clone());
            defects.push(IntakeDefect::unverifiable(format!(
                "duplicate result for evidence {}",
                result.evidence_id
            )));
        } else {
            accepted.insert(result.evidence_id.clone(), result.clone());
        }
    }
}

fn validate_execution(
    bundle: &ResultBundle,
    profile_contract: &ProfileContract,
    runner_contract: &RunnerContract,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
) -> Result<(), &'static str> {
    validate_provenance(bundle, profile_contract)?;
    validate_checks(bundle, runner_contract)?;
    validate_runner_receipt(bundle, runner_contract, expected)
}

fn validate_provenance(
    bundle: &ResultBundle,
    profile_contract: &ProfileContract,
) -> Result<(), &'static str> {
    if bundle.execution.plan.schema_version != crate::PLAN_SCHEMA_VERSION
        || bundle.execution.plan.profile != bundle.profile
        || bundle.execution.plan.contract != *profile_contract
        || !valid_plan_input(&bundle.execution.plan.registry)
        || !valid_plan_input(&bundle.execution.plan.manifest)
        || !valid_plan_input(&bundle.execution.plan.result_schema)
        || !valid_plan_input(&bundle.execution.plan.verdict_schema)
    {
        return Err("hashed execution plan does not match profile contract");
    }
    if bundle.execution.invocation.program.trim().is_empty()
        || !is_sha256(&bundle.execution.invocation.program_sha256)
        || bundle.execution.invocation.arguments.is_empty()
        || !bundle.execution.invocation.launchers.is_empty()
        || !Path::new(&bundle.execution.invocation.program).is_absolute()
        || !Path::new(&bundle.execution.invocation.current_dir).is_absolute()
        || Path::new(&bundle.execution.invocation.program)
            != crate::provenance::image::image_path(
                Path::new(&bundle.execution.invocation.current_dir),
                &bundle.execution.invocation.program_sha256,
            )
        || !crate::provenance::invocation::environment_matches_digest(
            &bundle.execution.invocation.environment,
            &bundle.execution.invocation.environment_sha256,
        )
        || !is_sha256(&bundle.execution.invocation.environment_sha256)
    {
        return Err("actual producer invocation provenance is incomplete");
    }
    validate_producer_invocation(bundle)?;
    if bundle.execution.source.commit != bundle.source_ref
        || !bundle.execution.source.clean
        || bundle.execution.source.tree.trim().is_empty()
        || !is_sha256(&bundle.execution.source.cargo_lock_sha256)
        || bundle.execution.source.cargo.trim().is_empty()
        || !is_sha256(&bundle.execution.source.cargo_sha256)
        || !is_sha256(&bundle.execution.source.cargo_config_sha256)
        || bundle.execution.source.rustc.trim().is_empty()
        || !is_sha256(&bundle.execution.source.rustc_sha256)
        || bundle.execution.source.target.trim().is_empty()
        || bundle.execution.source.build_profile.trim().is_empty()
        || bundle
            .execution
            .source
            .tools
            .values()
            .any(|tool| tool.version.trim().is_empty() || !is_sha256(&tool.sha256))
        || bundle.execution.source.process_runtime.is_empty()
        || bundle
            .execution
            .source
            .process_runtime
            .values()
            .any(|executable| {
                !Path::new(&executable.program).is_absolute() || !is_sha256(&executable.sha256)
            })
        || !is_sha256(&bundle.execution.source.environment_sha256)
    {
        return Err("source/toolchain provenance is incomplete or does not match source_ref");
    }
    Ok(())
}

fn validate_producer_invocation(bundle: &ResultBundle) -> Result<(), &'static str> {
    let binaries = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "producer-binary")
        .collect::<Vec<_>>();
    let [binary] = binaries.as_slice() else {
        return Err("producer invocation requires exactly one binary artifact");
    };
    if bundle.execution.producer.binding != crate::provenance::image::PRODUCER_BINDING
        || bundle.execution.producer.executable.kind != "producer-binary"
        || &bundle.execution.producer.executable != *binary
        || binary.sha256 != bundle.execution.invocation.program_sha256
    {
        return Err("producer invocation binary does not match its artifact");
    }
    let arguments = &bundle.execution.invocation.arguments;
    let profile = unique_argument(arguments, "--profile");
    let layer = unique_argument(arguments, "--layer");
    let command_matches = match arguments.first().map(String::as_str) {
        Some("run") => {
            profile == Some(bundle.profile.as_str()) && layer == Some(bundle.runner.as_str())
        }
        Some("run-all") => profile == Some(bundle.profile.as_str()) && layer.is_none(),
        _ => false,
    };
    if !command_matches {
        return Err("producer invocation does not select this profile and layer");
    }
    Ok(())
}

fn unique_argument<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    let values = arguments
        .windows(2)
        .filter(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
        .collect::<Vec<_>>();
    match values.as_slice() {
        [value] => Some(*value),
        _ => None,
    }
}

fn validate_checks(
    bundle: &ResultBundle,
    runner_contract: &RunnerContract,
) -> Result<(), &'static str> {
    if bundle.execution.checks.len() < runner_contract.minimum_observed_checks {
        return Err("observed check count is below the profile minimum");
    }
    if runner_contract.require_peak_rss && bundle.execution.peak_rss_kib == 0 {
        return Err("peak RSS measurement is missing");
    }
    if bundle.execution.artifacts.is_empty()
        || bundle
            .execution
            .artifacts
            .iter()
            .any(|artifact| !valid_artifact(artifact))
    {
        return Err("execution log artifacts are missing");
    }
    let execution_ids = bundle
        .execution
        .checks
        .iter()
        .map(|check| check.execution_id.as_str())
        .collect::<BTreeSet<_>>();
    if execution_ids.len() != bundle.execution.checks.len()
        || bundle
            .execution
            .checks
            .iter()
            .any(|check| !valid_check(check, runner_contract.require_peak_rss))
    {
        return Err("check receipts must be unique and complete");
    }
    let evaluated = bundle
        .execution
        .checks
        .iter()
        .flat_map(|check| check.evidence_ids.iter())
        .collect::<BTreeSet<_>>();
    let results = bundle
        .results
        .iter()
        .map(|result| &result.evidence_id)
        .collect::<BTreeSet<_>>();
    if results.len() != bundle.results.len() || evaluated != results {
        return Err("evaluated evidence IDs must uniquely match result evidence IDs");
    }
    for result in &bundle.results {
        let Some(check) = bundle
            .execution
            .checks
            .iter()
            .find(|check| check.execution_id == result.execution_id)
        else {
            return Err("result does not reference a check receipt");
        };
        if !check.evidence_ids.contains(&result.evidence_id)
            || !completion_allows(check.completion, result.status)
        {
            return Err("result status disagrees with its check completion");
        }
    }
    Ok(())
}

fn validate_runner_receipt(
    bundle: &ResultBundle,
    runner_contract: &RunnerContract,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
) -> Result<(), &'static str> {
    if bundle.runner == "tests" {
        crate::receipt_tests::validate(bundle, expected)?;
    } else if bundle.runner == "simulator" {
        crate::receipt_simulator::validate(bundle, expected, runner_contract)?;
    } else if bundle.runner == "tla" {
        crate::receipt_tla::validate(bundle, expected, runner_contract)?;
    } else if bundle.runner == "maelstrom" {
        crate::receipt_maelstrom::validate(bundle, expected, runner_contract)?;
    }
    Ok(())
}

fn valid_plan_input(input: &crate::PlanInput) -> bool {
    !input.path.trim().is_empty() && input.size_bytes > 0 && is_sha256(&input.sha256)
}

fn valid_check(check: &CheckReceipt, require_peak_rss: bool) -> bool {
    !check.execution_id.trim().is_empty()
        && !check.check_id.trim().is_empty()
        && !check.evidence_ids.is_empty()
        && (!require_peak_rss || check.peak_rss_kib > 0)
        && !check.artifacts.is_empty()
        && check.artifacts.iter().all(valid_artifact)
}

fn completion_allows(completion: CheckCompletion, status: EvidenceStatus) -> bool {
    matches!(
        (completion, status),
        (
            CheckCompletion::Completed | CheckCompletion::FrontierExhausted,
            EvidenceStatus::Pass
        ) | (
            CheckCompletion::Counterexample,
            EvidenceStatus::Fail | EvidenceStatus::Incomplete
        ) | (
            CheckCompletion::CoverageNotReached
                | CheckCompletion::BudgetExhausted
                | CheckCompletion::Timeout,
            EvidenceStatus::Incomplete
        ) | (CheckCompletion::HarnessError, EvidenceStatus::Error)
    )
}

fn valid_artifact(artifact: &ArtifactRef) -> bool {
    !artifact.kind.trim().is_empty()
        && !artifact.path.trim().is_empty()
        && artifact.size_bytes > 0
        && is_sha256(&artifact.sha256)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_result(result: &EvidenceResult) -> Result<(), &'static str> {
    let expected = match result.status {
        EvidenceStatus::Pass => None,
        EvidenceStatus::Fail => Some(FailureClassification::InvariantViolation),
        EvidenceStatus::Incomplete => Some(FailureClassification::CoverageNotReached),
        EvidenceStatus::Error => Some(FailureClassification::HarnessError),
    };
    if result.classification != expected {
        return Err("status and classification disagree");
    }
    if result.status != EvidenceStatus::Pass
        && result
            .message
            .as_deref()
            .is_none_or(|message| message.trim().is_empty())
    {
        return Err("non-pass result must include a message");
    }
    if result.status != EvidenceStatus::Pass && result.artifacts.is_empty() {
        return Err("non-pass result must preserve a replay or log artifact");
    }
    if result
        .artifacts
        .iter()
        .any(|artifact| !valid_artifact(artifact))
    {
        return Err("result contains a malformed artifact reference");
    }
    Ok(())
}
