use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path},
};

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{aggregate::AggregateError, EvidenceStatus, ResultBundle};

const EVENT_PREFIX: &str = "RAFTER_EVENT ";

pub(super) fn verify(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    for input in [
        &bundle.execution.plan.registry,
        &bundle.execution.plan.manifest,
    ] {
        crate::plan::verify_plan_input(input, root).map_err(|error| {
            AggregateError::new(format!(
                "verify execution-plan input {}: {error}",
                input.path
            ))
        })?;
    }
    let mut artifacts = bundle.execution.artifacts.iter().collect::<BTreeSet<_>>();
    artifacts.extend(
        bundle
            .execution
            .checks
            .iter()
            .flat_map(|check| check.artifacts.iter()),
    );
    artifacts.extend(
        bundle
            .results
            .iter()
            .flat_map(|result| result.artifacts.iter()),
    );
    for artifact in artifacts {
        let path = Path::new(&artifact.path);
        if path.is_absolute()
            || path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(AggregateError::new(format!(
                "artifact path must be repository-relative: {}",
                artifact.path
            )));
        }
        let bytes = fs::read(root.join(path)).map_err(|error| {
            AggregateError::new(format!("read artifact {}: {error}", artifact.path))
        })?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        if artifact.size_bytes != bytes.len() as u64 || artifact.sha256 != digest {
            return Err(AggregateError::new(format!(
                "artifact integrity mismatch: {}",
                artifact.path
            )));
        }
    }
    verify_compile_invocations(bundle, root)?;
    match bundle.runner.as_str() {
        "tests" => verify_test_logs(bundle, root),
        "simulator" => verify_simulator_logs(bundle, root),
        "tla" => crate::artifact_verify_tla::verify(bundle, root),
        "maelstrom" => crate::artifact_verify_maelstrom::verify(bundle, root),
        _ => Ok(()),
    }
}

fn verify_compile_invocations(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    let logs = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "compile-log")
        .collect::<Vec<_>>();
    if !matches!(bundle.runner.as_str(), "tests" | "simulator") {
        return Ok(());
    }
    if logs.is_empty() {
        return Err(AggregateError::new(format!(
            "{} execution has no compile invocation log",
            bundle.runner
        )));
    }
    let current_dir = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize compile root: {error}")))?
        .to_string_lossy()
        .into_owned();
    for log in logs {
        let source = fs::read_to_string(root.join(&log.path)).map_err(|error| {
            AggregateError::new(format!("read compile log {}: {error}", log.path))
        })?;
        let invocations = crate::producer::process::parse_combined_invocations(&source)
            .map_err(|error| AggregateError::new(format!("parse compile invocation: {error}")))?;
        let [observed] = invocations.as_slice() else {
            return Err(AggregateError::new(
                "compile log must contain exactly one invocation".to_owned(),
            ));
        };
        if observed.invocation.program != "cargo"
            || observed.invocation.program_sha256 != bundle.execution.source.cargo_sha256
            || observed.invocation.current_dir != current_dir
        {
            return Err(AggregateError::new(
                "compile executable or working directory does not match source provenance"
                    .to_owned(),
            ));
        }
        if bundle.runner == "tests" {
            verify_test_compile(bundle, observed)?;
        } else {
            verify_simulator_compile(bundle, observed)?;
        }
    }
    Ok(())
}

fn verify_test_compile(
    bundle: &ResultBundle,
    observed: &crate::producer::process::LabeledInvocation,
) -> Result<(), AggregateError> {
    let parts = observed.label.split('/').collect::<Vec<_>>();
    let [package, kind, target] = parts.as_slice() else {
        return Err(AggregateError::new(
            "test compile label does not name one Cargo target".to_owned(),
        ));
    };
    let selector = match *kind {
        "lib" => vec!["--lib".to_owned()],
        "test" => vec!["--test".to_owned(), (*target).to_owned()],
        "bin" => vec!["--bin".to_owned(), (*target).to_owned()],
        _ => {
            return Err(AggregateError::new(
                "test compile label has an unsupported target kind".to_owned(),
            ))
        }
    };
    let mut expected = vec![
        "test".to_owned(),
        "--locked".to_owned(),
        "--no-default-features".to_owned(),
        "-p".to_owned(),
        (*package).to_owned(),
    ];
    expected.extend(selector);
    expected.extend([
        "--no-run".to_owned(),
        "--message-format=json-render-diagnostics".to_owned(),
    ]);
    if observed.invocation.arguments != expected
        || observed.invocation.environment_sha256 != bundle.execution.source.environment_sha256
    {
        return Err(AggregateError::new(
            "test compile log does not match the exact Cargo invocation plan".to_owned(),
        ));
    }
    Ok(())
}

fn verify_simulator_compile(
    bundle: &ResultBundle,
    observed: &crate::producer::process::LabeledInvocation,
) -> Result<(), AggregateError> {
    let expected_arguments = [
        "build",
        "--release",
        "--locked",
        "-p",
        "rafter-sim",
        "--bin",
        "rafter-model-check-fast",
        "--message-format=json-render-diagnostics",
    ];
    let source_prefix = bundle.source_ref.get(..12).unwrap_or(&bundle.source_ref);
    let expected_target = format!(
        "target/rafter-invariants/simulator-build/{source_prefix}/{}",
        bundle.profile
    );
    let mut base_environment = observed.invocation.environment.clone();
    let target = base_environment.remove("CARGO_TARGET_DIR");
    if observed.label != "simulator compile"
        || observed.invocation.arguments != expected_arguments
        || target.as_deref() != Some(expected_target.as_str())
        || crate::producer::process::digest_environment(&base_environment)
            != bundle.execution.source.environment_sha256
    {
        return Err(AggregateError::new(
            "simulator compile log does not match the exact Cargo invocation plan".to_owned(),
        ));
    }
    Ok(())
}

fn verify_test_logs(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    for check in &bundle.execution.checks {
        if !is_passing(bundle, &check.execution_id) {
            continue;
        }
        let test_name = check
            .check_id
            .rsplit_once('#')
            .map(|(_, test_name)| test_name)
            .ok_or_else(|| {
                AggregateError::new(format!("invalid tests check ID {}", check.check_id))
            })?;
        let source = read_artifact_kind(check, "test-log", root)?;
        verify_test_invocations(bundle, check, &source, test_name, root)?;
        require_exact_test_pass(&source, test_name, &check.check_id)?;
    }
    Ok(())
}

fn verify_simulator_logs(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    verify_simulator_schedule(bundle, root)?;
    let events = simulator_events(bundle, root)?;
    let mut test_logs = BTreeMap::<String, String>::new();
    for check in &bundle.execution.checks {
        verify_simulator_observations(check, &events)?;
        if !is_passing(bundle, &check.execution_id) {
            continue;
        }
        let Some((_, fixture)) = check.evidence_ids[0].rsplit_once('@') else {
            continue;
        };
        let artifact = check
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "test-log")
            .ok_or_else(|| {
                AggregateError::new(format!("detector log missing for {}", check.check_id))
            })?;
        let source = if let Some(source) = test_logs.get(&artifact.path) {
            source.clone()
        } else {
            let source = fs::read_to_string(root.join(&artifact.path)).map_err(|error| {
                AggregateError::new(format!("read detector log {}: {error}", artifact.path))
            })?;
            test_logs.insert(artifact.path.clone(), source.clone());
            source
        };
        verify_test_invocations(bundle, check, &source, fixture, root)?;
        require_exact_test_pass(&source, fixture, &check.check_id)?;
    }
    Ok(())
}

fn verify_simulator_schedule(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    let logs = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "simulator-log")
        .collect::<Vec<_>>();
    let sources = logs
        .iter()
        .map(|log| {
            fs::read_to_string(root.join(&log.path)).map_err(|error| {
                AggregateError::new(format!(
                    "read scheduled simulator log {}: {error}",
                    log.path
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    verify_simulator_invocations(bundle, root, &sources)?;
    validate_simulator_schedule(&bundle.profile, &bundle.source_ref, &sources)
}

fn verify_test_invocations(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    source: &str,
    test_name: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    let invocations = crate::producer::process::parse_combined_invocations(source)
        .map_err(|error| AggregateError::new(format!("parse test invocation: {error}")))?;
    let binary = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "test-binary")
        .ok_or_else(|| AggregateError::new("test binary artifact is missing".to_owned()))?;
    let current_dir = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize test root: {error}")))?
        .to_string_lossy()
        .into_owned();
    let base_digest = bundle.execution.source.environment_sha256.as_str();
    let temporary = Path::new("target/rafter-invariants/tmp").join(&check.execution_id);
    let seed = crate::producer::artifact::deterministic_u64(
        "rafter-tests/v1",
        &format!("{}\0{}\0{test_name}", bundle.profile, bundle.source_ref),
    );
    let mut exact_environment = invocations
        .first()
        .map(|invocation| invocation.invocation.environment.clone())
        .unwrap_or_default();
    exact_environment.extend([
        ("PROPTEST_RNG_SEED".to_owned(), seed.to_string()),
        (
            "PROPTEST_DISABLE_FAILURE_PERSISTENCE".to_owned(),
            "1".to_owned(),
        ),
        (
            "TMPDIR".to_owned(),
            temporary.to_string_lossy().into_owned(),
        ),
        ("RUST_BACKTRACE".to_owned(), "1".to_owned()),
    ]);
    let exact_digest = crate::producer::process::digest_environment(&exact_environment);
    let expected = [
        (
            "libtest discovery",
            vec!["--list", "--format", "terse"],
            base_digest,
        ),
        (
            "libtest ignored discovery",
            vec!["--ignored", "--list", "--format", "terse"],
            base_digest,
        ),
    ];
    if invocations.len() != 3
        || expected
            .iter()
            .zip(&invocations[..2])
            .any(|(expected, observed)| {
                observed.label != expected.0
                    || observed.invocation.arguments != expected.1
                    || observed.invocation.environment_sha256 != expected.2
                    || crate::producer::process::digest_environment(
                        &observed.invocation.environment,
                    ) != expected.2
            })
    {
        return Err(AggregateError::new(
            "test log does not contain the exact discovery invocation plan".to_owned(),
        ));
    }
    let exact = &invocations[2];
    let exact_arguments = &exact.invocation.arguments;
    if exact.label != "exact libtest execution"
        || exact_arguments.len() < 5
        || exact_arguments[0] != test_name
        || exact_arguments[1..5] != ["--exact", "--test-threads=1", "--color", "never"]
        || (exact_arguments.len() == 6 && exact_arguments[5] != "--ignored")
        || exact_arguments.len() > 6
        || exact.invocation.environment_sha256 != exact_digest
        || invocations.iter().any(|invocation| {
            invocation.invocation.program_sha256 != binary.sha256
                || invocation.invocation.current_dir != current_dir
                || !Path::new(&invocation.invocation.program).is_absolute()
        })
    {
        return Err(AggregateError::new(
            "test log does not contain the exact executable invocation plan".to_owned(),
        ));
    }
    Ok(())
}

fn verify_simulator_invocations(
    bundle: &ResultBundle,
    root: &Path,
    sources: &[String],
) -> Result<(), AggregateError> {
    let binary = bundle
        .execution
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "simulator-binary")
        .ok_or_else(|| AggregateError::new("simulator binary artifact is missing".to_owned()))?;
    let environment_sha256 = bundle.execution.source.environment_sha256.as_str();
    let current_dir = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize simulator root: {error}")))?
        .to_string_lossy()
        .into_owned();
    let expected: Vec<(String, Vec<String>)> = match bundle.profile.as_str() {
        "pr" => vec![
            (
                "fast".to_owned(),
                vec!["--profile".to_owned(), "fast".to_owned()],
            ),
            (
                "raft-soak".to_owned(),
                vec!["--profile".to_owned(), "raft-soak".to_owned()],
            ),
        ],
        profile @ ("nightly" | "weekly") => {
            let label = format!("raft-{profile}");
            let seeds = crate::producer::expected_scheduled_seeds(profile, &bundle.source_ref)
                .ok_or_else(|| AggregateError::new("scheduled seeds are missing".to_owned()))?;
            vec![(
                label.clone(),
                vec!["--profile".to_owned(), label, "--seed".to_owned(), seeds],
            )]
        }
        profile => {
            return Err(AggregateError::new(format!(
                "unknown simulator profile {profile}"
            )))
        }
    };
    if sources.len() != expected.len() {
        return Err(AggregateError::new(
            "simulator log count does not match the execution plan".to_owned(),
        ));
    }
    for (label, arguments) in expected {
        let source = sources
            .iter()
            .find(|source| source.lines().any(|line| line == format!("label: {label}")))
            .ok_or_else(|| AggregateError::new(format!("simulator log {label} is missing")))?;
        let invocations = crate::producer::process::parse_combined_invocations(source)
            .map_err(|error| AggregateError::new(format!("parse simulator invocation: {error}")))?;
        let [observed] = invocations.as_slice() else {
            return Err(AggregateError::new(format!(
                "simulator log {label} must contain exactly one invocation"
            )));
        };
        if observed.label != label
            || observed.invocation.arguments != arguments
            || observed.invocation.program_sha256 != binary.sha256
            || observed.invocation.current_dir != current_dir
            || observed.invocation.environment_sha256 != environment_sha256
            || crate::producer::process::digest_environment(&observed.invocation.environment)
                != environment_sha256
            || !Path::new(&observed.invocation.program).is_absolute()
        {
            return Err(AggregateError::new(format!(
                "simulator log {label} does not match the exact invocation plan"
            )));
        }
    }
    Ok(())
}

fn validate_simulator_schedule(
    profile: &str,
    source_ref: &str,
    logs: &[String],
) -> Result<(), AggregateError> {
    let Some(expected_seeds) = crate::producer::expected_scheduled_seeds(profile, source_ref)
    else {
        return Ok(());
    };
    if logs.len() != 1 {
        return Err(AggregateError::new(format!(
            "scheduled simulator receipt must retain exactly one profile log, found {}",
            logs.len()
        )));
    }
    let model_profile = format!("raft-{profile}");
    let expected_profile = format!("model-check profile={model_profile} ");
    let expected_seed_line =
        format!("model-check {model_profile}-soak seeds source=replay seeds={expected_seeds}");
    let events = parse_machine_events(&logs[0])?;
    if !logs[0].lines().any(|line| line == "exit_code: Some(0)")
        || !logs[0]
            .lines()
            .any(|line| line.starts_with(&expected_profile))
        || !logs[0].lines().any(|line| line == expected_seed_line)
        || !profile_total_is_rederived(profile, &model_profile, &events)
        || !soak_seeds_are_rederived(&model_profile, &expected_seeds, &events)
    {
        return Err(AggregateError::new(format!(
            "scheduled simulator log does not prove the source-derived {profile} execution plan"
        )));
    }
    Ok(())
}

fn parse_machine_events(log: &str) -> Result<Vec<Value>, AggregateError> {
    log.lines()
        .filter_map(|line| line.strip_prefix(EVENT_PREFIX))
        .map(|line| {
            serde_json::from_str(line).map_err(|error| {
                AggregateError::new(format!("parse scheduled simulator event: {error}"))
            })
        })
        .collect()
}

fn profile_total_is_rederived(profile: &str, model_profile: &str, events: &[Value]) -> bool {
    let (protocol_floor, verifier_floor) = match profile {
        "nightly" => (100_000_000, 100_000_000),
        "weekly" => (250_000_000, 250_000_000),
        _ => return false,
    };
    let exhaustive = events
        .iter()
        .filter(|event| event["event"] == "exhaustive-check")
        .collect::<Vec<_>>();
    let Some(protocol_total) = exhaustive.iter().try_fold(0_u64, |total, event| {
        total.checked_add(event["unique_protocol_states"].as_u64()?)
    }) else {
        return false;
    };
    let Some(verifier_total) = exhaustive.iter().try_fold(0_u64, |total, event| {
        total.checked_add(event["unique_verifier_states"].as_u64()?)
    }) else {
        return false;
    };
    let profile_totals = events
        .iter()
        .filter(|event| event["event"] == "profile-total" && event["profile"] == model_profile)
        .collect::<Vec<_>>();
    profile_totals.len() == 1
        && !exhaustive.is_empty()
        && exhaustive.iter().all(|event| event["status"] == "pass")
        && profile_totals[0]["status"] == "pass"
        && profile_totals[0]["target_protocol_states"] == protocol_floor
        && profile_totals[0]["target_verifier_states"] == verifier_floor
        && profile_totals[0]["unique_protocol_states"] == protocol_total
        && profile_totals[0]["unique_verifier_states"] == verifier_total
        && protocol_total >= protocol_floor
        && verifier_total >= verifier_floor
}

fn soak_seeds_are_rederived(model_profile: &str, expected_seeds: &str, events: &[Value]) -> bool {
    let expected_values = expected_seeds
        .split(',')
        .map(|seed| u64::from_str_radix(seed.trim_start_matches("0x"), 16))
        .collect::<Result<Vec<_>, _>>();
    let Ok(expected_values) = expected_values else {
        return false;
    };
    let expected_checks = [
        format!("{model_profile}-soak"),
        format!("{model_profile}-soak-lease"),
        format!("{model_profile}-soak-membership"),
    ];
    let expected = expected_checks
        .iter()
        .flat_map(|check| {
            expected_values
                .iter()
                .map(move |seed| (check.clone(), *seed))
        })
        .collect::<BTreeSet<_>>();
    let mut observed = BTreeMap::<(String, u64), usize>::new();
    for event in events.iter().filter(|event| event["event"] == "soak-check") {
        let (Some(check), Some(seed)) = (event["check_id"].as_str(), event["seed"].as_u64()) else {
            return false;
        };
        if event["status"] != "pass" || !check.starts_with(&format!("{model_profile}-soak")) {
            return false;
        }
        *observed.entry((check.to_owned(), seed)).or_default() += 1;
    }
    observed.len() == expected.len()
        && observed.keys().cloned().collect::<BTreeSet<_>>() == expected
        && observed.values().all(|count| *count == 1)
}

fn simulator_events(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<BTreeMap<String, Vec<Value>>, AggregateError> {
    let logs = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "simulator-log")
        .collect::<Vec<_>>();
    if logs.is_empty() {
        return Err(AggregateError::new(
            "simulator execution has no machine-readable logs".to_owned(),
        ));
    }
    let mut events = BTreeMap::<String, Vec<Value>>::new();
    for log in logs {
        let source = fs::read_to_string(root.join(&log.path)).map_err(|error| {
            AggregateError::new(format!("read simulator log {}: {error}", log.path))
        })?;
        for line in source
            .lines()
            .filter_map(|line| line.strip_prefix(EVENT_PREFIX))
        {
            let event: Value = serde_json::from_str(line).map_err(|error| {
                AggregateError::new(format!("parse simulator event in {}: {error}", log.path))
            })?;
            let check_id = event["check_id"].as_str().ok_or_else(|| {
                AggregateError::new(format!("simulator event in {} lacks check_id", log.path))
            })?;
            events
                .entry(check_id.to_owned())
                .or_default()
                .push(event.clone());
            if let Some(canonical) = crate::producer::canonical_check_id(&bundle.profile, check_id)
            {
                events.entry(canonical).or_default().push(event);
            }
        }
    }
    Ok(events)
}

fn verify_simulator_observations(
    check: &crate::CheckReceipt,
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<(), AggregateError> {
    let names = check
        .observations
        .keys()
        .filter_map(|key| key.strip_prefix("runs:"))
        .collect::<Vec<_>>();
    if names.is_empty() {
        return Err(AggregateError::new(format!(
            "simulator receipt {} names no executed model check",
            check.check_id
        )));
    }
    let mut derived = BTreeMap::new();
    for name in names {
        let matching = events.get(name).map(Vec::as_slice).unwrap_or_default();
        derived.insert(format!("runs:{name}"), matching.len() as u64);
        derived.insert(
            format!("passes:{name}"),
            matching
                .iter()
                .filter(|event| event["status"] == "pass")
                .count() as u64,
        );
        derived.insert(
            format!("steps:{name}"),
            matching
                .iter()
                .filter_map(|event| event["steps"].as_u64())
                .min()
                .unwrap_or_default(),
        );
        for event in matching {
            merge_event_observations(event, &mut derived);
        }
    }
    let claimed = check
        .observations
        .iter()
        .filter(|(name, _)| name.as_str() != "detector_qualified")
        .map(|(name, value)| (name.clone(), *value))
        .collect::<BTreeMap<_, _>>();
    if claimed != derived {
        return Err(AggregateError::new(format!(
            "simulator receipt observations disagree with logs for {}",
            check.check_id
        )));
    }
    Ok(())
}

fn merge_event_observations(event: &Value, observations: &mut BTreeMap<String, u64>) {
    for field in ["unique_protocol_states", "unique_verifier_states"] {
        if let Some(value) = event[field].as_u64() {
            observations
                .entry(field.to_owned())
                .and_modify(|current| *current = (*current).max(value))
                .or_insert(value);
        }
    }
    if let Some(values) = event["observations"].as_object() {
        for (name, value) in values {
            if let Some(value) = value.as_u64() {
                *observations.entry(name.clone()).or_default() += value;
            }
        }
    }
}

fn read_artifact_kind(
    check: &crate::CheckReceipt,
    kind: &str,
    root: &Path,
) -> Result<String, AggregateError> {
    let artifact = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == kind)
        .ok_or_else(|| AggregateError::new(format!("{kind} missing for {}", check.check_id)))?;
    fs::read_to_string(root.join(&artifact.path))
        .map_err(|error| AggregateError::new(format!("read {kind} {}: {error}", artifact.path)))
}

fn require_exact_test_pass(
    source: &str,
    test_name: &str,
    check_id: &str,
) -> Result<(), AggregateError> {
    let full_result = format!("test {test_name} ... ok");
    let exact_result = format!("::{test_name} ... ok");
    if !source.lines().any(|line| line.trim() == "running 1 test")
        || !source
            .lines()
            .any(|line| line.trim() == full_result || line.trim_end().ends_with(&exact_result))
        || !source
            .lines()
            .any(|line| line.contains("1 passed; 0 failed; 0 ignored"))
    {
        return Err(AggregateError::new(format!(
            "test log does not prove one exact pass for {check_id}"
        )));
    }
    Ok(())
}

fn is_passing(bundle: &ResultBundle, execution_id: &str) -> bool {
    bundle
        .results
        .iter()
        .any(|result| result.execution_id == execution_id && result.status == EvidenceStatus::Pass)
}

#[cfg(test)]
mod tests {
    use super::validate_simulator_schedule;
    use crate::producer::expected_scheduled_seeds;
    use serde_json::json;

    #[test]
    fn scheduled_simulator_log_proves_exact_source_derived_plan() {
        let source_ref = "abc123";
        let log = scheduled_log(source_ref);

        assert!(
            validate_simulator_schedule("nightly", source_ref, std::slice::from_ref(&log)).is_ok()
        );
        assert!(validate_simulator_schedule("nightly", "different", &[log]).is_err());
        assert!(validate_simulator_schedule("nightly", source_ref, &[]).is_err());
    }

    #[test]
    fn scheduled_simulator_rejects_fabricated_totals_and_executed_seeds() {
        let source_ref = "abc123";
        let log = scheduled_log(source_ref);
        let fabricated_total = log.replace(
            "\"unique_protocol_states\":40000000",
            "\"unique_protocol_states\":4",
        );
        assert!(validate_simulator_schedule("nightly", source_ref, &[fabricated_total]).is_err());

        let wrong_seed = log.replacen("\"seed\":", "\"seed\":999,\"ignored_seed\":", 1);
        assert!(validate_simulator_schedule("nightly", source_ref, &[wrong_seed]).is_err());
    }

    #[test]
    fn scheduled_seed_banner_uses_simulator_canonical_hex() {
        let seeds = expected_scheduled_seeds("weekly", "abc123").expect("weekly seeds");
        assert!(seeds.contains("0xe00e6256b8bdd15"));
        assert!(!seeds.contains("0x0e00e6256b8bdd15"));
    }

    fn scheduled_log(source_ref: &str) -> String {
        let seeds = expected_scheduled_seeds("nightly", source_ref).expect("nightly seeds");
        let mut lines = vec![
            "label: raft-nightly".to_owned(),
            "exit_code: Some(0)".to_owned(),
            "model-check profile=raft-nightly expected_runtime=scheduled".to_owned(),
            format!("model-check raft-nightly-soak seeds source=replay seeds={seeds}"),
            event(&json!({
                "event": "exhaustive-check",
                "check_id": "raft-election-nightly",
                "status": "pass",
                "unique_protocol_states": 40_000_000,
                "unique_verifier_states": 40_000_000,
            })),
            event(&json!({
                "event": "exhaustive-check",
                "check_id": "raft-commit-nightly",
                "status": "pass",
                "unique_protocol_states": 60_000_000,
                "unique_verifier_states": 60_000_000,
            })),
            event(&json!({
                "event": "profile-total",
                "check_id": "raft-profile-total-nightly",
                "profile": "raft-nightly",
                "status": "pass",
                "unique_protocol_states": 100_000_000,
                "unique_verifier_states": 100_000_000,
                "target_protocol_states": 100_000_000,
                "target_verifier_states": 100_000_000,
            })),
        ];
        for seed in seeds.split(',') {
            let seed = u64::from_str_radix(seed.trim_start_matches("0x"), 16).expect("hex seed");
            for check_id in [
                "raft-nightly-soak",
                "raft-nightly-soak-lease",
                "raft-nightly-soak-membership",
            ] {
                lines.push(event(&json!({
                    "event": "soak-check",
                    "check_id": check_id,
                    "status": "pass",
                    "seed": seed,
                })));
            }
        }
        format!("{}\n", lines.join("\n"))
    }

    fn event(value: &serde_json::Value) -> String {
        format!("{}{}", super::EVENT_PREFIX, value)
    }
}
