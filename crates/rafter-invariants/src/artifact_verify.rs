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
        &bundle.execution.plan.result_schema,
        &bundle.execution.plan.verdict_schema,
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
    verify_producer_invocation_paths(bundle, root)?;
    verify_resource_metrics(bundle, root)?;
    verify_compile_invocations(bundle, root)?;
    match bundle.runner.as_str() {
        "tests" => verify_test_logs(bundle, root),
        "simulator" => verify_simulator_logs(bundle, root),
        "tla" => crate::artifact_verify_tla::verify(bundle, root),
        "maelstrom" => crate::artifact_verify_maelstrom::verify(bundle, root),
        _ => Ok(()),
    }
}

fn verify_producer_invocation_paths(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<(), AggregateError> {
    let repository = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize producer root: {error}")))?;
    let current_dir =
        fs::canonicalize(&bundle.execution.invocation.current_dir).map_err(|error| {
            AggregateError::new(format!("canonicalize producer working directory: {error}"))
        })?;
    if current_dir != repository {
        return Err(AggregateError::new(
            "producer working directory does not match the canonical source checkout".to_owned(),
        ));
    }
    let program = fs::canonicalize(&bundle.execution.invocation.program)
        .map_err(|error| AggregateError::new(format!("canonicalize producer program: {error}")))?;
    if !program.starts_with(&repository) {
        return Err(AggregateError::new(
            "producer program is outside the canonical source checkout".to_owned(),
        ));
    }
    let binaries = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "producer-binary")
        .collect::<Vec<_>>();
    let [binary] = binaries.as_slice() else {
        return Err(AggregateError::new(
            "producer invocation requires exactly one preserved binary".to_owned(),
        ));
    };
    let program_digest =
        format!(
            "{:x}",
            Sha256::digest(fs::read(program).map_err(|error| {
                AggregateError::new(format!("read producer program: {error}"))
            })?)
        );
    if program_digest != binary.sha256
        || program_digest != bundle.execution.invocation.program_sha256
    {
        return Err(AggregateError::new(
            "claimed producer program does not match the preserved producer binary".to_owned(),
        ));
    }
    Ok(())
}

fn verify_resource_metrics(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    for check in &bundle.execution.checks {
        let artifacts = check_metric_artifacts(&bundle.runner, &check.artifacts);
        let derived = derive_process_metrics(artifacts.into_iter(), root)?;
        if check.duration_ms != derived.duration_ms || check.peak_rss_kib != derived.peak_rss_kib {
            return Err(AggregateError::new(format!(
                "check resource metrics disagree with hashed process logs for {}",
                check.check_id
            )));
        }
    }

    let artifacts = bundle
        .execution
        .artifacts
        .iter()
        .chain(
            bundle
                .execution
                .checks
                .iter()
                .flat_map(|check| check.artifacts.iter()),
        )
        .filter(|artifact| is_process_log_kind(&artifact.kind));
    let derived = derive_process_metrics(artifacts, root)?;
    if bundle.execution.duration_ms != derived.duration_ms
        || bundle.execution.peak_rss_kib != derived.peak_rss_kib
    {
        return Err(AggregateError::new(
            "execution resource metrics disagree with hashed process logs".to_owned(),
        ));
    }
    Ok(())
}

fn check_metric_artifacts<'a>(
    runner: &str,
    artifacts: &'a [crate::ArtifactRef],
) -> Vec<&'a crate::ArtifactRef> {
    let has_runtime = artifacts.iter().any(|artifact| {
        matches!(
            artifact.kind.as_str(),
            "test-log" | "simulator-log" | "maelstrom-process-log"
        ) || is_tla_process_log(&artifact.kind)
    });
    artifacts
        .iter()
        .filter(|artifact| {
            is_process_log_kind(&artifact.kind)
                && (runner == "tla" || artifact.kind != "compile-log" || !has_runtime)
        })
        .collect()
}

fn derive_process_metrics<'a>(
    artifacts: impl Iterator<Item = &'a crate::ArtifactRef>,
    root: &Path,
) -> Result<crate::producer::process::ProcessMetrics, AggregateError> {
    let mut duration_ms = 0_u64;
    let mut peak_rss_kib = 0_u64;
    let mut paths = BTreeSet::new();
    for artifact in artifacts {
        if !paths.insert(artifact.path.as_str()) {
            continue;
        }
        let bytes = fs::read(root.join(&artifact.path)).map_err(|error| {
            AggregateError::new(format!("read process log {}: {error}", artifact.path))
        })?;
        let metrics = process_log_metrics(&artifact.kind, &bytes).map_err(|error| {
            AggregateError::new(format!("parse process log {}: {error}", artifact.path))
        })?;
        for metric in metrics {
            duration_ms = duration_ms.checked_add(metric.duration_ms).ok_or_else(|| {
                AggregateError::new("process duration total overflowed".to_owned())
            })?;
            peak_rss_kib = peak_rss_kib.max(metric.peak_rss_kib);
        }
    }
    if paths.is_empty() || peak_rss_kib == 0 {
        return Err(AggregateError::new(
            "receipt has no measurable hashed process logs".to_owned(),
        ));
    }
    Ok(crate::producer::process::ProcessMetrics {
        duration_ms,
        peak_rss_kib,
    })
}

fn process_log_metrics(
    kind: &str,
    bytes: &[u8],
) -> Result<Vec<crate::producer::process::ProcessMetrics>, String> {
    if matches!(kind, "compile-log" | "test-log" | "simulator-log") {
        let source = std::str::from_utf8(bytes)
            .map_err(|error| format!("combined process log is not UTF-8: {error}"))?;
        return crate::producer::process::parse_combined_processes(source).map(|processes| {
            processes
                .into_iter()
                .map(|process| process.metrics)
                .collect()
        });
    }
    let process: crate::producer::ProcessLog =
        serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    if process.peak_rss_kib == 0 {
        return Err("structured process log omitted peak RSS".to_owned());
    }
    Ok(vec![crate::producer::process::ProcessMetrics {
        duration_ms: process.duration_ms,
        peak_rss_kib: process.peak_rss_kib,
    }])
}

fn is_process_log_kind(kind: &str) -> bool {
    matches!(
        kind,
        "compile-log" | "test-log" | "simulator-log" | "maelstrom-process-log"
    ) || is_tla_process_log(kind)
}

fn is_tla_process_log(kind: &str) -> bool {
    matches!(kind, "tla-log" | "tla-trace-log") || kind.starts_with("tla-detector-log")
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
        if bundle.runner == "tests" || observed.label != "simulator compile" {
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
    let source_prefix = bundle.source_ref.get(..12).unwrap_or(&bundle.source_ref);
    let execution_profile = test_execution_profile(bundle);
    let expected_target =
        format!("target/rafter-invariants/build/{source_prefix}/{execution_profile}-tests");
    let mut base_environment = observed.invocation.environment.clone();
    let target_dir = base_environment.remove("CARGO_TARGET_DIR");
    if observed.invocation.arguments != expected
        || target_dir.as_deref() != Some(expected_target.as_str())
        || crate::producer::process::digest_environment(&base_environment)
            != bundle.execution.source.environment_sha256
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
    if observed.label != "simulator compile" {
        return Err(AggregateError::new(
            "simulator compile log has the wrong label".to_owned(),
        ));
    }
    if observed.invocation.arguments != expected_arguments {
        return Err(AggregateError::new(
            "simulator compile log has the wrong Cargo arguments".to_owned(),
        ));
    }
    if target.as_deref() != Some(expected_target.as_str()) {
        return Err(AggregateError::new(
            "simulator compile log has the wrong Cargo target directory".to_owned(),
        ));
    }
    if crate::producer::process::digest_environment(&base_environment)
        != bundle.execution.source.environment_sha256
    {
        return Err(AggregateError::new(
            "simulator compile log has the wrong base environment".to_owned(),
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
    let catalog =
        crate::Catalog::load(root.join(&bundle.execution.plan.registry.path).as_path())
            .map_err(|error| AggregateError::new(format!("reload simulator registry: {error}")))?;
    let descriptors = catalog
        .evidence
        .iter()
        .map(|descriptor| (descriptor.evidence_id(), descriptor))
        .collect::<BTreeMap<_, _>>();
    let liveness_contracts = catalog
        .evidence
        .iter()
        .filter_map(|descriptor| {
            descriptor
                .simulator
                .as_ref()?
                .liveness_report
                .as_ref()
                .map(|contract| (contract.feature_id.clone(), contract.clone()))
        })
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    let mut test_logs = BTreeMap::<String, String>::new();
    for check in &bundle.execution.checks {
        let [evidence_id] = check.evidence_ids.as_slice() else {
            return Err(AggregateError::new(format!(
                "simulator check {} must bind exactly one evidence record",
                check.check_id
            )));
        };
        let descriptor = descriptors.get(evidence_id).ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {} names unknown evidence {evidence_id}",
                check.check_id
            ))
        })?;
        let identity = descriptor.simulator.as_ref().ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {} names non-simulator evidence",
                check.check_id
            ))
        })?;
        verify_simulator_observations(bundle, check, identity, &liveness_contracts, &events)?;
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
    let configuration = bundle
        .execution
        .plan
        .contract
        .runners
        .get("simulator")
        .ok_or_else(|| AggregateError::new("simulator runner contract is missing".to_owned()))?
        .simulator_configuration()
        .map_err(|error| {
            AggregateError::new(format!("parse typed simulator runner contract: {error}"))
        })?;
    configuration
        .validate_profile(&bundle.profile)
        .map_err(|error| AggregateError::new(format!("validate simulator contract: {error}")))?;
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
    validate_simulator_schedule(
        &bundle.profile,
        &bundle.source_ref,
        &configuration,
        &sources,
    )
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
    let exact_digest = exact_test_environment_digest(bundle, check, &invocations, test_name)?;
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
        || (exact_arguments[0] != test_name
            && !exact_arguments[0].ends_with(&format!("::{test_name}")))
        || exact_arguments[1..5] != ["--exact", "--test-threads=1", "--color", "never"]
        || (exact_arguments.len() == 6 && exact_arguments[5] != "--ignored")
        || exact_arguments.len() > 6
    {
        return Err(AggregateError::new(format!(
            "test log does not contain the exact libtest argument plan for {test_name}: {exact_arguments:?}"
        )));
    }
    if exact.invocation.environment_sha256 != exact_digest {
        return Err(AggregateError::new(
            "test log does not contain the exact execution environment".to_owned(),
        ));
    }
    if invocations
        .iter()
        .any(|invocation| invocation.invocation.program_sha256 != binary.sha256)
    {
        return Err(AggregateError::new(
            "test log executable digest does not match its binary artifact".to_owned(),
        ));
    }
    if invocations
        .iter()
        .any(|invocation| invocation.invocation.current_dir != current_dir)
    {
        return Err(AggregateError::new(
            "test log working directory does not match the active checkout".to_owned(),
        ));
    }
    if invocations
        .iter()
        .any(|invocation| !Path::new(&invocation.invocation.program).is_absolute())
    {
        return Err(AggregateError::new(
            "test log executable path is not absolute".to_owned(),
        ));
    }
    Ok(())
}

fn exact_test_environment_digest(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    invocations: &[crate::producer::process::LabeledInvocation],
    test_name: &str,
) -> Result<String, AggregateError> {
    let execution_id = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "test-log")
        .and_then(|artifact| Path::new(&artifact.path).file_stem())
        .and_then(|value| value.to_str())
        .ok_or_else(|| AggregateError::new("test log path has no execution ID".to_owned()))?;
    let execution_profile = test_execution_profile(bundle);
    let executed_test_name = invocations
        .get(2)
        .and_then(|invocation| invocation.invocation.arguments.first())
        .map_or(test_name, String::as_str);
    let seed = crate::producer::artifact::deterministic_u64(
        "rafter-tests/v1",
        &format!(
            "{execution_profile}\0{}\0{executed_test_name}",
            bundle.source_ref
        ),
    );
    let mut environment = invocations
        .first()
        .map(|invocation| invocation.invocation.environment.clone())
        .unwrap_or_default();
    environment.extend([
        ("PROPTEST_RNG_SEED".to_owned(), seed.to_string()),
        (
            "PROPTEST_DISABLE_FAILURE_PERSISTENCE".to_owned(),
            "1".to_owned(),
        ),
        (
            "TMPDIR".to_owned(),
            Path::new("target/rafter-invariants/tmp")
                .join(execution_id)
                .to_string_lossy()
                .into_owned(),
        ),
        ("RUST_BACKTRACE".to_owned(), "1".to_owned()),
    ]);
    Ok(crate::producer::process::digest_environment(&environment))
}

fn test_execution_profile(bundle: &ResultBundle) -> String {
    if bundle.runner == "simulator" {
        format!("{}-simulator-detectors", bundle.profile)
    } else {
        bundle.profile.clone()
    }
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
    configuration: &crate::catalog::SimulatorRunnerConfiguration,
    logs: &[String],
) -> Result<(), AggregateError> {
    if profile == "pr" {
        return validate_pr_soak_schedule(configuration, logs);
    }
    let seed_count = configuration
        .seed_count
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| AggregateError::new("scheduled seed count is missing".to_owned()))?;
    let Some(expected_seeds) =
        crate::producer::expected_scheduled_seeds_with_count(profile, source_ref, seed_count)
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
        || !profile_total_is_rederived(&model_profile, &configuration.state_floors, &events)
        || !soak_seeds_are_rederived(
            &model_profile,
            &expected_seeds,
            configuration.soak_steps,
            &events,
        )
    {
        return Err(AggregateError::new(format!(
            "scheduled simulator log does not prove the source-derived {profile} execution plan"
        )));
    }
    Ok(())
}

fn validate_pr_soak_schedule(
    configuration: &crate::catalog::SimulatorRunnerConfiguration,
    logs: &[String],
) -> Result<(), AggregateError> {
    const EXPECTED_SEEDS: &str = "0x9103,0x9104,0x9105,0x9106";
    if logs.len() != 2 {
        return Err(AggregateError::new(format!(
            "PR simulator receipt must retain exactly two profile logs, found {}",
            logs.len()
        )));
    }
    let expected_seed_line =
        format!("model-check raft-soak seeds source=curated seeds={EXPECTED_SEEDS}");
    let seed_line_count = logs
        .iter()
        .flat_map(|log| log.lines())
        .filter(|line| *line == expected_seed_line)
        .count();
    let events = logs
        .iter()
        .map(|log| parse_machine_events(log))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if seed_line_count != 1
        || !soak_seeds_are_rederived("raft", EXPECTED_SEEDS, configuration.soak_steps, &events)
    {
        return Err(AggregateError::new(
            "PR simulator log does not prove the exact reviewed soak seed inventory".to_owned(),
        ));
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

fn profile_total_is_rederived(
    model_profile: &str,
    state_floors: &crate::catalog::SimulatorStateFloors,
    events: &[Value],
) -> bool {
    let (protocol_floor, verifier_floor) = match state_floors {
        crate::catalog::SimulatorStateFloors::Aggregate { protocol, verifier } => {
            (*protocol, *verifier)
        }
        crate::catalog::SimulatorStateFloors::PerEvidence => return false,
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

fn soak_seeds_are_rederived(
    model_profile: &str,
    expected_seeds: &str,
    expected_steps: u64,
    events: &[Value],
) -> bool {
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
        let (Some(check), Some(seed), Some(steps)) = (
            event["check_id"].as_str(),
            event["seed"].as_u64(),
            event["steps"].as_u64(),
        ) else {
            return false;
        };
        if event["status"] != "pass"
            || steps != expected_steps
            || !check.starts_with(&format!("{model_profile}-soak"))
        {
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
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    identity: &crate::SimulatorIdentity,
    liveness_contracts: &[crate::types::SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<(), AggregateError> {
    if identity.checks.is_empty() {
        return Err(AggregateError::new(format!(
            "simulator receipt {} names no executed model check",
            check.check_id
        )));
    }
    let mut derived = BTreeMap::new();
    for name in &identity.checks {
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
        if identity.liveness_report.is_none() {
            for event in matching {
                merge_event_observations(event, &mut derived);
            }
        }
    }
    if identity.liveness_report.is_some() {
        if is_passing(bundle, &check.execution_id) {
            let binding = crate::catalog::derive_liveness_binding(
                &bundle.profile,
                identity,
                liveness_contracts,
                events,
            )
            .map_err(|error| {
                AggregateError::new(format!(
                    "simulator raw liveness reports are invalid for {}: {}",
                    check.check_id, error.message
                ))
            })?;
            derived.insert(
                identity.required_observation.clone(),
                binding.reports.len() as u64,
            );
            if check.simulator_liveness.as_ref() != Some(&binding) {
                return Err(AggregateError::new(format!(
                    "simulator liveness binding disagrees with raw logs for {}",
                    check.check_id
                )));
            }
        } else {
            derived.insert(identity.required_observation.clone(), 0);
            if check.simulator_liveness.is_some() {
                return Err(AggregateError::new(format!(
                    "non-passing simulator check {} retains a liveness binding",
                    check.check_id
                )));
            }
        }
    } else if check.simulator_liveness.is_some() {
        return Err(AggregateError::new(format!(
            "simulator safety check {} retains a liveness binding",
            check.check_id
        )));
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
#[path = "artifact_verify/tests.rs"]
mod tests;
