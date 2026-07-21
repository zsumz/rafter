//! Simulator schedule, invocation, and compiler provenance verification.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use serde_json::Value;

use crate::{
    verification::{AggregateError, AuthenticatedArtifacts},
    ResultBundle,
};

mod events;

pub(super) use events::{scan_machine_events, ScannedSimulatorLog};

const SIMULATOR_PACKAGE: &str = "rafter-sim";
const SIMULATOR_PACKAGE_VERSION: &str = "0.0.1";
const SIMULATOR_TARGET: &str = "rafter-model-check-fast";

struct InvocationVerification {
    diagnostics: Vec<String>,
    complete: bool,
}

struct SimulatorRoots {
    producer: PathBuf,
    active: PathBuf,
}

pub(super) struct VerifiedSimulatorSchedule<'a> {
    pub(super) diagnostics: Vec<String>,
    pub(super) logs: Vec<ScannedSimulatorLog<'a>>,
}

pub(super) fn verify_simulator_schedule_authenticated<'a>(
    bundle: &ResultBundle,
    root: &Path,
    authenticated: &'a AuthenticatedArtifacts,
) -> Result<VerifiedSimulatorSchedule<'a>, AggregateError> {
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
        .map(|log| authenticated.text(log))
        .collect::<Result<Vec<_>, _>>()?;
    let invocation = verify_simulator_invocations(bundle, root, &sources, authenticated)?;
    let mut diagnostics = invocation.diagnostics;
    let mut event_diagnostics = Vec::new();
    let scanned_logs = sources
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let (events, diagnostics) =
                scan_machine_events(source, &format!("simulator log {index}"));
            event_diagnostics.extend(diagnostics);
            ScannedSimulatorLog { source, events }
        })
        .collect::<Vec<_>>();
    if invocation.complete && event_diagnostics.is_empty() {
        validate_scanned_simulator_schedule(
            &bundle.profile,
            &bundle.source_ref,
            &configuration,
            &scanned_logs,
        )?;
    }
    diagnostics.extend(event_diagnostics);
    Ok(VerifiedSimulatorSchedule {
        diagnostics,
        logs: scanned_logs,
    })
}

#[cfg(test)]
pub(super) fn verify_simulator_schedule(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<Vec<String>, AggregateError> {
    let authenticated = crate::verification::snapshot_available_artifacts(bundle, root)?;
    verify_simulator_schedule_authenticated(bundle, root, &authenticated)
        .map(|verified| verified.diagnostics)
}

fn verify_simulator_invocations(
    bundle: &ResultBundle,
    root: &Path,
    sources: &[&str],
    authenticated: &AuthenticatedArtifacts,
) -> Result<InvocationVerification, AggregateError> {
    let roots = simulator_roots(bundle, root)?;
    let binaries = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "simulator-binary")
        .collect::<Vec<_>>();
    let [binary] = binaries.as_slice() else {
        return Err(AggregateError::new(format!(
            "simulator execution must capture exactly one binary artifact, found {}",
            binaries.len()
        )));
    };
    let emitted = emitted_simulator_executable(bundle, &roots, authenticated)?;
    let environment = &bundle.execution.invocation.environment;
    let environment_sha256 = bundle.execution.invocation.environment_sha256.as_str();
    let expected = expected_simulator_invocations(bundle)?;
    if sources.len() > expected.len() {
        return Err(AggregateError::new(
            "simulator log count exceeds the execution plan".to_owned(),
        ));
    }
    let mut diagnostics = Vec::new();
    let expected_count = expected.len();
    let mut matched = 0_usize;
    for (label, arguments) in expected {
        let Some(source) = sources
            .iter()
            .find(|source| source.lines().any(|line| line == format!("label: {label}")))
        else {
            diagnostics.push(format!(
                "simulator execution plan did not run required profile {label}"
            ));
            continue;
        };
        matched += 1;
        let processes = crate::evidence::format::process::parse_combined_v4(source)
            .map_err(|error| AggregateError::new(format!("parse simulator invocation: {error}")))?;
        let [observed] = processes.as_slice() else {
            return Err(AggregateError::new(format!(
                "simulator log {label} must contain exactly one invocation"
            )));
        };
        if let Err(error) =
            verify_simulator_invocation_outcome(&label, observed.exit_code, observed.timed_out)
        {
            diagnostics.push(error.to_string());
        }
        if observed.label != label
            || observed.invocation.arguments != arguments
            || !simulator_program_matches(&observed.invocation, &emitted, &binary.sha256)
            || !crate::receipt::process_invocation_matches_source(
                &observed.invocation,
                &bundle.execution.source,
            )
            || Path::new(&observed.invocation.current_dir) != roots.producer
            || !invocation_environment_matches(
                &observed.invocation,
                environment,
                environment_sha256,
            )
        {
            return Err(AggregateError::new(format!(
                "simulator log {label} does not match the exact invocation plan"
            )));
        }
    }
    if matched != sources.len() {
        return Err(AggregateError::new(
            "simulator logs contain an unexpected or duplicate invocation".to_owned(),
        ));
    }
    Ok(InvocationVerification {
        diagnostics,
        complete: matched == expected_count,
    })
}

fn expected_simulator_invocations(
    bundle: &ResultBundle,
) -> Result<Vec<(String, Vec<String>)>, AggregateError> {
    match bundle.profile.as_str() {
        "pr" => Ok(vec![
            (
                "fast".to_owned(),
                vec!["--profile".to_owned(), "fast".to_owned()],
            ),
            (
                "raft-soak".to_owned(),
                vec!["--profile".to_owned(), "raft-soak".to_owned()],
            ),
        ]),
        profile @ ("nightly" | "weekly") => {
            let label = format!("raft-{profile}");
            let seeds = crate::producer::expected_scheduled_seeds(profile, &bundle.source_ref)
                .ok_or_else(|| AggregateError::new("scheduled seeds are missing".to_owned()))?;
            Ok(vec![(
                label.clone(),
                vec!["--profile".to_owned(), label, "--seed".to_owned(), seeds],
            )])
        }
        profile => Err(AggregateError::new(format!(
            "unknown simulator profile {profile}"
        ))),
    }
}

fn invocation_environment_matches(
    invocation: &crate::InvocationReceipt,
    expected: &std::collections::BTreeMap<String, String>,
    expected_digest: &str,
) -> bool {
    invocation.environment == *expected
        && invocation.environment_sha256 == expected_digest
        && crate::provenance::invocation::environment_matches_digest(
            &invocation.environment,
            expected_digest,
        )
}

fn simulator_roots(bundle: &ResultBundle, root: &Path) -> Result<SimulatorRoots, AggregateError> {
    let producer = PathBuf::from(&bundle.execution.invocation.current_dir);
    if !clean_absolute_path(&producer) {
        return Err(AggregateError::new(
            "simulator producer root must be a clean absolute path".to_owned(),
        ));
    }
    let active = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize simulator root: {error}")))?;
    if !clean_absolute_path(&active) {
        return Err(AggregateError::new(
            "simulator active root is not a clean canonical path".to_owned(),
        ));
    }
    Ok(SimulatorRoots { producer, active })
}

fn verify_simulator_invocation_outcome(
    label: &str,
    exit_code: Option<i32>,
    timed_out: bool,
) -> Result<(), AggregateError> {
    if exit_code != Some(0) || timed_out {
        return Err(AggregateError::new(format!(
            "simulator log {label} requires a zero-exit invocation that did not time out"
        )));
    }
    Ok(())
}

fn emitted_simulator_executable(
    bundle: &ResultBundle,
    roots: &SimulatorRoots,
    authenticated: &AuthenticatedArtifacts,
) -> Result<PathBuf, AggregateError> {
    let mut executables = Vec::new();
    for artifact in bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "compile-log")
    {
        let source = authenticated.text(artifact)?;
        let processes =
            crate::evidence::format::process::parse_combined_v4(source).map_err(|error| {
                AggregateError::new(format!(
                    "parse simulator compile log {}: {error}",
                    artifact.path
                ))
            })?;
        for process in processes
            .iter()
            .filter(|process| process.label == "simulator compile")
        {
            if process.exit_code != Some(0) || process.timed_out {
                return Err(AggregateError::new(
                    "simulator compiler-artifact provenance requires a successful build".to_owned(),
                ));
            }
            let target_dir = simulator_compile_target_dir(bundle, process, roots)?;
            let named_executable = super::compile::compiler_artifact_executable(
                process.stdout.as_bytes(),
                SIMULATOR_TARGET,
                "bin",
                "simulator compile",
            )?;
            let source_bound_executable = simulator_compiler_artifact_executable(
                process.stdout.as_bytes(),
                &roots.producer,
                &roots.active,
                &target_dir,
            )?;
            if named_executable != source_bound_executable {
                return Err(AggregateError::new(
                    "simulator compiler-artifact selectors resolved different executables"
                        .to_owned(),
                ));
            }
            executables.push(source_bound_executable);
        }
    }
    let [executable] = executables.as_slice() else {
        return Err(AggregateError::new(format!(
            "simulator compile logs must emit exactly one source-bound executable, found {}",
            executables.len()
        )));
    };
    Ok(executable.clone())
}

fn simulator_compile_target_dir(
    bundle: &ResultBundle,
    process: &crate::evidence::format::process::LabeledProcess,
    roots: &SimulatorRoots,
) -> Result<PathBuf, AggregateError> {
    let expected_arguments = [
        "build",
        "--release",
        "--locked",
        "-p",
        SIMULATOR_PACKAGE,
        "--bin",
        SIMULATOR_TARGET,
        "--message-format=json-render-diagnostics",
    ];
    let mut base_environment = process.invocation.environment.clone();
    let recorded_target_dir = base_environment.remove("CARGO_TARGET_DIR").ok_or_else(|| {
        AggregateError::new("simulator compile invocation omitted CARGO_TARGET_DIR".to_owned())
    })?;
    let target_dir = resolve_producer_path(&roots.producer, Path::new(&recorded_target_dir))?;
    let source_prefix = bundle.source_ref.get(..12).unwrap_or(&bundle.source_ref);
    let expected_target_dir = roots
        .producer
        .join("target/rafter-invariants/simulator-build")
        .join(source_prefix)
        .join(&bundle.profile);
    if process.label != "simulator compile"
        || process.invocation.program != "cargo"
        || process.invocation.program_sha256 != bundle.execution.source.cargo_sha256
        || !crate::receipt::process_invocation_matches_source(
            &process.invocation,
            &bundle.execution.source,
        )
        || process.invocation.arguments != expected_arguments
        || Path::new(&process.invocation.current_dir) != roots.producer
        || !crate::provenance::invocation::environment_matches_digest(
            &process.invocation.environment,
            &process.invocation.environment_sha256,
        )
        || base_environment != bundle.execution.invocation.environment
        || target_dir != expected_target_dir
    {
        return Err(AggregateError::new(
            "simulator compile log does not match its recorded producer invocation and source contract"
                .to_owned(),
        ));
    }
    Ok(target_dir)
}

fn resolve_producer_path(root: &Path, recorded: &Path) -> Result<PathBuf, AggregateError> {
    let resolved = if recorded.is_absolute() {
        recorded.to_owned()
    } else {
        if recorded
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(AggregateError::new(
                "simulator compile invocation recorded an unsafe relative Cargo target directory"
                    .to_owned(),
            ));
        }
        root.join(recorded)
    };
    if !clean_absolute_path(&resolved) {
        return Err(AggregateError::new(
            "simulator compile invocation recorded a non-canonical Cargo target directory"
                .to_owned(),
        ));
    }
    Ok(resolved)
}

fn simulator_compiler_artifact_executable(
    bytes: &[u8],
    producer_root: &Path,
    active_root: &Path,
    target_dir: &Path,
) -> Result<PathBuf, AggregateError> {
    if !clean_absolute_path(producer_root)
        || !clean_absolute_path(active_root)
        || !clean_absolute_path(target_dir)
    {
        return Err(AggregateError::new(
            "simulator compiler-artifact path contract is not canonical".to_owned(),
        ));
    }
    let producer_package = producer_root.join("crates").join(SIMULATOR_PACKAGE);
    let producer_source = producer_package
        .join("src/bin")
        .join(format!("{SIMULATOR_TARGET}.rs"));
    let active_package = active_root.join("crates").join(SIMULATOR_PACKAGE);
    let active_source = active_package
        .join("src/bin")
        .join(format!("{SIMULATOR_TARGET}.rs"));
    verify_active_path(&active_package, true, "simulator package")?;
    verify_active_path(&active_source, false, "simulator target source")?;
    let mut executables = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if message["reason"] != "compiler-artifact"
            || message["target"]["name"] != SIMULATOR_TARGET
            || !exact_string_array(&message["target"]["kind"], "bin")
            || !exact_string_array(&message["target"]["crate_types"], "bin")
        {
            continue;
        }
        if message["fresh"].as_bool() != Some(false) {
            return Err(AggregateError::new(
                "simulator compiler-artifact must record a non-fresh executable".to_owned(),
            ));
        }
        let package_id = message["package_id"].as_str().ok_or_else(|| {
            AggregateError::new("simulator compiler-artifact omitted Cargo package_id".to_owned())
        })?;
        let package_path = simulator_package_path(package_id)?;
        let mapped_package = map_producer_path(
            &package_path,
            producer_root,
            active_root,
            "simulator package_id",
        )?;
        if package_path != producer_package || mapped_package != active_package {
            return Err(AggregateError::new(
                "simulator compiler-artifact package_id does not match rafter-sim".to_owned(),
            ));
        }
        let src_path = message["target"]["src_path"].as_str().ok_or_else(|| {
            AggregateError::new(
                "simulator compiler-artifact omitted its target source path".to_owned(),
            )
        })?;
        let producer_src_path = Path::new(src_path);
        let mapped_source = map_producer_path(
            producer_src_path,
            producer_root,
            active_root,
            "simulator target source",
        )?;
        if producer_src_path != producer_source || mapped_source != active_source {
            return Err(AggregateError::new(
                "simulator compiler-artifact source path does not match the exact workspace bin target"
                    .to_owned(),
            ));
        }
        let executable = message["executable"].as_str().ok_or_else(|| {
            AggregateError::new("simulator compiler-artifact omitted its executable".to_owned())
        })?;
        let executable = PathBuf::from(executable);
        let expected_executable = target_dir.join("release").join(format!(
            "{SIMULATOR_TARGET}{}",
            std::env::consts::EXE_SUFFIX
        ));
        if !clean_absolute_path(&executable) || executable != expected_executable {
            return Err(AggregateError::new(
                "simulator compiler-artifact executable does not match the exact release target"
                    .to_owned(),
            ));
        }
        executables.push(executable);
    }
    let [executable] = executables.as_slice() else {
        return Err(AggregateError::new(format!(
            "simulator compile log must preserve exactly one package- and source-bound executable, found {}",
            executables.len()
        )));
    };
    Ok(executable.clone())
}

fn simulator_package_path(package_id: &str) -> Result<PathBuf, AggregateError> {
    let encoded = package_id.strip_prefix("path+file://").ok_or_else(|| {
        AggregateError::new(
            "simulator compiler-artifact package_id is not a workspace path package".to_owned(),
        )
    })?;
    let (path, version) = encoded.rsplit_once('#').ok_or_else(|| {
        AggregateError::new("simulator compiler-artifact package_id has no version".to_owned())
    })?;
    if version != SIMULATOR_PACKAGE_VERSION {
        return Err(AggregateError::new(
            "simulator compiler-artifact package_id has the wrong rafter-sim version".to_owned(),
        ));
    }
    Ok(PathBuf::from(path))
}

fn map_producer_path(
    path: &Path,
    producer_root: &Path,
    active_root: &Path,
    context: &str,
) -> Result<PathBuf, AggregateError> {
    if !clean_absolute_path(path) {
        return Err(AggregateError::new(format!(
            "{context} is not a clean absolute producer path"
        )));
    }
    let relative = path.strip_prefix(producer_root).map_err(|_| {
        AggregateError::new(format!("{context} escapes the recorded producer root"))
    })?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(AggregateError::new(format!(
            "{context} cannot be safely mapped into the active root"
        )));
    }
    Ok(active_root.join(relative))
}

fn verify_active_path(path: &Path, directory: bool, context: &str) -> Result<(), AggregateError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| AggregateError::new(format!("read active {context}: {error}")))?;
    let canonical = fs::canonicalize(path)
        .map_err(|error| AggregateError::new(format!("canonicalize active {context}: {error}")))?;
    let expected_type = if directory {
        metadata.file_type().is_dir()
    } else {
        metadata.file_type().is_file()
    };
    if !expected_type || canonical != path {
        return Err(AggregateError::new(format!(
            "active {context} is not the exact non-symlink workspace path"
        )));
    }
    Ok(())
}

fn exact_string_array(value: &Value, expected: &str) -> bool {
    value
        .as_array()
        .is_some_and(|values| values.len() == 1 && values[0].as_str() == Some(expected))
}

fn clean_absolute_path(path: &Path) -> bool {
    if !path.is_absolute() || path.components().collect::<PathBuf>() != path {
        return false;
    }
    let mut has_normal_component = false;
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {}
            Component::Normal(_) => has_normal_component = true,
            Component::CurDir | Component::ParentDir => return false,
        }
    }
    has_normal_component
}

fn simulator_program_matches(
    invocation: &crate::InvocationReceipt,
    emitted: &Path,
    captured_sha256: &str,
) -> bool {
    Path::new(&invocation.program) == emitted
        && emitted.is_absolute()
        && invocation.program_sha256 == captured_sha256
}

fn validate_scanned_simulator_schedule(
    profile: &str,
    source_ref: &str,
    configuration: &crate::contract::profile::SimulatorRunnerConfiguration,
    logs: &[ScannedSimulatorLog<'_>],
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
    if !logs[0]
        .source
        .lines()
        .any(|line| line == "exit_code: Some(0)")
        || !logs[0]
            .source
            .lines()
            .any(|line| line.starts_with(&expected_profile))
        || !logs[0]
            .source
            .lines()
            .any(|line| line == expected_seed_line)
        || !profile_total_is_rederived(&model_profile, &configuration.state_floors, &logs[0].events)
        || !soak_seeds_are_rederived(
            &model_profile,
            &expected_seeds,
            configuration.soak_steps,
            logs[0].events.iter(),
        )
    {
        return Err(AggregateError::new(format!(
            "scheduled simulator log does not prove the source-derived {profile} execution plan"
        )));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn validate_simulator_schedule(
    profile: &str,
    source_ref: &str,
    configuration: &crate::contract::profile::SimulatorRunnerConfiguration,
    logs: &[String],
) -> Result<(), AggregateError> {
    let scanned = logs
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let (events, diagnostics) =
                scan_machine_events(source, &format!("simulator test log {index}"));
            if let Some(diagnostic) = diagnostics.into_iter().next() {
                return Err(AggregateError::new(diagnostic));
            }
            Ok(ScannedSimulatorLog { source, events })
        })
        .collect::<Result<Vec<_>, _>>()?;
    validate_scanned_simulator_schedule(profile, source_ref, configuration, &scanned)
}

fn validate_pr_soak_schedule(
    configuration: &crate::contract::profile::SimulatorRunnerConfiguration,
    logs: &[ScannedSimulatorLog<'_>],
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
        .flat_map(|log| log.source.lines())
        .filter(|line| *line == expected_seed_line)
        .count();
    if seed_line_count != 1
        || !soak_seeds_are_rederived(
            "raft",
            EXPECTED_SEEDS,
            configuration.soak_steps,
            logs.iter().flat_map(|log| log.events.iter()),
        )
    {
        return Err(AggregateError::new(
            "PR simulator log does not prove the exact reviewed soak seed inventory".to_owned(),
        ));
    }
    Ok(())
}

fn profile_total_is_rederived(
    model_profile: &str,
    state_floors: &crate::contract::profile::SimulatorStateFloors,
    events: &[Value],
) -> bool {
    let (protocol_floor, verifier_floor) = match state_floors {
        crate::contract::profile::SimulatorStateFloors::Aggregate { protocol, verifier } => {
            (*protocol, *verifier)
        }
        crate::contract::profile::SimulatorStateFloors::PerEvidence => return false,
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

fn soak_seeds_are_rederived<'a>(
    model_profile: &str,
    expected_seeds: &str,
    expected_steps: u64,
    events: impl IntoIterator<Item = &'a Value>,
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
    for event in events
        .into_iter()
        .filter(|event| event["event"] == "soak-check")
    {
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

#[cfg(test)]
#[path = "simulator_schedule_provenance_tests.rs"]
mod provenance_tests;
