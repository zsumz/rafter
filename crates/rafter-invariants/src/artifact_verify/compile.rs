//! Source-bound Cargo compilation and executable evidence verification.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
};

use serde::Deserialize;

use crate::{
    verification::{AggregateError, AuthenticatedArtifacts},
    EvidenceStatus, FailureClassification, ResultBundle,
};

use super::test_logs::test_execution_profile;
use crate::verification::RecordedWorkspace;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct CargoTargetKey {
    pub(super) package: String,
    pub(super) kind: String,
    pub(super) target: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreservedTestBinary {
    sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct EmittedTestExecutable {
    pub(super) package_id: String,
    pub(super) target: CargoTargetKey,
    pub(super) executable: PathBuf,
    pub(super) sha256: String,
}

#[derive(Debug, Deserialize)]
struct CargoCompilerMessage {
    reason: String,
    package_id: Option<String>,
    target: Option<CargoMessageTarget>,
    fresh: Option<bool>,
    executable: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct CargoMessageTarget {
    kind: Vec<String>,
    name: String,
    src_path: PathBuf,
}

pub(super) fn verify_compile_invocations(
    bundle: &ResultBundle,
    root: &Path,
    catalog: &crate::Catalog,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
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
    let workspace = RecordedWorkspace::new(bundle, root)?;
    let current_dir = workspace.producer().to_string_lossy().into_owned();
    let preserved_test_binaries = preserved_test_binaries(bundle, catalog)?;
    let mut emitted_test_executables = BTreeMap::new();
    for log in logs {
        let source = authenticated.text(log)?;
        let invocations = crate::evidence::format::process::parse_combined_v4(source)
            .map_err(|error| AggregateError::new(format!("parse compile invocation: {error}")))?;
        let [observed] = invocations.as_slice() else {
            return Err(AggregateError::new(
                "compile log must contain exactly one invocation".to_owned(),
            ));
        };
        if observed.invocation.program != "cargo"
            || observed.invocation.program_sha256 != bundle.execution.source.cargo_sha256
            || observed.invocation.current_dir != current_dir
            || !crate::verification::process_invocation_matches_source(
                &observed.invocation,
                &bundle.execution.source,
            )
        {
            return Err(AggregateError::new(
                "compile executable or working directory does not match source provenance"
                    .to_owned(),
            ));
        }
        if bundle.runner == "tests" || observed.label != "simulator compile" {
            if let Some(executable) =
                verify_test_compile(bundle, observed, &workspace, &preserved_test_binaries)?
            {
                let target = executable.target.clone();
                if emitted_test_executables
                    .insert(target.clone(), executable)
                    .is_some()
                {
                    return Err(AggregateError::new(format!(
                        "Cargo target {target:?} has multiple successful compile receipts"
                    )));
                }
            }
        } else {
            verify_simulator_compile(bundle, observed, &workspace)?;
        }
        verify_compile_process_outcome(bundle, observed)?;
    }
    if emitted_test_executables.len() != preserved_test_binaries.len()
        || emitted_test_executables
            .keys()
            .ne(preserved_test_binaries.keys())
    {
        return Err(AggregateError::new(
            "successful test compiler targets do not exactly match preserved test binaries"
                .to_owned(),
        ));
    }
    verify_test_programs_were_emitted(bundle, catalog, &emitted_test_executables, authenticated)?;
    Ok(())
}

fn verify_test_compile(
    bundle: &ResultBundle,
    observed: &crate::evidence::format::process::LabeledProcess,
    workspace: &RecordedWorkspace,
    preserved: &BTreeMap<CargoTargetKey, PreservedTestBinary>,
) -> Result<Option<EmittedTestExecutable>, AggregateError> {
    let parts = observed.label.split('/').collect::<Vec<_>>();
    let [package, kind, target] = parts.as_slice() else {
        return Err(AggregateError::new(
            "test compile label does not name one Cargo target".to_owned(),
        ));
    };
    let target_key = CargoTargetKey {
        package: (*package).to_owned(),
        kind: (*kind).to_owned(),
        target: (*target).to_owned(),
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
    let expected_target_dir = workspace.producer_path(Path::new(&expected_target))?;
    let mut base_environment = observed.invocation.environment.clone();
    let target_dir = base_environment.remove("CARGO_TARGET_DIR");
    if observed.invocation.arguments != expected
        || !target_directory_matches(target_dir.as_deref(), &expected_target_dir)
        || !crate::provenance::invocation::environment_matches_digest(
            &observed.invocation.environment,
            &observed.invocation.environment_sha256,
        )
        || base_environment != bundle.execution.invocation.environment
    {
        return Err(AggregateError::new(
            "test compile log does not match the exact Cargo invocation plan".to_owned(),
        ));
    }
    if observed.exit_code != Some(0) || observed.timed_out {
        return Ok(None);
    }
    let binary = preserved.get(&target_key).ok_or_else(|| {
        AggregateError::new(format!(
            "successful Cargo target {} has no uniquely preserved test binary",
            observed.label
        ))
    })?;
    let artifact = compiler_artifact_for_test(
        observed.stdout.as_bytes(),
        &target_key,
        workspace,
        &expected_target_dir,
        &observed.label,
    )?;
    Ok(Some(EmittedTestExecutable {
        package_id: artifact.package_id,
        target: target_key,
        executable: artifact.executable,
        sha256: binary.sha256.clone(),
    }))
}

struct ParsedCompilerArtifact {
    package_id: String,
    executable: PathBuf,
}

fn compiler_artifact_for_test(
    bytes: &[u8],
    expected: &CargoTargetKey,
    workspace: &RecordedWorkspace,
    expected_target_dir: &Path,
    target_label: &str,
) -> Result<ParsedCompilerArtifact, AggregateError> {
    crate::verification::target::verify_protected_compiler_artifacts(
        bytes,
        workspace.producer(),
        workspace.active(),
    )
    .map_err(AggregateError::new)?;
    let mut artifacts = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<CargoCompilerMessage>(line) else {
            continue;
        };
        if message.reason != "compiler-artifact" {
            continue;
        }
        let Some(target) = message.target else {
            continue;
        };
        if target.name != expected.target || target.kind != [expected.kind.as_str()] {
            continue;
        }
        if message.fresh == Some(true) {
            return Err(AggregateError::new(format!(
                "fresh cached executable is forbidden for {target_label}"
            )));
        }
        let package_id = message.package_id.ok_or_else(|| {
            AggregateError::new(format!(
                "compiler-artifact for {target_label} omitted Cargo package_id"
            ))
        })?;
        verify_cargo_package_identity(&package_id, &target.src_path, &expected.package, workspace)?;
        let executable = message.executable.ok_or_else(|| {
            AggregateError::new(format!(
                "compiler-artifact for {target_label} omitted its executable"
            ))
        })?;
        verify_emitted_test_path(
            &executable,
            expected_target_dir,
            &expected.target,
            target_label,
        )?;
        artifacts.push(ParsedCompilerArtifact {
            package_id,
            executable,
        });
    }
    let [artifact] = artifacts.as_slice() else {
        return Err(AggregateError::new(format!(
            "compile log does not preserve exactly one package-bound executable for {target_label}; found {}",
            artifacts.len()
        )));
    };
    Ok(ParsedCompilerArtifact {
        package_id: artifact.package_id.clone(),
        executable: artifact.executable.clone(),
    })
}

fn verify_cargo_package_identity(
    package_id: &str,
    src_path: &Path,
    expected_package: &str,
    workspace: &RecordedWorkspace,
) -> Result<(), AggregateError> {
    let expected_package_dir =
        workspace.producer_path(Path::new("crates").join(expected_package).as_path())?;
    let encoded = package_id.strip_prefix("path+file://").ok_or_else(|| {
        AggregateError::new(format!(
            "compiler-artifact package_id for {expected_package} is not a workspace path package"
        ))
    })?;
    let (package_path, version) = encoded.rsplit_once('#').ok_or_else(|| {
        AggregateError::new(format!(
            "compiler-artifact package_id for {expected_package} has no version"
        ))
    })?;
    if version.is_empty() {
        return Err(AggregateError::new(format!(
            "compiler-artifact package_id for {expected_package} has an empty version"
        )));
    }
    let observed_package_dir = Path::new(package_path);
    let observed_source = src_path;
    if observed_package_dir != expected_package_dir
        || !observed_source.starts_with(&expected_package_dir)
    {
        return Err(AggregateError::new(format!(
            "compiler-artifact package_id or source path does not match workspace package {expected_package}"
        )));
    }
    workspace.verify_active_directory(
        observed_package_dir,
        &format!("compiler-artifact package {expected_package}"),
    )?;
    workspace.verify_active_file(
        observed_source,
        &format!("compiler-artifact source for {expected_package}"),
    )?;
    Ok(())
}

fn verify_emitted_test_path(
    executable: &Path,
    expected_target_dir: &Path,
    expected_target: &str,
    target_label: &str,
) -> Result<(), AggregateError> {
    if !executable.is_absolute()
        || executable
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        || !executable.starts_with(expected_target_dir)
    {
        return Err(AggregateError::new(format!(
            "Cargo emitted a non-canonical or cross-build executable for {target_label}"
        )));
    }
    let expected_prefix = expected_target.replace('-', "_");
    let file_name = executable
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            AggregateError::new(format!(
                "Cargo emitted a non-UTF-8 executable name for {target_label}"
            ))
        })?;
    if file_name != expected_prefix
        && !file_name
            .strip_prefix(&expected_prefix)
            .is_some_and(|suffix| suffix.starts_with('-'))
    {
        return Err(AggregateError::new(format!(
            "Cargo executable name {file_name} does not match target {expected_target}"
        )));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn compiler_artifact_executable(
    bytes: &[u8],
    target_name: &str,
    target_kind: &str,
    target_label: &str,
) -> Result<PathBuf, AggregateError> {
    let mut executables = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if message["reason"] == "compiler-artifact"
            && message["target"]["name"] == target_name
            && message["target"]["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind.as_str() == Some(target_kind)))
        {
            if message["fresh"] == true {
                return Err(AggregateError::new(format!(
                    "fresh cached executable is forbidden for {target_label}"
                )));
            }
            if let Some(executable) = message["executable"].as_str() {
                executables.push(PathBuf::from(executable));
            }
        }
    }
    let [executable] = executables.as_slice() else {
        return Err(AggregateError::new(format!(
            "compile log does not preserve exactly one emitted executable for {target_label}; found {}",
            executables.len()
        )));
    };
    if !executable.is_absolute() {
        return Err(AggregateError::new(format!(
            "Cargo emitted a non-absolute executable for {target_label}"
        )));
    }
    Ok(executable.clone())
}

fn preserved_test_binaries(
    bundle: &ResultBundle,
    catalog: &crate::Catalog,
) -> Result<BTreeMap<CargoTargetKey, PreservedTestBinary>, AggregateError> {
    let descriptors = catalog
        .evidence
        .iter()
        .map(|descriptor| (descriptor.evidence_id(), descriptor))
        .collect::<BTreeMap<_, _>>();
    let mut binaries = BTreeMap::new();
    for check in &bundle.execution.checks {
        let artifacts = check
            .artifacts
            .iter()
            .filter(|artifact| artifact.kind == "test-binary")
            .collect::<BTreeSet<_>>();
        if artifacts.is_empty() {
            continue;
        }
        let artifacts = artifacts.into_iter().collect::<Vec<_>>();
        let [binary] = artifacts.as_slice() else {
            return Err(AggregateError::new(format!(
                "check {} does not preserve exactly one test binary",
                check.check_id
            )));
        };
        let target = registered_test_target(&descriptors, check)?;
        let preserved = PreservedTestBinary {
            sha256: binary.sha256.clone(),
        };
        if let Some(previous) = binaries.insert(target.clone(), preserved.clone()) {
            if previous != preserved {
                return Err(AggregateError::new(format!(
                    "Cargo target {target:?} is bound to conflicting preserved binaries"
                )));
            }
        }
    }
    Ok(binaries)
}

fn registered_test_target(
    descriptors: &BTreeMap<String, &crate::EvidenceDescriptor>,
    check: &crate::CheckReceipt,
) -> Result<CargoTargetKey, AggregateError> {
    let mut targets = BTreeSet::new();
    for evidence_id in &check.evidence_ids {
        let descriptor = descriptors.get(evidence_id).ok_or_else(|| {
            AggregateError::new(format!(
                "check {} references unknown registry evidence {evidence_id}",
                check.check_id
            ))
        })?;
        let identity = descriptor.test.as_ref().or_else(|| {
            descriptor
                .simulator
                .as_ref()
                .and_then(|identity| identity.negative_test.as_ref())
        });
        if let Some(identity) = identity {
            targets.insert(CargoTargetKey {
                package: identity.package.clone(),
                kind: identity.target_kind.clone(),
                target: identity.target.clone(),
            });
        }
    }
    let targets = targets.into_iter().collect::<Vec<_>>();
    let [target] = targets.as_slice() else {
        return Err(AggregateError::new(format!(
            "check {} does not bind exactly one registered Cargo test target",
            check.check_id
        )));
    };
    Ok(target.clone())
}

fn verify_test_programs_were_emitted(
    bundle: &ResultBundle,
    catalog: &crate::Catalog,
    emitted: &BTreeMap<CargoTargetKey, EmittedTestExecutable>,
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    let descriptors = catalog
        .evidence
        .iter()
        .map(|descriptor| (descriptor.evidence_id(), descriptor))
        .collect::<BTreeMap<_, _>>();
    for check in &bundle.execution.checks {
        let logs = check
            .artifacts
            .iter()
            .filter(|artifact| artifact.kind == "test-log")
            .collect::<BTreeSet<_>>();
        if logs.is_empty() {
            continue;
        }
        let target = registered_test_target(&descriptors, check)?;
        let executable = emitted.get(&target).ok_or_else(|| {
            AggregateError::new(format!(
                "check {} executed target {target:?} without its source-bound compiler artifact",
                check.check_id
            ))
        })?;
        for log in logs {
            let source = authenticated.text(log)?;
            let processes = crate::evidence::format::process::parse_combined_processes(source)
                .map_err(|error| {
                    AggregateError::new(format!("parse test log {}: {error}", log.path))
                })?;
            verify_target_process_binding(&processes, executable, &log.path)?;
        }
    }
    Ok(())
}

pub(super) fn verify_target_process_binding(
    processes: &[crate::evidence::format::process::LabeledProcess],
    emitted: &EmittedTestExecutable,
    log_path: &str,
) -> Result<(), AggregateError> {
    if processes.is_empty()
        || processes.iter().any(|process| {
            Path::new(&process.invocation.program) != emitted.executable
                || process.invocation.program_sha256 != emitted.sha256
        })
    {
        return Err(AggregateError::new(format!(
            "test log {log_path} does not invoke the exact package-bound executable for {:?} ({})",
            emitted.target, emitted.package_id
        )));
    }
    Ok(())
}

fn verify_simulator_compile(
    bundle: &ResultBundle,
    observed: &crate::evidence::format::process::LabeledProcess,
    workspace: &RecordedWorkspace,
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
    let expected_target_dir = workspace.producer_path(Path::new(&expected_target))?;
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
    if !target_directory_matches(target.as_deref(), &expected_target_dir) {
        return Err(AggregateError::new(
            "simulator compile log has the wrong Cargo target directory".to_owned(),
        ));
    }
    if base_environment != bundle.execution.invocation.environment {
        return Err(AggregateError::new(
            "simulator compile log has the wrong base environment".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn target_directory_matches(recorded: Option<&str>, expected: &Path) -> bool {
    recorded.is_some_and(|recorded| Path::new(recorded) == expected && expected.is_absolute())
}

fn verify_compile_process_outcome(
    bundle: &ResultBundle,
    observed: &crate::evidence::format::process::LabeledProcess,
) -> Result<(), AggregateError> {
    if observed.exit_code == Some(0) && !observed.timed_out {
        return Ok(());
    }
    let check_prefix = format!("tests/{}#", observed.label);
    let execution_ids = bundle
        .execution
        .checks
        .iter()
        .filter(|check| bundle.runner != "tests" || check.check_id.starts_with(&check_prefix))
        .map(|check| check.execution_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if execution_ids.is_empty()
        || bundle.results.iter().any(|result| {
            execution_ids.contains(result.execution_id.as_str())
                && (result.status != EvidenceStatus::Error
                    || result.classification != Some(FailureClassification::HarnessError))
        })
    {
        return Err(AggregateError::new(
            "failed compile process is not reflected as a harness error for every affected check"
                .to_owned(),
        ));
    }
    Ok(())
}
