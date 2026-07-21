//! Cargo invocation and compiler-artifact provenance for the simulator binary.

use std::path::{Path, PathBuf};

use serde_json::Value;

use super::paths::{
    clean_absolute_path, map_producer_path, resolve_producer_path, verify_active_path,
    SimulatorRoots,
};
use crate::{
    evidence::ResultBundle,
    verification::{process_invocation_matches_source, AggregateError, AuthenticatedArtifacts},
};

mod selector;

use selector::compiler_artifact_executable;

const SIMULATOR_PACKAGE: &str = "rafter-sim";
const SIMULATOR_PACKAGE_VERSION: &str = "0.0.1";
const SIMULATOR_TARGET: &str = "rafter-model-check-fast";

struct CompilerIdentityPaths<'a> {
    producer_root: &'a Path,
    active_root: &'a Path,
    producer_package: PathBuf,
    active_package: PathBuf,
    producer_source: PathBuf,
    active_source: PathBuf,
}

pub(super) fn emitted_simulator_executable(
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
        let processes = authenticated.combined_v4(artifact)?;
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
            let named_executable = compiler_artifact_executable(
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
        || !process_invocation_matches_source(&process.invocation, &bundle.execution.source)
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

pub(crate) fn simulator_compiler_artifact_executable(
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
    let identity = CompilerIdentityPaths {
        producer_root,
        active_root,
        producer_package: producer_root.join("crates").join(SIMULATOR_PACKAGE),
        active_package: active_root.join("crates").join(SIMULATOR_PACKAGE),
        producer_source: producer_root
            .join("crates")
            .join(SIMULATOR_PACKAGE)
            .join("src/bin")
            .join(format!("{SIMULATOR_TARGET}.rs")),
        active_source: active_root
            .join("crates")
            .join(SIMULATOR_PACKAGE)
            .join("src/bin")
            .join(format!("{SIMULATOR_TARGET}.rs")),
    };
    verify_active_path(&identity.active_package, true, "simulator package")?;
    verify_active_path(&identity.active_source, false, "simulator target source")?;
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
        verify_compiler_identity(&message, &identity)?;
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

fn verify_compiler_identity(
    message: &Value,
    paths: &CompilerIdentityPaths<'_>,
) -> Result<(), AggregateError> {
    let package_id = message["package_id"].as_str().ok_or_else(|| {
        AggregateError::new("simulator compiler-artifact omitted Cargo package_id".to_owned())
    })?;
    let package_path = simulator_package_path(package_id)?;
    let mapped_package = map_producer_path(
        &package_path,
        paths.producer_root,
        paths.active_root,
        "simulator package_id",
    )?;
    if package_path != paths.producer_package || mapped_package != paths.active_package {
        return Err(AggregateError::new(
            "simulator compiler-artifact package_id does not match rafter-sim".to_owned(),
        ));
    }
    let src_path = message["target"]["src_path"].as_str().ok_or_else(|| {
        AggregateError::new("simulator compiler-artifact omitted its target source path".to_owned())
    })?;
    let producer_src_path = Path::new(src_path);
    let mapped_source = map_producer_path(
        producer_src_path,
        paths.producer_root,
        paths.active_root,
        "simulator target source",
    )?;
    if producer_src_path != paths.producer_source || mapped_source != paths.active_source {
        return Err(AggregateError::new(
            "simulator compiler-artifact source path does not match the exact workspace bin target"
                .to_owned(),
        ));
    }
    Ok(())
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

fn exact_string_array(value: &Value, expected: &str) -> bool {
    value
        .as_array()
        .is_some_and(|values| values.len() == 1 && values[0].as_str() == Some(expected))
}
