//! Exact test-target planning, preservation, and execution binding.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use crate::{
    contract::catalog::{Catalog, EvidenceDescriptor},
    evidence::{CheckReceipt, ResultBundle},
    verification::{AggregateError, AuthenticatedArtifacts, RecordedWorkspace},
};

use super::{
    cargo_output::compiler_artifact_for_test,
    invocation::target_directory_matches,
    model::{CargoTargetKey, EmittedTestExecutable, PreservedTestBinary},
};

pub(super) fn verify_test_compile(
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
            ));
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
    let execution_profile = super::super::test_execution::profile(bundle);
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

pub(super) fn preserved_test_binaries(
    bundle: &ResultBundle,
    catalog: &Catalog,
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
    descriptors: &BTreeMap<String, &EvidenceDescriptor>,
    check: &CheckReceipt,
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

pub(super) fn verify_test_programs_were_emitted(
    bundle: &ResultBundle,
    catalog: &Catalog,
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
            let processes = authenticated.combined_processes(log)?;
            verify_target_process_binding(&processes, executable, &log.path)?;
        }
    }
    Ok(())
}

pub(crate) fn verify_target_process_binding(
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
