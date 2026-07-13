use std::{fs, path::Path};

use crate::{aggregate::AggregateError, EvidenceStatus, ResultBundle};

pub(super) fn verify_compile_invocations(
    bundle: &ResultBundle,
    root: &Path,
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

pub(super) fn verify_test_logs(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
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

pub(super) fn verify_test_invocations(
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

pub(super) fn require_exact_test_pass(
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

pub(super) fn is_passing(bundle: &ResultBundle, execution_id: &str) -> bool {
    bundle
        .results
        .iter()
        .any(|result| result.execution_id == execution_id && result.status == EvidenceStatus::Pass)
}
