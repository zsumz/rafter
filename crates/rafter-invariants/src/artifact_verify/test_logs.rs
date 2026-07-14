use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use crate::{aggregate::AggregateError, EvidenceStatus, FailureClassification, ResultBundle};

pub(super) fn verify_test_logs(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    let catalog =
        crate::Catalog::load(root.join(&bundle.execution.plan.registry.path).as_path())
            .map_err(|error| AggregateError::new(format!("reload tests registry: {error}")))?;
    for check in &bundle.execution.checks {
        let outcomes = bundle
            .results
            .iter()
            .filter(|result| result.execution_id == check.execution_id)
            .map(|result| (result.status, result.classification))
            .collect::<Vec<_>>();
        let Some(outcome) = outcomes.first().copied() else {
            return Err(AggregateError::new(format!(
                "tests check {} has no evidence result",
                check.check_id
            )));
        };
        if outcomes.iter().any(|candidate| *candidate != outcome) {
            return Err(AggregateError::new(format!(
                "tests check {} has conflicting evidence outcomes",
                check.check_id
            )));
        }
        let test_name = registered_test_name(&catalog, check)?;
        let Some(test_log) = check
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "test-log")
        else {
            if outcome
                == (
                    EvidenceStatus::Error,
                    Some(FailureClassification::HarnessError),
                )
                && check
                    .artifacts
                    .iter()
                    .any(|artifact| artifact.kind == "compile-log")
            {
                continue;
            }
            return Err(AggregateError::new(format!(
                "tests check {} is missing its runtime transcript",
                check.check_id
            )));
        };
        let source = fs::read_to_string(root.join(&test_log.path)).map_err(|error| {
            AggregateError::new(format!("read test-log {}: {error}", test_log.path))
        })?;
        match outcome {
            (EvidenceStatus::Pass, None) => {
                verify_test_invocations(bundle, check, &source, &test_name, &check.check_id, root)?;
                require_exact_test_pass(&source, &test_name, &check.check_id)?;
            }
            (EvidenceStatus::Fail, Some(FailureClassification::InvariantViolation)) => {
                verify_oracle_failure_invocations(bundle, check, &source, &test_name, root)?;
                require_exact_test_failure(&source, &test_name, &check.check_id)?;
            }
            (EvidenceStatus::Incomplete, Some(FailureClassification::CoverageNotReached)) => {
                verify_incomplete_test_invocations(bundle, check, &source, &test_name, root)?;
            }
            (EvidenceStatus::Error, Some(FailureClassification::HarnessError)) => {
                verify_harness_error_test_invocations(bundle, check, &source, &test_name, root)?;
            }
            _ => {
                return Err(AggregateError::new(format!(
                    "tests check {} has an invalid status/classification pair",
                    check.check_id
                )))
            }
        }
    }
    Ok(())
}

fn registered_test_name(
    catalog: &crate::Catalog,
    check: &crate::CheckReceipt,
) -> Result<String, AggregateError> {
    let descriptors = catalog
        .evidence
        .iter()
        .map(|descriptor| (descriptor.evidence_id(), descriptor))
        .collect::<BTreeMap<_, _>>();
    let mut identities = BTreeSet::new();
    for evidence_id in &check.evidence_ids {
        let descriptor = descriptors.get(evidence_id).ok_or_else(|| {
            AggregateError::new(format!(
                "tests check {} references unknown registry evidence {evidence_id}",
                check.check_id
            ))
        })?;
        let identity = descriptor.test.as_ref().ok_or_else(|| {
            AggregateError::new(format!(
                "tests check {} references non-tests evidence {evidence_id}",
                check.check_id
            ))
        })?;
        if identity.check_id() != check.check_id {
            return Err(AggregateError::new(format!(
                "tests check {} does not match registered identity {}",
                check.check_id,
                identity.check_id()
            )));
        }
        identities.insert(identity.test_name.clone());
    }
    let identities = identities.into_iter().collect::<Vec<_>>();
    let [test_name] = identities.as_slice() else {
        return Err(AggregateError::new(format!(
            "tests check {} does not bind exactly one registered test identity",
            check.check_id
        )));
    };
    Ok(test_name.clone())
}

pub(super) fn verify_test_invocations(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    source: &str,
    test_name: &str,
    oracle_check_id: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    let processes =
        verify_test_process_plan(bundle, check, source, test_name, oracle_check_id, root)?;
    if processes.len() != 3
        || processes
            .iter()
            .any(|process| process.exit_code != Some(0) || process.timed_out)
    {
        return Err(AggregateError::new(
            "test log contains an unsuccessful process invocation".to_owned(),
        ));
    }
    require_unique_discovery(&processes, test_name)?;
    let Some(exact) = processes.last() else {
        return Err(AggregateError::new(
            "test process plan omitted the exact invocation".to_owned(),
        ));
    };
    let executed_test_name = &exact.invocation.arguments[0];
    if crate::producer::test_exec::classify_exact_execution(
        exact.stdout.as_bytes(),
        exact.stderr.as_bytes(),
        executed_test_name,
        &crate::producer::test_exec::oracle_token(&bundle.source_ref, oracle_check_id),
        exact.exit_code,
        exact.timed_out,
    ) != crate::producer::test_exec::ExactTestExecution::Pass
    {
        return Err(AggregateError::new(
            "test log does not prove one strict exact pass".to_owned(),
        ));
    }
    Ok(())
}

fn verify_oracle_failure_invocations(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    source: &str,
    test_name: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    let processes =
        verify_test_process_plan(bundle, check, source, test_name, &check.check_id, root)?;
    let [listed, ignored, exact] = processes.as_slice() else {
        unreachable!("test process plan verifies exact cardinality");
    };
    if listed.exit_code != Some(0)
        || listed.timed_out
        || ignored.exit_code != Some(0)
        || ignored.timed_out
        || exact.exit_code != Some(101)
        || exact.timed_out
    {
        return Err(AggregateError::new(
            "oracle failure receipt must contain two successful discoveries and one clean libtest failure"
                .to_owned(),
        ));
    }
    require_unique_discovery(&processes, test_name)?;
    let executed_test_name = &exact.invocation.arguments[0];
    if crate::producer::test_exec::classify_exact_execution(
        exact.stdout.as_bytes(),
        exact.stderr.as_bytes(),
        executed_test_name,
        &crate::producer::test_exec::oracle_token(&bundle.source_ref, &check.check_id),
        exact.exit_code,
        exact.timed_out,
    ) != crate::producer::test_exec::ExactTestExecution::InvariantViolation
    {
        return Err(AggregateError::new(
            "oracle failure transcript does not prove one strict exact rejection".to_owned(),
        ));
    }
    Ok(())
}

fn verify_incomplete_test_invocations(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    source: &str,
    test_name: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    let processes =
        verify_test_process_plan(bundle, check, source, test_name, &check.check_id, root)?;
    let discovery_success = processes
        .iter()
        .take(2)
        .all(|process| process.exit_code == Some(0) && !process.timed_out);
    if !discovery_success {
        return Err(AggregateError::new(
            "coverage-not-reached transcript contains a failed discovery".to_owned(),
        ));
    }
    match processes.as_slice() {
        [listed, _ignored] => {
            let matches = crate::producer::test_exec::listed_tests(listed.stdout.as_bytes())
                .into_iter()
                .filter(|test| test.as_str() == test_name)
                .count();
            if matches != 0 {
                return Err(AggregateError::new(
                    "coverage-not-reached discovery transcript contains the exact test".to_owned(),
                ));
            }
        }
        [_, _, exact] => {
            let executed_test_name = &exact.invocation.arguments[0];
            if crate::producer::test_exec::classify_exact_execution(
                exact.stdout.as_bytes(),
                exact.stderr.as_bytes(),
                executed_test_name,
                &crate::producer::test_exec::oracle_token(&bundle.source_ref, &check.check_id),
                exact.exit_code,
                exact.timed_out,
            ) != crate::producer::test_exec::ExactTestExecution::CoverageNotReached
            {
                return Err(AggregateError::new(
                    "coverage-not-reached transcript does not prove zero exact executions"
                        .to_owned(),
                ));
            }
            require_unique_discovery(&processes, test_name)?;
        }
        _ => {
            return Err(AggregateError::new(
                "coverage-not-reached transcript does not prove zero exact executions".to_owned(),
            ))
        }
    }
    Ok(())
}

fn verify_harness_error_test_invocations(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    source: &str,
    test_name: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    let processes =
        verify_test_process_plan(bundle, check, source, test_name, &check.check_id, root)?;
    match processes.as_slice() {
        [listed, ignored] => {
            let discovery_failed = [listed, ignored]
                .into_iter()
                .any(|process| process.exit_code != Some(0) || process.timed_out);
            let listed_matches = crate::producer::test_exec::listed_tests(listed.stdout.as_bytes())
                .into_iter()
                .filter(|test| test.as_str() == test_name)
                .count();
            let ignored_matches =
                crate::producer::test_exec::listed_tests(ignored.stdout.as_bytes())
                    .into_iter()
                    .filter(|test| test.as_str() == test_name)
                    .count();
            if !discovery_failed && listed_matches <= 1 && ignored_matches <= 1 {
                return Err(AggregateError::new(
                    "harness-error discovery transcript contains no discovery failure or duplicate identity"
                        .to_owned(),
                ));
            }
        }
        [listed, ignored, exact] => {
            require_unique_discovery(&processes, test_name)?;
            let executed_test_name = &exact.invocation.arguments[0];
            if [listed, ignored]
                .into_iter()
                .any(|process| process.exit_code != Some(0) || process.timed_out)
                || crate::producer::test_exec::classify_exact_execution(
                    exact.stdout.as_bytes(),
                    exact.stderr.as_bytes(),
                    executed_test_name,
                    &crate::producer::test_exec::oracle_token(&bundle.source_ref, &check.check_id),
                    exact.exit_code,
                    exact.timed_out,
                ) != crate::producer::test_exec::ExactTestExecution::HarnessError
            {
                return Err(AggregateError::new(
                    "harness-error execution transcript does not prove an abnormal exact run"
                        .to_owned(),
                ));
            }
        }
        _ => unreachable!("test process plan has two or three invocations"),
    }
    Ok(())
}

pub(super) fn require_unique_discovery(
    processes: &[crate::producer::process::LabeledProcess],
    test_name: &str,
) -> Result<(), AggregateError> {
    let listed_matches = crate::producer::test_exec::listed_tests(processes[0].stdout.as_bytes())
        .into_iter()
        .filter(|test| test.as_str() == test_name)
        .count();
    let ignored_matches = crate::producer::test_exec::listed_tests(processes[1].stdout.as_bytes())
        .into_iter()
        .filter(|test| test.as_str() == test_name)
        .count();
    if listed_matches != 1 || ignored_matches > 1 {
        return Err(AggregateError::new(format!(
            "exact test discovery is not unique: normal={listed_matches}, ignored={ignored_matches}"
        )));
    }
    Ok(())
}

fn verify_test_process_plan(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    source: &str,
    test_name: &str,
    oracle_check_id: &str,
    root: &Path,
) -> Result<Vec<crate::producer::process::LabeledProcess>, AggregateError> {
    let invocations = crate::producer::process::parse_combined_processes(source)
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
    let exact_environment =
        exact_test_environment(bundle, check, &invocations, test_name, oracle_check_id)?;
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
    if !(2..=3).contains(&invocations.len())
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
    verify_reconstructed_test_observations(check, &invocations, test_name)?;
    if let Some(exact) = invocations.get(2) {
        let ignored_matches =
            crate::producer::test_exec::listed_tests(invocations[1].stdout.as_bytes())
                .into_iter()
                .filter(|test| test.as_str() == test_name)
                .count();
        verify_exact_test_arguments(exact, test_name, ignored_matches)?;
        verify_exact_environment(exact, &exact_environment, &exact_digest)?;
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
    Ok(invocations)
}

fn verify_exact_test_arguments(
    exact: &crate::producer::process::LabeledProcess,
    test_name: &str,
    ignored_matches: usize,
) -> Result<(), AggregateError> {
    let arguments = &exact.invocation.arguments;
    if exact.label != "exact libtest execution"
        || arguments.len() < 6
        || arguments[0] != test_name
        || arguments[1..6]
            != [
                "--exact",
                "--test-threads=1",
                "--show-output",
                "--color",
                "never",
            ]
        || (arguments.len() == 7 && arguments[6] != "--ignored")
        || arguments.len() > 7
    {
        return Err(AggregateError::new(format!(
            "test log does not contain the exact libtest argument plan for {test_name}: {arguments:?}"
        )));
    }
    if (arguments.len() == 7) != (ignored_matches == 1) {
        return Err(AggregateError::new(
            "test log ignored execution mode disagrees with ignored discovery".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn verify_exact_environment(
    exact: &crate::producer::process::LabeledProcess,
    expected: &BTreeMap<String, String>,
    expected_digest: &str,
) -> Result<(), AggregateError> {
    if exact.invocation.environment != *expected
        || exact.invocation.environment_sha256 != expected_digest
        || crate::producer::process::digest_environment(&exact.invocation.environment)
            != exact.invocation.environment_sha256
    {
        return Err(AggregateError::new(
            "test log does not contain the exact execution environment".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn verify_reconstructed_test_observations(
    check: &crate::CheckReceipt,
    invocations: &[crate::producer::process::LabeledProcess],
    test_name: &str,
) -> Result<(), AggregateError> {
    let listed_matches = crate::producer::test_exec::listed_tests(invocations[0].stdout.as_bytes())
        .into_iter()
        .filter(|test| test.as_str() == test_name)
        .count();
    let exact = invocations.get(2);
    let executed_test_name = exact
        .and_then(|process| process.invocation.arguments.first())
        .map_or(test_name, String::as_str);
    let executed = usize::from(exact.is_some_and(|process| {
        crate::producer::test_exec::exact_pass(process.stdout.as_bytes(), executed_test_name)
            || crate::producer::test_exec::exact_failure(
                process.stdout.as_bytes(),
                executed_test_name,
            )
    }));
    let passed = usize::from(exact.is_some_and(|process| {
        crate::producer::test_exec::exact_pass(process.stdout.as_bytes(), executed_test_name)
    }));
    let reconstructed = BTreeMap::from([
        ("discovered".to_owned(), listed_matches as u64),
        ("executed".to_owned(), executed as u64),
        ("passed".to_owned(), passed as u64),
    ]);
    if check.observations != reconstructed {
        return Err(AggregateError::new(format!(
            "test receipt observations do not match discovery and execution transcripts: claimed {:?}, reconstructed {reconstructed:?}",
            check.observations
        )));
    }
    Ok(())
}

fn exact_test_environment(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    invocations: &[crate::producer::process::LabeledProcess],
    test_name: &str,
    oracle_check_id: &str,
) -> Result<BTreeMap<String, String>, AggregateError> {
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
        (
            crate::producer::test_exec::ORACLE_TOKEN_ENV.to_owned(),
            crate::producer::test_exec::oracle_token(&bundle.source_ref, oracle_check_id),
        ),
    ]);
    Ok(environment)
}

pub(super) fn test_execution_profile(bundle: &ResultBundle) -> String {
    if bundle.runner == "simulator" {
        format!("{}-simulator-detectors", bundle.profile)
    } else {
        bundle.profile.clone()
    }
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

fn require_exact_test_failure(
    source: &str,
    test_name: &str,
    check_id: &str,
) -> Result<(), AggregateError> {
    let full_result = format!("test {test_name} ... FAILED");
    let exact_result = format!("::{test_name} ... FAILED");
    if !source.lines().any(|line| line.trim() == "running 1 test")
        || !source
            .lines()
            .any(|line| line.trim() == full_result || line.trim_end().ends_with(&exact_result))
        || !source
            .lines()
            .any(|line| line.contains("0 passed; 1 failed; 0 ignored"))
    {
        return Err(AggregateError::new(format!(
            "test log does not prove one exact oracle failure for {check_id}"
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
