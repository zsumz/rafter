//! Libtest invocation plans, provenance, environments, and observations.

use std::{collections::BTreeMap, path::Path};

use crate::{
    evidence::{CheckReceipt, ResultBundle},
    verification::{AggregateError, RecordedWorkspace},
};

use super::environment::{exact_test_environment, verify_exact_environment};

pub(crate) fn require_unique_discovery(
    processes: &[crate::evidence::format::process::LabeledProcess],
    test_name: &str,
) -> Result<(), AggregateError> {
    let listed_matches =
        crate::evidence::format::libtest::listed_tests(processes[0].stdout.as_bytes())
            .into_iter()
            .filter(|test| test.as_str() == test_name)
            .count();
    let ignored_matches =
        crate::evidence::format::libtest::listed_tests(processes[1].stdout.as_bytes())
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

pub(super) fn verify_test_process_plan(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    source: &str,
    test_name: &str,
    oracle_check_id: &str,
    root: &Path,
) -> Result<Vec<crate::evidence::format::process::LabeledProcess>, AggregateError> {
    let invocations = crate::evidence::format::process::parse_combined_processes(source)
        .map_err(|error| AggregateError::new(format!("parse test invocation: {error}")))?;
    if invocations.iter().take(2).any(|process| {
        process.schema_version != crate::evidence::format::process::COMBINED_PROCESS_SCHEMA_VERSION
    }) {
        return Err(AggregateError::new(
            "test discovery invocation uses a noncanonical process schema".to_owned(),
        ));
    }
    let binary = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "test-binary")
        .ok_or_else(|| AggregateError::new("test binary artifact is missing".to_owned()))?;
    let workspace = RecordedWorkspace::new(bundle, root)?;
    let current_dir = workspace.producer().to_string_lossy().into_owned();
    let base_environment = &bundle.execution.invocation.environment;
    let base_digest = bundle.execution.invocation.environment_sha256.as_str();
    let exact_environment = exact_test_environment(
        bundle,
        check,
        &invocations,
        test_name,
        oracle_check_id,
        Path::new(&current_dir),
    )?;
    let exact_digest = crate::provenance::invocation::digest_environment(&exact_environment)
        .map_err(|error| AggregateError::new(error.to_string()))?;
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
                    || observed.invocation.environment != *base_environment
                    || observed.invocation.environment_sha256 != expected.2
                    || !crate::provenance::invocation::environment_matches_digest(
                        &observed.invocation.environment,
                        expected.2,
                    )
            })
    {
        return Err(AggregateError::new(
            "test log does not contain the exact discovery invocation plan".to_owned(),
        ));
    }
    verify_runner_test_observations(bundle, check, &invocations, test_name)?;
    if let Some(exact) = invocations.get(2) {
        let ignored_matches =
            crate::evidence::format::libtest::listed_tests(invocations[1].stdout.as_bytes())
                .into_iter()
                .filter(|test| test.as_str() == test_name)
                .count();
        verify_exact_test_arguments(exact, test_name, ignored_matches)?;
        verify_exact_environment(exact, &exact_environment, &exact_digest)?;
    }
    verify_test_invocation_provenance(bundle, &invocations, &binary.sha256, &current_dir)?;
    Ok(invocations)
}

fn verify_test_invocation_provenance(
    bundle: &ResultBundle,
    invocations: &[crate::evidence::format::process::LabeledProcess],
    binary_sha256: &str,
    current_dir: &str,
) -> Result<(), AggregateError> {
    if invocations
        .iter()
        .any(|invocation| invocation.invocation.program_sha256 != binary_sha256)
    {
        return Err(AggregateError::new(
            "test log executable digest does not match its binary artifact".to_owned(),
        ));
    }
    if invocations.iter().any(|invocation| {
        !crate::verification::process_invocation_matches_source(
            &invocation.invocation,
            &bundle.execution.source,
        )
    }) {
        return Err(AggregateError::new(
            "test log launcher chain does not match source provenance".to_owned(),
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

fn verify_exact_test_arguments(
    exact: &crate::evidence::format::process::LabeledProcess,
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

pub(crate) fn verify_reconstructed_test_observations(
    check: &CheckReceipt,
    invocations: &[crate::evidence::format::process::LabeledProcess],
    test_name: &str,
) -> Result<(), AggregateError> {
    let listed_matches =
        crate::evidence::format::libtest::listed_tests(invocations[0].stdout.as_bytes())
            .into_iter()
            .filter(|test| test.as_str() == test_name)
            .count();
    let exact = invocations.get(2);
    let executed_test_name = exact
        .and_then(|process| process.invocation.arguments.first())
        .map_or(test_name, String::as_str);
    let executed = usize::from(exact.is_some_and(|process| {
        crate::evidence::format::libtest::exact_pass(process.stdout.as_bytes(), executed_test_name)
            || crate::evidence::format::libtest::exact_failure(
                process.stdout.as_bytes(),
                executed_test_name,
            )
    }));
    let passed = usize::from(exact.is_some_and(|process| {
        crate::evidence::format::libtest::exact_pass(process.stdout.as_bytes(), executed_test_name)
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

pub(crate) fn verify_runner_test_observations(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    invocations: &[crate::evidence::format::process::LabeledProcess],
    test_name: &str,
) -> Result<(), AggregateError> {
    match bundle.runner.as_str() {
        "tests" => verify_reconstructed_test_observations(check, invocations, test_name),
        "simulator" => require_unique_discovery(invocations, test_name),
        runner => Err(AggregateError::new(format!(
            "test transcript verification is unsupported for runner {runner}"
        ))),
    }
}
