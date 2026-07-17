use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use crate::{aggregate::AggregateError, EvidenceStatus, ResultBundle};

mod runner;

pub(super) use runner::verify_test_logs;

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

#[cfg(test)]
pub(super) fn require_detector_witness(
    bundle: &ResultBundle,
    source: &str,
    oracle_check_id: &str,
    registered_identity: &str,
) -> Result<(), AggregateError> {
    valid_witness_identity(registered_identity).ok_or_else(|| {
        AggregateError::new(format!(
            "registered detector identity is malformed: {registered_identity}"
        ))
    })?;
    require_detector_witness_contract(
        bundle,
        source,
        oracle_check_id,
        registered_identity,
        &BTreeMap::from([(format!("expect-err:{registered_identity}"), 1)]),
    )
}

pub(super) fn require_detector_witness_contract(
    bundle: &ResultBundle,
    source: &str,
    oracle_check_id: &str,
    registered_identity: &str,
    expected_witnesses: &BTreeMap<String, usize>,
) -> Result<(), AggregateError> {
    if !expected_witnesses.keys().any(|witness| {
        witness
            .split_once(':')
            .is_some_and(|(_, identity)| identity == registered_identity)
    }) {
        return Err(AggregateError::new(format!(
            "source invocation contract omits registered detector {registered_identity}"
        )));
    }
    let processes = crate::producer::process::parse_combined_processes(source)
        .map_err(|error| AggregateError::new(format!("parse detector invocation: {error}")))?;
    let exact = processes
        .iter()
        .find(|process| process.label == "exact libtest execution")
        .ok_or_else(|| {
            AggregateError::new("detector log omitted its exact invocation".to_owned())
        })?;
    let token = crate::producer::test_exec::oracle_token(&bundle.source_ref, oracle_check_id);
    let challenge = exact.detector_challenge.as_deref().ok_or_else(|| {
        AggregateError::new("detector log omitted its parent-issued challenge".to_owned())
    })?;
    require_detector_witness_contract_in_streams(
        &exact.stdout,
        &exact.stderr,
        &token,
        challenge,
        expected_witnesses,
    )
}

#[cfg(test)]
fn require_detector_witness_in_streams(
    stdout: &str,
    stderr: &str,
    token: &str,
    challenge: &str,
    registered_identity: &str,
) -> Result<(), AggregateError> {
    valid_witness_identity(registered_identity).ok_or_else(|| {
        AggregateError::new(format!(
            "registered detector identity is malformed: {registered_identity}"
        ))
    })?;
    require_detector_witness_contract_in_streams(
        stdout,
        stderr,
        token,
        challenge,
        &BTreeMap::from([(format!("expect-err:{registered_identity}"), 1)]),
    )
}

fn require_detector_witness_contract_in_streams(
    stdout: &str,
    stderr: &str,
    token: &str,
    challenge: &str,
    expected_witnesses: &BTreeMap<String, usize>,
) -> Result<(), AggregateError> {
    let witnesses = crate::detector_proof::verify_transcript(stdout, stderr, token, challenge)
        .map_err(|error| AggregateError::new(format!("detector proof failed: {error}")))?;
    if &witnesses != expected_witnesses {
        return Err(AggregateError::new(format!(
            "detector log witness contract mismatch: expected {expected_witnesses:?}, observed {witnesses:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
fn valid_witness_identity(identity: &str) -> Option<()> {
    let mut segments = identity.split("::");
    valid_identifier(segments.next()?)?;
    for segment in segments {
        valid_identifier(segment)?;
    }
    Some(())
}

#[cfg(test)]
fn valid_identifier(identifier: &str) -> Option<()> {
    (!identifier.is_empty()
        && identifier.chars().enumerate().all(|(index, character)| {
            character == '_'
                || character.is_ascii_alphanumeric() && (index > 0 || !character.is_ascii_digit())
        }))
    .then_some(())
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
    oracle_check_id: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    let processes =
        verify_test_process_plan(bundle, check, source, test_name, oracle_check_id, root)?;
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
                    &crate::producer::test_exec::oracle_token(&bundle.source_ref, oracle_check_id),
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

pub(super) fn verify_detector_harness_error_invocations(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    source: &str,
    test_name: &str,
    oracle_check_id: &str,
    root: &Path,
) -> Result<(), AggregateError> {
    verify_harness_error_test_invocations(bundle, check, source, test_name, oracle_check_id, root)?;
    let processes = crate::producer::process::parse_combined_processes(source)
        .map_err(|error| AggregateError::new(format!("parse detector invocation: {error}")))?;
    if let Some(exact) = processes
        .iter()
        .find(|process| process.label == "exact libtest execution")
    {
        verify_detector_harness_challenge(exact.detector_challenge.as_deref())?;
    }
    Ok(())
}

fn verify_detector_harness_challenge(challenge: Option<&str>) -> Result<(), AggregateError> {
    let challenge = challenge.ok_or_else(|| {
        AggregateError::new("detector harness-error log omitted its challenge".to_owned())
    })?;
    crate::detector_proof::validate_challenge(challenge).map_err(|error| {
        AggregateError::new(format!(
            "detector harness-error challenge is invalid: {error}"
        ))
    })
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
    let exact_environment = exact_test_environment(
        bundle,
        check,
        &invocations,
        test_name,
        oracle_check_id,
        Path::new(&current_dir),
    )?;
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
    verify_runner_test_observations(bundle, check, &invocations, test_name)?;
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

pub(super) fn verify_runner_test_observations(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    invocations: &[crate::producer::process::LabeledProcess],
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

fn exact_test_environment(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    invocations: &[crate::producer::process::LabeledProcess],
    test_name: &str,
    oracle_check_id: &str,
    root: &Path,
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
            root.join("target/rafter-invariants/tmp")
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
    if bundle.runner == "simulator" {
        let detector_environment = invocations
            .get(2)
            .map(|invocation| &invocation.invocation.environment)
            .ok_or_else(|| {
                AggregateError::new("detector log omitted its exact invocation".to_owned())
            })?;
        let socket = detector_environment
            .get(crate::detector_proof::PROOF_SOCKET_ENV)
            .ok_or_else(|| {
                AggregateError::new(
                    "detector execution environment omitted its proof socket".to_owned(),
                )
            })?;
        let socket_path = Path::new(socket);
        if !managed_detector_proof_socket(socket_path) {
            return Err(AggregateError::new(
                "detector proof socket is outside its managed scratch directory".to_owned(),
            ));
        }
        environment.insert(
            crate::detector_proof::PROOF_SOCKET_ENV.to_owned(),
            socket.clone(),
        );
    }
    Ok(environment)
}

fn managed_detector_proof_socket(path: &Path) -> bool {
    crate::detector_proof::managed_socket_path(path)
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

#[cfg(test)]
mod detector_witness_tests {
    use std::collections::BTreeMap;
    use std::fmt::Write as _;

    use super::{
        require_detector_witness, require_detector_witness_contract_in_streams,
        require_detector_witness_in_streams, verify_detector_harness_challenge,
    };

    const CHALLENGE: [u8; crate::detector_proof::CHALLENGE_BYTES] = [0x5a; 32];

    fn proven_transcript(token: &str, witnesses: &[&str]) -> (String, String) {
        let challenge = crate::detector_proof::encode_challenge(&CHALLENGE);
        let mut transcript = String::new();
        for witness in witnesses {
            write!(
                transcript,
                "{}{token}:{witness}()\n{}{token}:{witness}():{challenge}\n",
                crate::detector_proof::WITNESS_PREFIX,
                crate::detector_proof::PROOF_PREFIX,
            )
            .expect("writing to a String cannot fail");
        }
        (challenge, transcript)
    }

    #[test]
    fn detector_proof_socket_must_be_a_normal_managed_relative_path() {
        assert!(super::managed_detector_proof_socket(std::path::Path::new(
            "target/rafter-invariants/tmp/detector-proof/12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.sock",
        )));
        for path in [
            "/target/rafter-invariants/tmp/detector-proof/12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.sock",
            "target/rafter-invariants/tmp/detector-proof/../12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.sock",
            "target/rafter-invariants/tmp/detector-proof/nested/12-3-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a.sock",
            "target/rafter-invariants/tmp/detector-proof/12-3.txt",
        ] {
            assert!(!super::managed_detector_proof_socket(std::path::Path::new(
                path
            )));
        }
    }

    #[test]
    fn exact_detector_harness_receipt_requires_a_valid_parent_challenge() {
        let challenge = crate::detector_proof::encode_challenge(&CHALLENGE);
        verify_detector_harness_challenge(Some(&challenge))
            .expect("a parent-issued challenge is valid harness evidence");

        let missing = verify_detector_harness_challenge(None)
            .expect_err("an exact detector receipt cannot omit its challenge");
        assert!(missing.to_string().contains("omitted its challenge"));

        let malformed = verify_detector_harness_challenge(Some("not-a-challenge"))
            .expect_err("an exact detector receipt cannot invent a challenge");
        assert!(malformed.to_string().contains("challenge is invalid"));
    }

    #[test]
    fn adversarial_noop_oracle_observation_cannot_qualify_a_detector() {
        let token = "source-bound-token";
        let stdout = format!("RAFTER_INVARIANT_ORACLE_OBSERVED:{token}\n");
        let challenge = crate::detector_proof::encode_challenge(&CHALLENGE);

        let error = require_detector_witness_in_streams(
            &stdout,
            "",
            token,
            &challenge,
            "fixture::check_committed_prefix_history_stability",
        )
        .expect_err("a generic true assertion must not qualify a detector");

        assert!(error.to_string().contains("no runtime witnesses"));
    }

    #[test]
    fn detector_witness_contract_rejects_missing_duplicate_and_extra_markers() {
        let token = "source-bound-token";
        let expected = BTreeMap::from([
            ("recorder:fixture::record_observation".to_owned(), 1),
            ("expect-err:fixture::check_history".to_owned(), 1),
        ]);
        let (challenge, exact) = proven_transcript(
            token,
            &[
                "recorder:fixture::record_observation",
                "expect-err:fixture::check_history",
            ],
        );
        require_detector_witness_contract_in_streams("", &exact, token, &challenge, &expected)
            .expect("the exact source-derived witness multiset qualifies");

        for witnesses in [
            vec!["recorder:fixture::record_observation"],
            vec![
                "recorder:fixture::record_observation",
                "expect-err:fixture::check_history",
                "recorder:fixture::record_observation",
            ],
            vec![
                "recorder:fixture::record_observation",
                "expect-err:fixture::check_history",
                "recorder:fixture::unregistered",
            ],
        ] {
            let (altered_challenge, altered) = proven_transcript(token, &witnesses);
            assert!(require_detector_witness_contract_in_streams(
                "",
                &altered,
                token,
                &altered_challenge,
                &expected,
            )
            .is_err());
        }
    }

    #[test]
    fn detector_expression_witness_qualifies_only_its_named_detector() {
        let token = "source-bound-token";
        let (challenge, stderr) = proven_transcript(
            token,
            &["expect-err:fixture::check_committed_prefix_history_stability"],
        );

        require_detector_witness_in_streams(
            "",
            &stderr,
            token,
            &challenge,
            "fixture::check_committed_prefix_history_stability",
        )
        .expect("the actual detector expression is witnessed");
        assert!(require_detector_witness_in_streams(
            "",
            &stderr,
            token,
            &challenge,
            "fixture::check_stable_commit_quorums",
        )
        .is_err());
        assert!(require_detector_witness_in_streams(
            "",
            &proven_transcript(
                token,
                &["expect-err:other(check_committed_prefix_history_stability())"],
            )
            .1,
            token,
            &challenge,
            "fixture::check_committed_prefix_history_stability",
        )
        .is_err());
    }

    #[test]
    fn same_leaf_decoy_identity_cannot_qualify_the_registered_detector() {
        let token = "source-bound-token";
        let (challenge, stderr) =
            proven_transcript(token, &["expect-err:fixture::decoy::detector"]);
        let error = require_detector_witness_in_streams(
            "",
            &stderr,
            token,
            &challenge,
            "fixture::detector::detector",
        )
        .expect_err("a compiler-resolved same-leaf decoy must not qualify");
        assert!(error.to_string().contains("witness contract mismatch"));
    }

    #[test]
    fn token_bound_macro_witness_survives_libtest_capture_and_process_framing() {
        let (catalog, manifest) = crate::tests::loaded();
        let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
            .into_iter()
            .next()
            .expect("passing fixture bundle");
        bundle.source_ref = format!("e2e{:09}-detector-witness", std::process::id());
        let (check_id, source) = crate::producer::test_exec::capture_detector_witness_fixture_log(
            &bundle.source_ref,
            "token_bound_detector_witness_subprocess_fixture",
        )
        .expect("capture the real oracle macro through an exact libtest subprocess");

        require_detector_witness(
            &bundle,
            &source,
            &check_id,
            "rafter_invariant_test::tests::token_bound_regression_detector",
        )
        .expect("the framed exact-process log retains the source-bound detector witness");

        bundle.source_ref.push_str("-foreign");
        let error = require_detector_witness(
            &bundle,
            &source,
            &check_id,
            "rafter_invariant_test::tests::token_bound_regression_detector",
        )
        .expect_err("the captured witness must not qualify another source token");
        assert!(error.to_string().contains("another execution token"));
    }
}
