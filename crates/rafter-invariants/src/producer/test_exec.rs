use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path, time::Instant};

use crate::{ArtifactRef, CheckCompletion, EvidenceStatus, FailureClassification, TestIdentity};

use super::{
    artifact,
    filesystem::{HeldDirectory, OperationDeadline, TREE_LIMITS},
    process,
    test_compile::CompiledTarget,
};

mod detector_proof;

pub(crate) const ORACLE_TOKEN_ENV: &str = "RAFTER_INVARIANT_ORACLE_TOKEN";
const ORACLE_OBSERVED_PREFIX: &str = "RAFTER_INVARIANT_ORACLE_OBSERVED:";
const ORACLE_MARKER_PREFIX: &str = "RAFTER_INVARIANT_ORACLE_VIOLATION:";

pub(super) struct TestOutcome {
    pub completion: CheckCompletion,
    pub status: EvidenceStatus,
    pub classification: Option<FailureClassification>,
    pub message: Option<String>,
    pub observations: BTreeMap<String, u64>,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub artifacts: Vec<ArtifactRef>,
}

struct DiscoveryOutput {
    listed: process::ProcessOutput,
    ignored: process::ProcessOutput,
    log: Vec<u8>,
}

#[derive(Clone, Copy)]
struct EvaluationRequest<'a> {
    identity: &'a TestIdentity,
    compiled: &'a CompiledTarget,
    profile: &'a str,
    source_ref: &'a str,
    execution_id: &'a str,
    output_dir: &'a Path,
    scratch_deadline: Instant,
}

#[derive(Clone, Copy)]
enum EvaluationMode {
    Ordinary,
    Detector,
}

pub(super) fn evaluate(
    identity: &TestIdentity,
    compiled: &CompiledTarget,
    profile: &str,
    source_ref: &str,
    execution_id: &str,
    output_dir: &Path,
    scratch_deadline: Instant,
) -> Result<TestOutcome, Box<dyn Error>> {
    evaluate_request(
        EvaluationRequest {
            identity,
            compiled,
            profile,
            source_ref,
            execution_id,
            output_dir,
            scratch_deadline,
        },
        EvaluationMode::Ordinary,
    )
}

pub(super) fn evaluate_detector(
    identity: &TestIdentity,
    compiled: &CompiledTarget,
    profile: &str,
    source_ref: &str,
    execution_id: &str,
    output_dir: &Path,
    scratch_deadline: Instant,
) -> Result<TestOutcome, Box<dyn Error>> {
    evaluate_request(
        EvaluationRequest {
            identity,
            compiled,
            profile,
            source_ref,
            execution_id,
            output_dir,
            scratch_deadline,
        },
        EvaluationMode::Detector,
    )
}

fn evaluate_request(
    request: EvaluationRequest<'_>,
    mode: EvaluationMode,
) -> Result<TestOutcome, Box<dyn Error>> {
    let EvaluationRequest {
        identity,
        compiled,
        profile,
        source_ref,
        execution_id,
        output_dir,
        scratch_deadline,
    } = request;
    let Some(executable) = compiled.executable.as_ref() else {
        return Ok(error_outcome(
            compiled
                .error
                .clone()
                .unwrap_or_else(|| "test target did not compile".to_owned()),
            compiled.artifact.clone(),
            compiled.peak_rss_kib,
            compiled.duration_ms,
            0,
        ));
    };
    let program = executable
        .to_str()
        .ok_or("test executable path is not valid UTF-8")?;
    let executable_handle = compiled
        .executable_handle
        .as_ref()
        .ok_or("compiled test executable omitted its held file capability")?;
    executable_handle.verify_path_binding()?;
    let DiscoveryOutput {
        listed,
        ignored,
        log,
    } = discover(program, profile, source_ref, execution_id, output_dir)?;
    let discovery_rss = listed.peak_rss_kib.max(ignored.peak_rss_kib);
    let discovery_ms =
        process::duration_ms(listed.duration) + process::duration_ms(ignored.duration);
    let matches = discovery_matches(&listed.stdout, &identity.test_name);
    let ignored_matches = discovery_matches(&ignored.stdout, &identity.test_name);
    if let Some(message) = discovery_failure(&listed, &ignored) {
        let artifact = write_log(output_dir, profile, source_ref, execution_id, &log)?;
        return Ok(error_outcome(
            message.to_owned(),
            artifact,
            discovery_rss,
            discovery_ms,
            matches,
        ));
    }
    if matches == 0 {
        let artifact = write_log(output_dir, profile, source_ref, execution_id, &log)?;
        return Ok(TestOutcome {
            completion: CheckCompletion::CoverageNotReached,
            status: EvidenceStatus::Incomplete,
            classification: Some(FailureClassification::CoverageNotReached),
            message: Some(format!(
                "exact libtest identity {} was discovered {matches} times",
                identity.test_name
            )),
            observations: observations(matches, 0, 0),
            duration_ms: discovery_ms,
            peak_rss_kib: discovery_rss,
            artifacts: vec![artifact],
        });
    }
    if matches != 1 || ignored_matches > 1 {
        let artifact = write_log(output_dir, profile, source_ref, execution_id, &log)?;
        return Ok(error_outcome(
            format!(
                "exact libtest identity {} was discovered {matches} times and ignored-discovered {ignored_matches} times",
                identity.test_name
            ),
            artifact,
            discovery_rss,
            discovery_ms,
            matches,
        ));
    }
    executable_handle.verify_path_binding()?;
    execute_exact(
        identity,
        profile,
        source_ref,
        execution_id,
        output_dir,
        program,
        ignored_matches == 1,
        log,
        discovery_ms,
        discovery_rss,
        scratch_deadline,
        matches!(mode, EvaluationMode::Detector),
    )
}

fn discovery_failure(
    listed: &process::ProcessOutput,
    ignored: &process::ProcessOutput,
) -> Option<&'static str> {
    if listed.timed_out || ignored.timed_out {
        Some("libtest discovery process timed out")
    } else if !listed.status.success() || !ignored.status.success() {
        Some("libtest discovery process failed")
    } else {
        None
    }
}

#[cfg(test)]
pub(crate) fn capture_detector_witness_fixture_log(
    source_ref: &str,
    fixture: &str,
) -> Result<(String, String), Box<dyn Error>> {
    let identity = TestIdentity {
        package: "rafter-invariant-test".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_invariant_test".to_owned(),
        test_name: format!("tests::{fixture}"),
    };
    capture_detector_witness_identity_log(source_ref, &identity, true, true)
}

#[cfg(test)]
pub(crate) fn capture_fabricated_detector_witness_fixture_log(
    source_ref: &str,
    fixture: &str,
) -> Result<(String, String), Box<dyn Error>> {
    let identity = TestIdentity {
        package: "rafter-invariant-test".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_invariant_test".to_owned(),
        test_name: format!("tests::{fixture}"),
    };
    capture_detector_witness_identity_log(source_ref, &identity, true, false)
}

#[cfg(test)]
pub(crate) fn capture_qualified_helper_forged_transcript_fixture_log(
    source_ref: &str,
) -> Result<(String, String), Box<dyn Error>> {
    let identity = TestIdentity {
        package: "rafter-invariant-test".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_invariant_test".to_owned(),
        test_name: "tests::qualified_helper_forged_transcript_subprocess_fixture".to_owned(),
    };
    capture_detector_witness_identity_log(source_ref, &identity, true, true)
}

#[cfg(test)]
pub(crate) fn capture_hidden_proof_socket_fixture_log(
    source_ref: &str,
) -> Result<(String, String), Box<dyn Error>> {
    let identity = TestIdentity {
        package: "rafter-invariant-test".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_invariant_test".to_owned(),
        test_name: "tests::proof_socket_is_hidden_from_fixture_body_subprocess_fixture".to_owned(),
    };
    capture_detector_witness_identity_log(source_ref, &identity, true, true)
}

#[cfg(test)]
pub(crate) fn capture_removed_token_detector_fixture_log(
    source_ref: &str,
) -> Result<(String, String), Box<dyn Error>> {
    let identity = TestIdentity {
        package: "rafter-invariant-test".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_invariant_test".to_owned(),
        test_name: "tests::detector_witness_with_removed_token_subprocess_fixture".to_owned(),
    };
    capture_detector_witness_identity_log(source_ref, &identity, true, false)
}

#[cfg(test)]
pub(crate) fn capture_registered_detector_fixture_log(
    source_ref: &str,
    identity: &TestIdentity,
) -> Result<(String, String), Box<dyn Error>> {
    capture_detector_witness_identity_log(source_ref, identity, false, true)
}

#[cfg(test)]
fn capture_detector_witness_identity_log(
    source_ref: &str,
    identity: &TestIdentity,
    ignored: bool,
    expect_success: bool,
) -> Result<(String, String), Box<dyn Error>> {
    if identity.target_kind != "lib" {
        return Err("detector witness regression helper requires a library target".into());
    }
    let check_id = identity.check_id();
    let fixture = identity
        .test_name
        .rsplit("::")
        .next()
        .ok_or("detector witness fixture has no leaf name")?;
    let target_dir = Path::new("target/rafter-invariants").join(format!(
        "detector-witness-e2e-build-{}-{fixture}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&target_dir);

    let result = (|| {
        let executable = compile_detector_witness_executable(identity, &target_dir)?;
        let program = executable
            .to_str()
            .ok_or("detector witness fixture path is not valid UTF-8")?;
        let mut arguments = vec![
            identity.test_name.clone().into(),
            "--exact".into(),
            "--test-threads=1".into(),
            "--show-output".into(),
            "--color".into(),
            "never".into(),
        ];
        if ignored {
            arguments.push("--ignored".into());
        }
        let mut environment = process::base_environment();
        environment.insert(
            ORACLE_TOKEN_ENV.to_owned(),
            oracle_token(source_ref, &check_id),
        );
        let detector_proof::Execution {
            output: captured,
            challenge,
            channel_error,
        } = detector_proof::execute_for_test(program, &arguments, &mut environment)?;
        if let Some(error) = channel_error {
            return Err(format!("detector witness proof channel failed: {error}").into());
        }
        if captured.status.success() != expect_success {
            return Err(format!(
                "detector witness fixture exact libtest execution success={} expected={expect_success}; stdout={}; stderr={}",
                captured.status.success(),
                String::from_utf8_lossy(&captured.stdout),
                String::from_utf8_lossy(&captured.stderr),
            )
            .into());
        }
        let source = String::from_utf8(process::combined_detector_log(
            "exact libtest execution",
            &captured,
            &challenge,
        )?)?;
        Ok((check_id, source))
    })();

    let _ = std::fs::remove_dir_all(target_dir);
    result
}

#[cfg(test)]
fn compile_detector_witness_executable(
    identity: &TestIdentity,
    target_dir: &Path,
) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let compiled = std::process::Command::new("cargo")
        .args([
            "test",
            "--locked",
            "--no-default-features",
            "-p",
            &identity.package,
            "--lib",
            "--no-run",
            "--message-format=json-render-diagnostics",
        ])
        .env("CARGO_TARGET_DIR", target_dir)
        .output()?;
    if !compiled.status.success() {
        return Err(format!(
            "compile detector witness fixture: {}",
            String::from_utf8_lossy(&compiled.stderr)
        )
        .into());
    }
    let executables = String::from_utf8_lossy(&compiled.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|message| message["reason"] == "compiler-artifact")
        .filter(|message| message["target"]["name"] == identity.target)
        .filter(|message| message["target"]["kind"] == serde_json::json!(["lib"]))
        .filter_map(|message| message["executable"].as_str().map(std::path::PathBuf::from))
        .collect::<Vec<_>>();
    let [executable] = executables.as_slice() else {
        return Err(format!(
            "expected one detector witness fixture executable, found {}",
            executables.len()
        )
        .into());
    };
    Ok(std::fs::canonicalize(executable)?)
}

fn discover(
    program: &str,
    profile: &str,
    source_ref: &str,
    execution_id: &str,
    output_dir: &Path,
) -> Result<DiscoveryOutput, Box<dyn Error>> {
    let environment = process::base_environment();
    let listed = process::timed_for(
        process::ProcessKind::TestDiscovery,
        program,
        &["--list".into(), "--format".into(), "terse".into()],
        &environment,
        Path::new("."),
    )?;
    let mut log = process::combined_log("libtest discovery", &listed)?;
    persist_log(output_dir, profile, source_ref, execution_id, &log)?;
    let ignored = process::timed_for(
        process::ProcessKind::TestDiscovery,
        program,
        &[
            "--ignored".into(),
            "--list".into(),
            "--format".into(),
            "terse".into(),
        ],
        &environment,
        Path::new("."),
    )?;
    log.extend(process::combined_log(
        "libtest ignored discovery",
        &ignored,
    )?);
    persist_log(output_dir, profile, source_ref, execution_id, &log)?;
    Ok(DiscoveryOutput {
        listed,
        ignored,
        log,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_exact(
    identity: &TestIdentity,
    profile: &str,
    source_ref: &str,
    execution_id: &str,
    output_dir: &Path,
    program: &str,
    ignored: bool,
    mut log: Vec<u8>,
    discovery_ms: u64,
    discovery_rss: u64,
    scratch_deadline: Instant,
    require_detector_proof: bool,
) -> Result<TestOutcome, Box<dyn Error>> {
    let temporary = Path::new("target/rafter-invariants/tmp").join(execution_id);
    let temporary_guard = reset_test_scratch(&temporary, scratch_deadline)?;
    let seed = artifact::deterministic_u64(
        "rafter-tests/v1",
        &format!("{profile}\0{source_ref}\0{}", identity.test_name),
    );
    let mut run_environment = process::base_environment();
    run_environment.extend([
        ("PROPTEST_RNG_SEED".to_owned(), seed.to_string()),
        (
            "PROPTEST_DISABLE_FAILURE_PERSISTENCE".to_owned(),
            "1".to_owned(),
        ),
        (
            "TMPDIR".to_owned(),
            temporary_guard
                .external_path()
                .to_string_lossy()
                .into_owned(),
        ),
    ]);
    run_environment.insert("RUST_BACKTRACE".to_owned(), "1".to_owned());
    let oracle_token = oracle_token(source_ref, &identity.check_id());
    run_environment.insert(ORACLE_TOKEN_ENV.to_owned(), oracle_token.clone());
    let mut arguments = vec![
        OsString::from(&identity.test_name),
        "--exact".into(),
        "--test-threads=1".into(),
        "--show-output".into(),
        "--color".into(),
        "never".into(),
    ];
    if ignored {
        arguments.push("--ignored".into());
    }
    temporary_guard.verify_path_binding()?;
    let ExactProcessExecution {
        output: executed,
        detector_challenge,
        classification: execution,
        harness_error,
    } = run_exact_process(
        program,
        &arguments,
        &mut run_environment,
        &identity.test_name,
        &oracle_token,
        require_detector_proof,
    )?;
    if let Some(challenge) = &detector_challenge {
        log.extend(process::combined_detector_log(
            "exact libtest execution",
            &executed,
            challenge,
        )?);
    } else {
        log.extend(process::combined_log("exact libtest execution", &executed)?);
    }
    let artifact = write_log(output_dir, profile, source_ref, execution_id, &log)?;
    let peak_rss_kib = discovery_rss.max(executed.peak_rss_kib);
    let duration_ms = discovery_ms + process::duration_ms(executed.duration);
    let exact_passed = !executed.timed_out
        && executed.status.code() == Some(0)
        && exact_pass(&executed.stdout, &identity.test_name);
    let exact_was_run = exact_passed
        || (executed.status.code() == Some(101)
            && !executed.timed_out
            && exact_failure(&executed.stdout, &identity.test_name));
    Ok(outcome_from_execution(
        execution,
        &identity.test_name,
        ExactOutcomeEvidence {
            artifact,
            duration_ms,
            peak_rss_kib,
            exact_was_run,
            exact_passed,
            harness_error: harness_error.as_deref(),
        },
    ))
}

struct ExactProcessExecution {
    output: process::ProcessOutput,
    detector_challenge: Option<String>,
    classification: ExactTestExecution,
    harness_error: Option<String>,
}

fn run_exact_process(
    program: &str,
    arguments: &[OsString],
    environment: &mut BTreeMap<String, String>,
    test_name: &str,
    oracle_token: &str,
    require_detector_proof: bool,
) -> Result<ExactProcessExecution, Box<dyn Error>> {
    let (output, detector_challenge, harness_error) = if require_detector_proof {
        let detector_proof::Execution {
            output,
            challenge,
            channel_error,
        } = detector_proof::execute(program, arguments, environment)?;
        (
            output,
            Some(challenge),
            channel_error.map(|error| format!("detector proof channel failed: {error}")),
        )
    } else {
        (
            process::timed_for(
                process::ProcessKind::TestExecution,
                program,
                arguments,
                environment,
                Path::new("."),
            )?,
            None,
            None,
        )
    };
    let mut classification = classify_exact_execution(
        &output.stdout,
        &output.stderr,
        test_name,
        oracle_token,
        output.status.code(),
        output.timed_out,
    );
    if harness_error.is_some() {
        classification = ExactTestExecution::HarnessError;
    } else if let Some(challenge) = &detector_challenge {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let proven =
            crate::detector_proof::verify_transcript(&stdout, &stderr, oracle_token, challenge);
        if !proven.is_ok_and(|witnesses| {
            witnesses
                .keys()
                .any(|witness| witness.starts_with("expect-err:"))
        }) {
            classification = ExactTestExecution::HarnessError;
        }
    }
    Ok(ExactProcessExecution {
        output,
        detector_challenge,
        classification,
        harness_error,
    })
}

pub(super) fn reset_test_scratch(
    path: &Path,
    deadline: Instant,
) -> Result<HeldDirectory, Box<dyn Error>> {
    HeldDirectory::replace_tree(
        path,
        TREE_LIMITS,
        OperationDeadline::at(deadline, "test scratch cleanup"),
    )
}

struct ExactOutcomeEvidence<'a> {
    artifact: ArtifactRef,
    duration_ms: u64,
    peak_rss_kib: u64,
    exact_was_run: bool,
    exact_passed: bool,
    harness_error: Option<&'a str>,
}

fn outcome_from_execution(
    execution: ExactTestExecution,
    test_name: &str,
    evidence: ExactOutcomeEvidence<'_>,
) -> TestOutcome {
    let ExactOutcomeEvidence {
        artifact,
        duration_ms,
        peak_rss_kib,
        exact_was_run,
        exact_passed,
        harness_error,
    } = evidence;
    match execution {
        ExactTestExecution::Pass => TestOutcome {
            completion: CheckCompletion::Completed,
            status: EvidenceStatus::Pass,
            classification: None,
            message: None,
            observations: observations(1, 1, 1),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        },
        ExactTestExecution::CoverageNotReached => TestOutcome {
            completion: CheckCompletion::CoverageNotReached,
            status: EvidenceStatus::Incomplete,
            classification: Some(FailureClassification::CoverageNotReached),
            message: Some("libtest executed zero exact tests".to_owned()),
            observations: observations(1, usize::from(exact_was_run), usize::from(exact_passed)),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        },
        ExactTestExecution::InvariantViolation => TestOutcome {
            completion: CheckCompletion::Counterexample,
            status: EvidenceStatus::Fail,
            classification: Some(FailureClassification::InvariantViolation),
            message: Some(format!(
                "direct oracle {test_name} reported an invariant violation"
            )),
            observations: observations(1, 1, 0),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        },
        ExactTestExecution::HarnessError => TestOutcome {
            completion: CheckCompletion::HarnessError,
            status: EvidenceStatus::Error,
            classification: Some(FailureClassification::HarnessError),
            message: Some(harness_error.map_or_else(
                || {
                    format!(
                        "exact test process {test_name} failed without one canonical libtest verdict"
                    )
                },
                |error| format!("exact test process {test_name} failed: {error}"),
            )),
            observations: observations(1, usize::from(exact_was_run), 0),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        },
    }
}

pub(crate) fn listed_tests(output: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(output)
        .lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .map(str::to_owned)
        .collect()
}

fn discovery_matches(output: &[u8], test_name: &str) -> usize {
    listed_tests(output)
        .iter()
        .filter(|test| test.as_str() == test_name)
        .count()
}

pub(crate) fn exact_pass(output: &[u8], test_name: &str) -> bool {
    let output = String::from_utf8_lossy(output);
    count_exact_line(&output, "running 1 test") == 1
        && count_exact_line(&output, &format!("test {test_name} ... ok")) == 1
        && count_summary(&output, "test result: ok. 1 passed; 0 failed; 0 ignored") == 1
}

pub(crate) fn exact_failure(output: &[u8], test_name: &str) -> bool {
    let output = String::from_utf8_lossy(output);
    count_exact_line(&output, "running 1 test") == 1
        && count_exact_line(&output, &format!("test {test_name} ... FAILED")) == 1
        && count_summary(
            &output,
            "test result: FAILED. 0 passed; 1 failed; 0 ignored",
        ) == 1
}

pub(crate) fn oracle_token(source_ref: &str, check_id: &str) -> String {
    artifact::stable_id("oracle", &format!("{source_ref}\0{check_id}"))
}

fn oracle_markers(stdout: &[u8], stderr: &[u8], token: &str) -> Option<(usize, usize)> {
    let observed = format!("{ORACLE_OBSERVED_PREFIX}{token}");
    let violation = format!("{ORACLE_MARKER_PREFIX}{token}");
    let stdout = String::from_utf8_lossy(stdout);
    let stderr = String::from_utf8_lossy(stderr);
    let streams = [stdout.as_ref(), stderr.as_ref()];
    let observed_count = streams
        .iter()
        .map(|stream| stream.matches(&observed).count())
        .sum::<usize>();
    let violation_count = streams
        .iter()
        .map(|stream| stream.matches(&violation).count())
        .sum::<usize>();
    let all_observed = streams
        .iter()
        .map(|stream| stream.matches(ORACLE_OBSERVED_PREFIX).count())
        .sum::<usize>();
    let all_violations = streams
        .iter()
        .map(|stream| stream.matches(ORACLE_MARKER_PREFIX).count())
        .sum::<usize>();
    (observed_count == all_observed && violation_count == all_violations)
        .then_some((observed_count, violation_count))
}

fn exact_zero_execution(output: &[u8]) -> bool {
    let output = String::from_utf8_lossy(output);
    count_exact_line(&output, "running 0 tests") == 1
        && count_summary(&output, "test result: ok. 0 passed; 0 failed; 0 ignored") == 1
        && !output
            .lines()
            .any(|line| line.trim_start().starts_with("test ") && line.contains(" ... "))
}

fn count_exact_line(output: &str, expected: &str) -> usize {
    output
        .lines()
        .filter(|line| line.trim() == expected)
        .count()
}

fn count_summary(output: &str, expected_prefix: &str) -> usize {
    output
        .lines()
        .filter(|line| line.trim().starts_with(expected_prefix))
        .count()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExactTestExecution {
    Pass,
    InvariantViolation,
    CoverageNotReached,
    HarnessError,
}

pub(crate) fn classify_exact_execution(
    stdout: &[u8],
    stderr: &[u8],
    test_name: &str,
    oracle_token: &str,
    exit_code: Option<i32>,
    timed_out: bool,
) -> ExactTestExecution {
    if timed_out {
        return ExactTestExecution::HarnessError;
    }
    let Some((observed, violations)) = oracle_markers(stdout, stderr, oracle_token) else {
        return ExactTestExecution::HarnessError;
    };
    match exit_code {
        Some(0) if exact_pass(stdout, test_name) && observed == 1 && violations == 0 => {
            ExactTestExecution::Pass
        }
        Some(0) if exact_pass(stdout, test_name) && observed == 0 && violations == 0 => {
            ExactTestExecution::CoverageNotReached
        }
        Some(0) if exact_zero_execution(stdout) && observed == 0 && violations == 0 => {
            ExactTestExecution::CoverageNotReached
        }
        Some(101) if exact_failure(stdout, test_name) && observed <= 1 && violations == 1 => {
            ExactTestExecution::InvariantViolation
        }
        _ => ExactTestExecution::HarnessError,
    }
}

fn observations(discovered: usize, executed: usize, passed: usize) -> BTreeMap<String, u64> {
    BTreeMap::from([
        ("discovered".to_owned(), discovered as u64),
        ("executed".to_owned(), executed as u64),
        ("passed".to_owned(), passed as u64),
    ])
}

fn write_log(
    output_dir: &Path,
    profile: &str,
    source_ref: &str,
    execution_id: &str,
    bytes: &[u8],
) -> Result<ArtifactRef, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    artifact::write(
        output_dir,
        Path::new(&format!(
            "{profile}-tests/{source_prefix}/checks/{execution_id}.log"
        )),
        "test-log",
        bytes,
    )
}

fn persist_log(
    output_dir: &Path,
    profile: &str,
    source_ref: &str,
    execution_id: &str,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    write_log(output_dir, profile, source_ref, execution_id, bytes).map(|_| ())
}

fn error_outcome(
    message: String,
    artifact: ArtifactRef,
    peak_rss_kib: u64,
    duration_ms: u64,
    discovered: usize,
) -> TestOutcome {
    TestOutcome {
        completion: CheckCompletion::HarnessError,
        status: EvidenceStatus::Error,
        classification: Some(FailureClassification::HarnessError),
        message: Some(message),
        observations: observations(discovered, 0, 0),
        duration_ms,
        peak_rss_kib,
        artifacts: vec![artifact],
    }
}

#[cfg(all(test, unix))]
#[path = "test_exec/process_tests.rs"]
mod process_tests;
