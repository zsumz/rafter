//! End-to-end evaluation of one compiled exact libtest identity.

use std::{error::Error, ffi::OsString, path::Path, time::Instant};

use crate::{
    contract::TestIdentity,
    evidence::{
        format::libtest::{exact_failure, exact_pass, oracle_token, ORACLE_TOKEN_ENV},
        CheckCompletion, EvidenceStatus, FailureClassification,
    },
};

use super::{
    artifact_log,
    discovery::{self, DiscoveryOutput},
    outcome::{self, ExactOutcomeEvidence, TestOutcome},
};
use crate::producer::{process, test_compile::CompiledTarget};

use super::execution::{reset_test_scratch, run_exact_process, ExactProcessExecution};

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

pub(in crate::producer) fn evaluate(
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

pub(in crate::producer) fn evaluate_detector(
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
        scratch_deadline: _,
    } = request;
    let Some(executable) = compiled.executable.as_ref() else {
        return Ok(outcome::error(
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
    } = discovery::discover(program, profile, source_ref, execution_id, output_dir)?;
    let discovery_rss = listed.peak_rss_kib.max(ignored.peak_rss_kib);
    let discovery_ms =
        process::duration_ms(listed.duration) + process::duration_ms(ignored.duration);
    let matches = discovery::exact_matches(&listed.stdout, &identity.test_name);
    let ignored_matches = discovery::exact_matches(&ignored.stdout, &identity.test_name);
    if let Some(message) = discovery::failure(&listed, &ignored) {
        let artifact = artifact_log::write(output_dir, profile, source_ref, execution_id, &log)?;
        return Ok(outcome::error(
            message.to_owned(),
            artifact,
            discovery_rss,
            discovery_ms,
            matches,
        ));
    }
    if matches == 0 {
        let artifact = artifact_log::write(output_dir, profile, source_ref, execution_id, &log)?;
        return Ok(TestOutcome {
            completion: CheckCompletion::CoverageNotReached,
            status: EvidenceStatus::Incomplete,
            classification: Some(FailureClassification::CoverageNotReached),
            message: Some(format!(
                "exact libtest identity {} was discovered {matches} times",
                identity.test_name
            )),
            observations: outcome::observations(matches, 0, 0),
            duration_ms: discovery_ms,
            peak_rss_kib: discovery_rss,
            artifacts: vec![artifact],
        });
    }
    if matches != 1 || ignored_matches > 1 {
        let artifact = artifact_log::write(output_dir, profile, source_ref, execution_id, &log)?;
        return Ok(outcome::error(
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
        request,
        program,
        ignored_matches == 1,
        log,
        discovery_ms,
        discovery_rss,
        matches!(mode, EvaluationMode::Detector),
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_exact(
    request: EvaluationRequest<'_>,
    program: &str,
    ignored: bool,
    mut log: Vec<u8>,
    discovery_ms: u64,
    discovery_rss: u64,
    require_detector_proof: bool,
) -> Result<TestOutcome, Box<dyn Error>> {
    let EvaluationRequest {
        identity,
        profile,
        source_ref,
        execution_id,
        output_dir,
        scratch_deadline,
        ..
    } = request;
    let temporary = Path::new("target/rafter-invariants/tmp").join(execution_id);
    let temporary_guard = reset_test_scratch(&temporary, scratch_deadline)?;
    let seed = crate::provenance::invocation::deterministic_u64(
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
        classification,
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
    let artifact = artifact_log::write(output_dir, profile, source_ref, execution_id, &log)?;
    let peak_rss_kib = discovery_rss.max(executed.peak_rss_kib);
    let duration_ms = discovery_ms + process::duration_ms(executed.duration);
    let exact_passed = !executed.timed_out
        && executed.status.code() == Some(0)
        && exact_pass(&executed.stdout, &identity.test_name);
    let exact_was_run = exact_passed
        || (executed.status.code() == Some(101)
            && !executed.timed_out
            && exact_failure(&executed.stdout, &identity.test_name));

    Ok(outcome::from_execution(
        classification,
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
