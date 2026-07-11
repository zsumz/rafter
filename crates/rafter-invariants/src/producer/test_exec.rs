use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    ffi::OsString,
    fs,
    path::Path,
};

use crate::{ArtifactRef, CheckCompletion, EvidenceStatus, FailureClassification, TestIdentity};

use super::{artifact, process, tests::CompiledTarget};

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

pub(super) fn evaluate(
    identity: &TestIdentity,
    compiled: &CompiledTarget,
    profile: &str,
    source_ref: &str,
    execution_id: &str,
    output_dir: &Path,
) -> Result<TestOutcome, Box<dyn Error>> {
    let Some(executable) = compiled.executable.as_ref() else {
        return Ok(error_outcome(
            compiled
                .error
                .clone()
                .unwrap_or_else(|| "test target did not compile".to_owned()),
            compiled.artifact.clone(),
            compiled.peak_rss_kib,
        ));
    };
    let program = executable
        .to_str()
        .ok_or("test executable path is not valid UTF-8")?;
    let environment = process::base_environment();
    let listed = process::timed(
        program,
        &["--list".into(), "--format".into(), "terse".into()],
        &environment,
        Path::new("."),
    )?;
    let ignored = process::timed(
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
    let mut log = process::combined_log("libtest discovery", &listed);
    log.extend(process::combined_log("libtest ignored discovery", &ignored));
    let discovery_rss = listed.peak_rss_kib.max(ignored.peak_rss_kib);
    let discovery_ms =
        process::duration_ms(listed.duration) + process::duration_ms(ignored.duration);
    if !listed.status.success() || !ignored.status.success() {
        let artifact = write_log(output_dir, profile, source_ref, execution_id, &log)?;
        return Ok(error_outcome(
            "libtest discovery process failed".to_owned(),
            artifact,
            discovery_rss,
        ));
    }
    let discovered = listed_tests(&listed.stdout);
    let ignored_tests = listed_tests(&ignored.stdout);
    let matches = usize::from(discovered.contains(&identity.test_name));
    let ignored_matches = usize::from(ignored_tests.contains(&identity.test_name));
    if matches != 1 {
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
    )
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
) -> Result<TestOutcome, Box<dyn Error>> {
    let temporary = Path::new("target/rafter-invariants/tmp").join(execution_id);
    if temporary.exists() {
        fs::remove_dir_all(&temporary)?;
    }
    fs::create_dir_all(&temporary)?;
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
            temporary.to_string_lossy().into_owned(),
        ),
    ]);
    run_environment.insert("RUST_BACKTRACE".to_owned(), "1".to_owned());
    let mut arguments = vec![
        OsString::from(&identity.test_name),
        "--exact".into(),
        "--test-threads=1".into(),
        "--color".into(),
        "never".into(),
    ];
    if ignored {
        arguments.push("--ignored".into());
    }
    let executed = process::timed(program, &arguments, &run_environment, Path::new("."))?;
    log.extend(process::combined_log("exact libtest execution", &executed));
    let artifact = write_log(output_dir, profile, source_ref, execution_id, &log)?;
    let peak_rss_kib = discovery_rss.max(executed.peak_rss_kib);
    let duration_ms = discovery_ms + process::duration_ms(executed.duration);
    let exact_pass = exact_pass(&executed.stdout, &identity.test_name);
    if executed.status.success() && exact_pass {
        Ok(TestOutcome {
            completion: CheckCompletion::Completed,
            status: EvidenceStatus::Pass,
            classification: None,
            message: None,
            observations: observations(1, 1, 1),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        })
    } else if executed.status.success() {
        Ok(TestOutcome {
            completion: CheckCompletion::CoverageNotReached,
            status: EvidenceStatus::Incomplete,
            classification: Some(FailureClassification::CoverageNotReached),
            message: Some("libtest exited successfully without one exact passing test".to_owned()),
            observations: observations(1, 0, 0),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        })
    } else if confirmed_test_failure(&executed.stdout, &identity.test_name) {
        Ok(TestOutcome {
            completion: CheckCompletion::Counterexample,
            status: EvidenceStatus::Fail,
            classification: Some(FailureClassification::InvariantViolation),
            message: Some(format!("exact test {} failed", identity.test_name)),
            observations: observations(1, 1, 0),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        })
    } else {
        Ok(TestOutcome {
            completion: CheckCompletion::HarnessError,
            status: EvidenceStatus::Error,
            classification: Some(FailureClassification::HarnessError),
            message: Some(format!(
                "exact test process {} terminated without a confirmed libtest assertion failure",
                identity.test_name
            )),
            observations: observations(1, 0, 0),
            duration_ms,
            peak_rss_kib,
            artifacts: vec![artifact],
        })
    }
}

fn listed_tests(output: &[u8]) -> BTreeSet<String> {
    String::from_utf8_lossy(output)
        .lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .map(str::to_owned)
        .collect()
}

fn exact_pass(output: &[u8], test_name: &str) -> bool {
    let output = String::from_utf8_lossy(output);
    output.lines().any(|line| line.trim() == "running 1 test")
        && output
            .lines()
            .any(|line| line.trim() == format!("test {test_name} ... ok"))
        && output
            .lines()
            .any(|line| line.contains("1 passed; 0 failed; 0 ignored"))
}

fn confirmed_test_failure(output: &[u8], test_name: &str) -> bool {
    let output = String::from_utf8_lossy(output);
    output
        .lines()
        .any(|line| line.trim() == format!("test {test_name} ... FAILED"))
        && output
            .lines()
            .any(|line| line.contains("0 passed; 1 failed"))
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

fn error_outcome(message: String, artifact: ArtifactRef, peak_rss_kib: u64) -> TestOutcome {
    TestOutcome {
        completion: CheckCompletion::HarnessError,
        status: EvidenceStatus::Error,
        classification: Some(FailureClassification::HarnessError),
        message: Some(message),
        observations: observations(0, 0, 0),
        duration_ms: 0,
        peak_rss_kib,
        artifacts: vec![artifact],
    }
}

#[cfg(test)]
mod tests {
    use super::{confirmed_test_failure, exact_pass, listed_tests};

    #[test]
    fn exact_pass_rejects_zero_test_success() {
        let zero = b"running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored";
        assert!(!exact_pass(zero, "module::test"));
    }

    #[test]
    fn terse_discovery_uses_exact_identity() {
        let tests = listed_tests(b"module::one: test\nmodule::two: test\n");
        assert!(tests.contains("module::one"));
        assert!(!tests.contains("one"));
    }

    #[test]
    fn abnormal_exit_is_not_a_confirmed_invariant_failure() {
        assert!(confirmed_test_failure(
            b"test module::test ... FAILED\ntest result: FAILED. 0 passed; 1 failed",
            "module::test"
        ));
        assert!(!confirmed_test_failure(
            b"dyld: Library not loaded",
            "module::test"
        ));
    }
}
