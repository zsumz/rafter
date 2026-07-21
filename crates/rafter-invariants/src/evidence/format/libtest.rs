//! Neutral decoding for canonical libtest and oracle-marker transcripts.

use sha2::{Digest, Sha256};

pub(crate) const ORACLE_TOKEN_ENV: &str = "RAFTER_INVARIANT_ORACLE_TOKEN";
const ORACLE_OBSERVED_PREFIX: &str = "RAFTER_INVARIANT_ORACLE_OBSERVED:";
const ORACLE_MARKER_PREFIX: &str = "RAFTER_INVARIANT_ORACLE_VIOLATION:";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OracleMarkerCounts {
    pub observed: usize,
    pub violations: usize,
}

pub(crate) fn listed_tests(output: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(output)
        .lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .map(str::to_owned)
        .collect()
}

pub(crate) fn exact_pass(output: &[u8], test_name: &str) -> bool {
    let output = String::from_utf8_lossy(output);
    let lines = output.lines().map(str::trim).collect::<Vec<_>>();
    count_exact_line(&output, "running 1 test") == 1
        && count_exact_line(&output, &format!("test {test_name} ... ok")) == 1
        && count_summary(&output, "test result: ok. 1 passed; 0 failed; 0 ignored") == 1
        && lines
            .iter()
            .filter(|line| line.starts_with("running ") && line.ends_with(" test"))
            .count()
            == 1
        && lines
            .iter()
            .filter(|line| line.starts_with("running ") && line.ends_with(" tests"))
            .count()
            == 0
        && lines
            .iter()
            .filter(|line| line.starts_with("test ") && line.contains(" ... "))
            .count()
            == 1
        && lines
            .iter()
            .filter(|line| line.starts_with("test result:"))
            .count()
            == 1
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

pub(crate) fn exact_zero_execution(output: &[u8]) -> bool {
    let output = String::from_utf8_lossy(output);
    count_exact_line(&output, "running 0 tests") == 1
        && count_summary(&output, "test result: ok. 0 passed; 0 failed; 0 ignored") == 1
        && !output
            .lines()
            .any(|line| line.trim_start().starts_with("test ") && line.contains(" ... "))
}

pub(crate) fn oracle_token(source_ref: &str, check_id: &str) -> String {
    let value = format!("{source_ref}\0{check_id}");
    let digest = format!("{:x}", Sha256::digest(format!("oracle\0{value}")));
    format!("oracle-{}", &digest[..16])
}

pub(crate) fn oracle_markers(
    stdout: &[u8],
    stderr: &[u8],
    token: &str,
) -> Option<OracleMarkerCounts> {
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
    (observed_count == all_observed && violation_count == all_violations).then_some(
        OracleMarkerCounts {
            observed: observed_count,
            violations: violation_count,
        },
    )
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
