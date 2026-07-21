//! Adversarial typed replay-process validation scenarios.

use super::*;
use crate::verification::detector_replay::artifact::model::{
    ProcessExitReport, ProcessResourceReport, ProcessTerminationReport,
};

#[test]
fn completed_process_resources_and_termination_are_fail_closed() {
    assert!(validate(&completed(0, 30_000, false, false), true)
        .expect_err("missing RSS must fail")
        .contains("peak-RSS"));
    assert!(validate(&completed(1, 1, false, false), true)
        .expect_err("substituted grace must fail")
        .contains("termination grace"));
    assert!(validate(&completed(1, 30_000, true, false), true)
        .expect_err("successful termination signal must fail")
        .contains("termination signals"));

    let mut killed = completed(1, 30_000, false, true);
    let ProcessReport::Completed { exit, .. } = &mut killed else {
        unreachable!();
    };
    exit.success = false;
    exit.exit_code = None;
    assert!(validate(&killed, false)
        .expect_err("kill without termination must fail")
        .contains("not preceded"));
}

#[test]
fn completed_process_duration_is_bounded_by_its_phase() {
    let process = completed(1, 30_000, false, false);
    require_duration_at_most(&process, 1).expect("exact duration is allowed");
    assert!(require_duration_at_most(&process, 0)
        .expect_err("duration beyond the phase must fail")
        .contains("phase budget"));
}

fn completed(
    peak_rss_kib: u64,
    termination_grace_ms: u64,
    term_signal_sent: bool,
    kill_signal_sent: bool,
) -> ProcessReport {
    let artifact = |stream: &str, digit: char| ArtifactRef {
        kind: "verifier-replay-process-log".to_owned(),
        path: format!("target/verifier/{stream}-{}", digit.to_string().repeat(64)),
        sha256: digit.to_string().repeat(64),
        size_bytes: 1,
    };
    ProcessReport::Completed {
        role: "cargo-metadata".to_owned(),
        execution_id: "cargo-metadata".to_owned(),
        exit: ProcessExitReport {
            success: true,
            exit_code: Some(0),
            timed_out: false,
        },
        resources: ProcessResourceReport {
            duration_ms: 1,
            peak_rss_kib,
        },
        termination: ProcessTerminationReport {
            process_group: true,
            term_signal_sent,
            termination_grace_ms,
            kill_signal_sent,
        },
        logs: vec![artifact("stdout", '1'), artifact("stderr", '2')],
    }
}
