//! Scenarios: one stalled observation costs a retry, not the run.

use std::{path::Path, time::Duration};

use super::{
    super::{
        base_environment, delay_next_process_group_observation,
        fail_next_process_group_observation_command, inject_next_internal_drain_error,
        inject_next_internal_drain_errors,
    },
    support::run_shell,
};

/// A transient observation stall costs one retry, not the run. The simulator's
/// evidence is seed-deterministic, so a host too slow to be watched has not
/// changed what the run proves.
#[test]
fn a_transient_observation_stall_is_retried_once_and_the_run_survives() {
    inject_next_internal_drain_error();

    let output = run_shell(
        "printf ready; exit 0",
        &base_environment(),
        Path::new("."),
        Duration::from_secs(20),
        Duration::from_millis(200),
    )
    .expect("one stalled observation must not destroy the run");
    assert!(!output.timed_out);
    assert!(output.status.success());
}

/// An observer that ran and exited non-zero costs the same one retry as one
/// that never answered. Both are the observer *command* failing, and on the
/// starved host the retry exists for, a `ps` that loses a fork to memory
/// pressure is at least as likely as a `ps` that hangs.
#[test]
fn a_failed_observer_command_is_retried_like_a_stalled_one() {
    fail_next_process_group_observation_command();

    let output = run_shell(
        "printf ready; exit 0",
        &base_environment(),
        Path::new("."),
        Duration::from_secs(20),
        Duration::from_millis(200),
    )
    .expect("one failed observer command must not destroy the run");
    assert!(!output.timed_out);
    assert!(output.status.success());
}

/// A second failure is as fatal as a first one always was: persistent
/// inability to watch the process tree is exactly what failing closed is for.
#[test]
fn a_persistent_observation_failure_is_still_fatal() {
    inject_next_internal_drain_errors(2);

    let error = run_shell(
        "printf ready; exit 0",
        &base_environment(),
        Path::new("."),
        Duration::from_secs(20),
        Duration::from_millis(200),
    )
    .expect_err("a repeated observer failure must fail closed");
    assert!(
        error
            .to_string()
            .contains("injected internal drain failure"),
        "unexpected persistent-failure classification: {error}"
    );
}

/// A stall that consumes the execution window gets no retry: one the window
/// cannot fit would be cut off for the same reason, and an observer that spent
/// a whole window stays fail-closed rather than being quietly reclassified.
/// `stalled_process_observer_cannot_escape_the_lifecycle_deadline` holds the
/// other end of that promise.
#[test]
fn a_stall_that_exhausts_the_window_is_not_retried() {
    delay_next_process_group_observation(Duration::from_secs(30));

    let error = run_shell(
        "sleep 30",
        &base_environment(),
        Path::new("."),
        Duration::from_millis(50),
        Duration::from_millis(200),
    )
    .expect_err("an observer that consumed its window must not be retried into success");
    assert!(
        error
            .to_string()
            .contains("observer exhausted its absolute deadline"),
        "unexpected exhausted-window classification: {error}"
    );
}

/// The retry belongs to the execution-window loop alone. Termination and grace
/// keep one attempt, because there the grace clock is the authority over how
/// long anything may take and a retry would spend a window it does not own.
#[test]
fn only_the_execution_window_loop_retries_an_observation() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/execution/process");
    let read = |name: &str| std::fs::read_to_string(root.join(name)).expect("read process source");

    assert!(
        read("output.rs").contains("fn observe_within_execution_window"),
        "the execution-window loop owns the retry"
    );
    for single_attempt in ["termination.rs", "reaping.rs"] {
        assert!(
            !read(single_attempt).contains("observe_within_execution_window"),
            "{single_attempt} must keep its single observation attempt"
        );
    }
}
