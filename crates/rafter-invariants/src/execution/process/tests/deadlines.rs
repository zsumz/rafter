//! Absolute launch, observation, target, and cleanup deadline scenarios.

use std::{
    path::Path,
    time::{Duration, Instant},
};

use super::{
    super::{
        base_environment, clear_signal_attempts, confirm_process_group_absent_with,
        delay_next_process_group_observation, delay_next_process_group_receipt,
        delay_next_target_release, hold_next_poll_until_the_execution_window_closes,
        omit_anchor_from_next_process_group_observation,
        omit_target_rows_from_process_group_observations, process_group_state,
        take_last_delayed_process_group, take_last_unreleased_process_group, take_signal_attempts,
        FinalizationPolicy, ProcessDeadlines, ProcessGroupState, ProcessSignal,
    },
    support::{measured_launch_cost, run_shell, run_shell_with_deadlines, unique_test_path},
};

/// How much launcher startup a fixture allows before its own deadlines bite.
///
/// The measurement is what makes this safe on a loaded machine; the multiplier
/// covers the spread between two consecutive launches on that same machine.
fn managed_process_startup_allowance() -> Duration {
    measured_launch_cost() * 4
}

#[test]
fn natural_exit_after_the_deadline_cannot_be_reported_as_success() {
    // The observation must not happen until the target has exited on its own,
    // or the harness sees a live target and signals it. The stall is derived
    // from a measured launch so it still outlasts the target on a machine where
    // starting a shell costs more than the stall a fast one needed.
    delay_next_process_group_observation(managed_process_startup_allowance());
    let output = run_shell(
        "sleep 0.05; exit 0",
        &base_environment(),
        Path::new("."),
        Duration::from_millis(10),
        Duration::from_millis(20),
    )
    .expect("deadline race produces a classified process result");

    assert!(output.status.success());
    assert!(output.timed_out);
    assert!(!output.termination.term_signal_sent);
    assert!(!output.termination.kill_signal_sent);
}

#[test]
fn delayed_process_group_publication_cannot_escape_the_lifecycle_deadline() {
    // The injected receipt delay has to outlast the lifecycle it must not
    // escape, and the lifecycle has to outlast launcher startup. Deriving both
    // from a launch measured on this machine keeps that ordering on a host
    // where starting Perl costs far more than the constants a fast machine
    // suggested.
    let startup = managed_process_startup_allowance();
    let publication_delay = startup * 8;
    delay_next_process_group_receipt(publication_delay);
    let started = Instant::now();
    let execution_window = started + startup;
    let lifecycle = started + startup * 2;
    let error = run_shell_with_deadlines(
        "sleep 5",
        &base_environment(),
        Path::new("."),
        ProcessDeadlines::new(
            Duration::from_secs(1),
            execution_window,
            lifecycle
                .checked_sub(startup / 4)
                .expect("cleanup boundary"),
            lifecycle
                .checked_sub(startup / 8)
                .expect("finalization boundary"),
            lifecycle,
        )
        .expect("valid delayed-publication deadlines"),
        Duration::from_millis(20),
        FinalizationPolicy::bounded(Duration::from_millis(20)),
    )
    .expect_err("unpublished process group must fail closed");
    assert!(error
        .to_string()
        .contains("did not publish its process group"));
    assert!(
        started.elapsed() < publication_delay,
        "the run outlasted the publication stall it was supposed to cut off"
    );
    let process_group = take_last_delayed_process_group()
        .expect("the delayed launcher published its planned process group");
    assert_eq!(
        process_group_state(process_group).expect("probe delayed target process group"),
        ProcessGroupState::Absent,
        "failed publication must not orphan the target process group"
    );
}

#[test]
fn target_cannot_execute_before_the_parent_releases_ready_ownership() {
    clear_signal_attempts();
    let sentinel = unique_test_path("target-release-sentinel");
    std::fs::create_dir_all(sentinel.parent().expect("sentinel parent"))
        .expect("create sentinel parent");
    let _ = std::fs::remove_file(&sentinel);
    let startup = managed_process_startup_allowance();
    let release_delay = startup * 8;
    delay_next_target_release(release_delay);
    let started = Instant::now();
    let execution_window = started + startup;
    let lifecycle = started + startup * 2;
    let script = format!("printf executed > '{}'", sentinel.display());
    let error = run_shell_with_deadlines(
        &script,
        &base_environment(),
        Path::new("."),
        ProcessDeadlines::new(
            Duration::from_secs(1),
            execution_window,
            lifecycle
                .checked_sub(startup / 4)
                .expect("cleanup boundary"),
            lifecycle
                .checked_sub(startup / 8)
                .expect("finalization boundary"),
            lifecycle,
        )
        .expect("valid target-release deadlines"),
        Duration::from_millis(20),
        FinalizationPolicy::bounded(Duration::from_millis(20)),
    )
    .expect_err("an unreleased target must fail closed");
    assert!(error
        .to_string()
        .contains("target execution release deadline expired"));
    assert!(!sentinel.exists(), "the target executed before release");
    let process_group = take_last_unreleased_process_group()
        .expect("the parent retained the unreleased target identity");
    let attempts = take_signal_attempts();
    assert_eq!(
        attempts.len(),
        2,
        "transition cleanup signals: {attempts:?}"
    );
    assert!(attempts
        .iter()
        .all(|(_, signal)| *signal == ProcessSignal::Kill));
    let signaled_groups = attempts
        .iter()
        .map(|(process_group, _)| *process_group)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(signaled_groups.len(), 2);
    assert!(signaled_groups.contains(&process_group));
    // A liveness safety net, not a timing assertion: the reaper will remove the
    // group, and this only bounds how long the test is willing to wait for it.
    confirm_process_group_absent_with(release_delay, || process_group_state(process_group))
        .expect("the no-signal reaper eventually removes the unreleased target process group");
}

#[test]
fn observer_omission_of_a_live_anchor_is_a_harness_error() {
    omit_anchor_from_next_process_group_observation();
    let error = run_shell(
        "sleep 1",
        &base_environment(),
        Path::new("."),
        Duration::from_millis(200),
        Duration::from_millis(20),
    )
    .expect_err("a missing live anchor row must fail closed");
    assert!(error
        .to_string()
        .contains("process observer omitted live group anchor"));
}

#[test]
fn observer_omission_of_live_target_members_is_a_harness_error() {
    // The omission can only be diagnosed on an observation taken after the
    // resource wrapper has exited, because that is what makes a still-held
    // lifetime lease contradict an empty inventory. Keeping the omission armed
    // for every observation puts it on the first one that can decide, instead
    // of guessing with a sleep how long the wrapper needs to get there.
    let _omission = omit_target_rows_from_process_group_observations();
    let error = run_shell(
        "(trap '' TERM; sleep 5) & exit 0",
        &base_environment(),
        Path::new("."),
        Duration::from_secs(1),
        Duration::from_millis(20),
    )
    .expect_err("a live descendant omitted from process inventory must fail closed");
    assert!(error
        .to_string()
        .contains("process observer omitted live target members"));
}

#[test]
fn stalled_process_observer_cannot_escape_the_lifecycle_deadline() {
    // The observation window has to outlast launcher startup, or the run fails
    // in an earlier phase and never reaches the stalled observer at all. The
    // injected stall then has to outlast that window, so that being cut off is
    // distinguishable from the stall simply finishing.
    let startup = managed_process_startup_allowance();
    let observer_stall = startup * 8;
    delay_next_process_group_observation(observer_stall);
    let started = Instant::now();
    let execution_window = started + startup;
    let lifecycle = started + startup * 2;
    let error = run_shell_with_deadlines(
        "sleep 30",
        &base_environment(),
        Path::new("."),
        ProcessDeadlines::new(
            Duration::from_secs(1),
            execution_window,
            lifecycle
                .checked_sub(startup / 4)
                .expect("cleanup boundary"),
            lifecycle
                .checked_sub(startup / 8)
                .expect("finalization boundary"),
            lifecycle,
        )
        .expect("valid stalled-observer deadlines"),
        Duration::from_millis(20),
        FinalizationPolicy::bounded(Duration::from_millis(20)),
    )
    .expect_err("stalled observer must fail closed");
    assert!(
        error
            .to_string()
            .contains("observer exhausted its absolute deadline"),
        "unexpected stalled-observer classification: {error}"
    );
    assert!(
        started.elapsed() < observer_stall,
        "the run outlasted the observer stall it was supposed to cut off"
    );
}

#[test]
fn a_window_edge_reached_between_observations_is_a_timeout() {
    // A window that closes while the collection loop is between two
    // observations is that loop reaching its own timeout, and the run has to
    // report the timeout it reached. Entering the observer first turns it into
    // an exhausted-deadline stall instead -- correct for a stall, fatal for a
    // run that had simply run out of time -- and the poll is where that edge
    // lands, being a hundred milliseconds against an observation of single-digit
    // ones. The target outlives every deadline here so the loop can only leave
    // by timing out.
    let startup = managed_process_startup_allowance();
    let finalization_reserve = startup * 2;
    let started = Instant::now();
    let execution_window = started + startup;
    let cleanup_start = started + startup * 3;
    let finalization_start = started + startup * 5;
    let lifecycle = finalization_start + finalization_reserve;
    hold_next_poll_until_the_execution_window_closes();
    let output = run_shell_with_deadlines(
        "sleep 30",
        &base_environment(),
        Path::new("."),
        ProcessDeadlines::new(
            // Long enough that the window, not the target timeout, is the
            // deadline the loop reaches -- which is the configuration a loaded
            // runner produces on its own once startup has eaten the difference.
            Duration::from_secs(30),
            execution_window,
            cleanup_start,
            finalization_start,
            lifecycle,
        )
        .expect("valid window-edge deadlines"),
        finalization_reserve,
        FinalizationPolicy::bounded(finalization_reserve),
    )
    .expect("a window edge reached between observations must not fail the run");
    assert!(
        output.timed_out,
        "a run that reached its execution window must report the timeout it reached"
    );
}

#[test]
fn late_termination_preserves_the_receipt_finalization_boundary() {
    // Termination must stop at the cleanup boundary and leave the whole
    // finalization reserve untouched. Both the reserve and the phases ahead of
    // it are sized from a measured launch, so the check states "the reserve
    // survived" rather than "the run fitted inside a fixed budget".
    let startup = managed_process_startup_allowance();
    let finalization_reserve = startup * 2;
    let started = Instant::now();
    let execution_window = started + startup;
    let cleanup_start = started + startup * 2;
    let finalization_start = started + startup * 4;
    let lifecycle = finalization_start + finalization_reserve;
    let _result = run_shell_with_deadlines(
        "trap '' TERM; while :; do :; done",
        &base_environment(),
        Path::new("."),
        ProcessDeadlines::new(
            Duration::from_millis(20),
            execution_window,
            cleanup_start,
            finalization_start,
            lifecycle,
        )
        .expect("valid finalization reserve"),
        finalization_reserve,
        FinalizationPolicy::bounded(finalization_reserve),
    );
    assert!(
        lifecycle.saturating_duration_since(Instant::now()) > finalization_reserve,
        "termination consumed the reserved receipt-finalization phase"
    );
}
