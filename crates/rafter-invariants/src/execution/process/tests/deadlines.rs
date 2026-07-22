//! Absolute launch, observation, target, and cleanup deadline scenarios.

use std::{
    path::Path,
    time::{Duration, Instant},
};

use super::{
    super::{
        base_environment, clear_signal_attempts, confirm_process_group_absent_with,
        delay_next_process_group_observation, delay_next_process_group_receipt,
        delay_next_target_release, omit_anchor_from_next_process_group_observation,
        omit_target_rows_from_next_process_group_observation, process_group_state,
        take_last_delayed_process_group, take_last_unreleased_process_group, take_signal_attempts,
        FinalizationPolicy, ProcessDeadlines, ProcessGroupState, ProcessSignal,
    },
    support::{run_shell, run_shell_with_deadlines, unique_test_path},
};

#[test]
fn natural_exit_after_the_deadline_cannot_be_reported_as_success() {
    delay_next_process_group_observation(Duration::from_millis(500));
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
    delay_next_process_group_receipt(Duration::from_secs(5));
    let started = Instant::now();
    // The first managed process on a cold macOS runner may need more than
    // 100 ms to start Perl and publish its planned group. Keep the injected
    // five-second receipt delay beyond this absolute lifecycle while giving
    // launcher startup a deterministic cross-platform allowance.
    let execution_window = started + Duration::from_secs(2);
    let lifecycle = started + Duration::from_millis(2_500);
    let error = run_shell_with_deadlines(
        "sleep 5",
        &base_environment(),
        Path::new("."),
        ProcessDeadlines::new(
            Duration::from_secs(1),
            execution_window,
            lifecycle
                .checked_sub(Duration::from_millis(300))
                .expect("cleanup boundary"),
            lifecycle
                .checked_sub(Duration::from_millis(100))
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
    assert!(started.elapsed() < Duration::from_secs(4));
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
    delay_next_target_release(Duration::from_secs(5));
    let started = Instant::now();
    let execution_window = started + Duration::from_millis(100);
    let lifecycle = started + Duration::from_millis(400);
    let script = format!("printf executed > '{}'", sentinel.display());
    let error = run_shell_with_deadlines(
        &script,
        &base_environment(),
        Path::new("."),
        ProcessDeadlines::new(
            Duration::from_secs(1),
            execution_window,
            lifecycle
                .checked_sub(Duration::from_millis(70))
                .expect("cleanup boundary"),
            lifecycle
                .checked_sub(Duration::from_millis(20))
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
    confirm_process_group_absent_with(Duration::from_secs(2), || {
        process_group_state(process_group)
    })
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
    delay_next_process_group_observation(Duration::from_millis(100));
    omit_target_rows_from_next_process_group_observation();
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
    delay_next_process_group_observation(Duration::from_secs(5));
    let started = Instant::now();
    let execution_window = started + Duration::from_millis(250);
    let lifecycle = started + Duration::from_millis(500);
    let error = run_shell_with_deadlines(
        "sleep 5",
        &base_environment(),
        Path::new("."),
        ProcessDeadlines::new(
            Duration::from_secs(1),
            execution_window,
            lifecycle
                .checked_sub(Duration::from_millis(70))
                .expect("cleanup boundary"),
            lifecycle
                .checked_sub(Duration::from_millis(20))
                .expect("finalization boundary"),
            lifecycle,
        )
        .expect("valid stalled-observer deadlines"),
        Duration::from_millis(20),
        FinalizationPolicy::bounded(Duration::from_millis(20)),
    )
    .expect_err("stalled observer must fail closed");
    assert!(error
        .to_string()
        .contains("observer exhausted its absolute deadline"));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn late_termination_preserves_the_receipt_finalization_boundary() {
    let started = Instant::now();
    let execution_window = started + Duration::from_millis(100);
    let cleanup_start = started + Duration::from_millis(200);
    let finalization_start = started + Duration::from_millis(300);
    let lifecycle = started + Duration::from_secs(2);
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
        Duration::from_secs(1),
        FinalizationPolicy::bounded(Duration::from_secs(1)),
    );
    assert!(
        lifecycle.saturating_duration_since(Instant::now()) > Duration::from_secs(1),
        "termination consumed the reserved receipt-finalization phase"
    );
}
