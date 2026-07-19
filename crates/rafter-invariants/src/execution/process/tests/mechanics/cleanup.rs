//! Managed-process cleanup, diagnostics, and telemetry allocation scenarios.

use std::{
    path::Path,
    sync::atomic::AtomicU64,
    time::{Duration, Instant},
};

#[cfg(unix)]
use super::super::super::model::DEFAULT_KILL_CONFIRMATION_TIMEOUT;
use super::super::super::{
    allocate_process_artifacts_at, cleanup_error, force_next_cleanup_target_alive, CleanupFailures,
    ProcessGroupState,
};
#[cfg(unix)]
use super::super::super::{
    clear_signal_attempts, process_group_state, take_signal_attempts, ProcessSignal,
};
use super::super::support::{managed_process_fixture, process_observer, unique_test_path};

#[cfg(unix)]
#[test]
fn managed_process_drop_kills_and_reaps_an_armed_group() {
    let cleanup_failures = CleanupFailures::default();
    let (process, wrapper_group, target_group, _reaper, _deadline) = managed_process_fixture(
        "trap '' TERM; while :; do :; done",
        DEFAULT_KILL_CONFIRMATION_TIMEOUT,
        DEFAULT_KILL_CONFIRMATION_TIMEOUT,
        cleanup_failures.clone(),
        Some(process_observer()),
    );
    drop(process);

    assert_eq!(
        process_group_state(wrapper_group).expect("probe cleaned wrapper group"),
        ProcessGroupState::Absent
    );
    assert_eq!(
        process_group_state(target_group).expect("probe cleaned target group"),
        ProcessGroupState::Absent
    );
    assert!(cleanup_failures.take().is_empty());
}

#[cfg(unix)]
#[test]
fn managed_process_wrapper_wait_respects_its_deadline() {
    let cleanup_failures = CleanupFailures::default();
    let (mut process, wrapper_group, target_group, _reaper, _deadline) = managed_process_fixture(
        "sleep 5",
        DEFAULT_KILL_CONFIRMATION_TIMEOUT,
        DEFAULT_KILL_CONFIRMATION_TIMEOUT,
        cleanup_failures.clone(),
        Some(process_observer()),
    );

    let started = Instant::now();
    assert!(process
        .wait_until(Instant::now())
        .expect("probe wrapper")
        .is_none());
    assert!(started.elapsed() < Duration::from_millis(100));
    drop(process);

    assert_eq!(
        process_group_state(wrapper_group).expect("probe cleaned wrapper group"),
        ProcessGroupState::Absent
    );
    assert_eq!(
        process_group_state(target_group).expect("probe cleaned target group"),
        ProcessGroupState::Absent
    );
    assert!(cleanup_failures.take().is_empty());
}

#[cfg(unix)]
#[test]
fn late_cleanup_error_cannot_refresh_the_absolute_deadline() {
    clear_signal_attempts();
    let cleanup_failures = CleanupFailures::default();
    let (mut process, _wrapper, target, reaper, _deadline) = managed_process_fixture(
        "sleep 0.2",
        Duration::from_millis(80),
        DEFAULT_KILL_CONFIRMATION_TIMEOUT,
        cleanup_failures.clone(),
        Some(process_observer()),
    );
    process
        .set_target_group(target)
        .expect("place fixture target in anchor group");
    force_next_cleanup_target_alive();
    std::thread::sleep(Duration::from_millis(60));

    let drop_started = Instant::now();
    drop(process);
    assert!(
        drop_started.elapsed() < Duration::from_millis(50),
        "cleanup refreshed an already-running absolute deadline"
    );
    let failures = cleanup_failures.take();
    assert!(failures
        .iter()
        .any(|failure| failure.contains("remained owned after emergency cleanup")));
    assert_eq!(reaper.snapshot().adopted, 2);
    assert_eq!(
        reaper.snapshot().reaped,
        0,
        "the anchor cannot be reaped while the wrapper still holds the target lifetime lease"
    );
    assert_eq!(take_signal_attempts(), vec![(target, ProcessSignal::Kill)]);
    let reaping_deadline = Instant::now() + Duration::from_secs(2);
    while reaper.snapshot().reaped < 2 && Instant::now() < reaping_deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(reaper.snapshot().reaped, 2);
    assert!(reaper.snapshot().failures.is_empty());
    assert!(take_signal_attempts().is_empty());
}

#[cfg(unix)]
#[test]
fn failed_cleanup_is_monotonic_and_never_retried() {
    let cleanup_failures = CleanupFailures::default();
    let (mut process, _wrapper, target, reaper, deadline) = managed_process_fixture(
        "exit 0",
        Duration::from_millis(20),
        DEFAULT_KILL_CONFIRMATION_TIMEOUT,
        cleanup_failures.clone(),
        Some(process_observer()),
    );
    process
        .set_target_group(target)
        .expect("place fixture target in anchor group");
    force_next_cleanup_target_alive();

    let first = process
        .cleanup_until(Instant::now(), deadline)
        .expect_err("forced live target ownership must fail cleanup");
    assert!(first.contains("remained owned after emergency cleanup"));
    let second = process
        .cleanup_until(Instant::now(), deadline)
        .expect_err("failed cleanup cannot be retried");
    assert!(second.contains("already failed and will not be retried"));
    drop(process);
    assert!(cleanup_failures.take().is_empty());
    assert_eq!(reaper.snapshot().adopted, 1);
}

#[cfg(unix)]
#[test]
fn cleanup_confirmation_has_its_own_deadline_and_never_resignals() {
    let cleanup_failures = CleanupFailures::default();
    let (mut process, _wrapper, target, reaper, lifecycle_deadline) = managed_process_fixture(
        "exit 0",
        Duration::from_secs(2),
        Duration::from_millis(20),
        cleanup_failures.clone(),
        Some(process_observer()),
    );
    process
        .set_target_group(target)
        .expect("place fixture target in anchor group");
    process.record_target_kill_for_test();
    force_next_cleanup_target_alive();

    let started = Instant::now();
    let error = process
        .cleanup_until(Instant::now(), lifecycle_deadline)
        .expect_err("forced live ownership must expire");
    assert!(started.elapsed() < Duration::from_millis(200));
    assert!(error.contains("remained owned after emergency cleanup"));
    assert!(!error.contains("process group ID exceeds i32"));
    drop(process);
    assert!(cleanup_failures.take().is_empty());
    assert_eq!(reaper.snapshot().adopted, 1);
}

#[test]
fn cleanup_errors_name_retained_telemetry() {
    let error = cleanup_error(
        "permission denied",
        Path::new("target/telemetry.stdout"),
        Path::new("target/telemetry.stderr"),
    );
    let message = error.to_string();
    assert!(message.contains("permission denied"));
    assert!(message.contains("target/telemetry.stdout"));
    assert!(message.contains("target/telemetry.stderr"));
}

#[test]
fn telemetry_allocation_never_reuses_stale_process_receipts() {
    let directory = unique_test_path("telemetry-collision");
    std::fs::create_dir_all(&directory).expect("create telemetry scratch directory");
    std::fs::write(directory.join("42-0.stdout"), b"stale").expect("retain stale stdout receipt");

    let reused_process_sequence = AtomicU64::new(0);
    let artifacts = allocate_process_artifacts_at(&directory, 42, &reused_process_sequence)
        .expect("skip stale telemetry path");
    let root = std::env::current_dir().expect("resolve workspace");
    assert_eq!(
        artifacts.resource_path(),
        root.join(&directory).join("42-1.time")
    );
    for path in [
        artifacts.stdout_path(),
        artifacts.stderr_path(),
        artifacts.resource_path(),
        artifacts.process_group_path(),
        artifacts.reservation_path(),
    ] {
        assert!(
            path.is_file(),
            "artifact was not retained: {}",
            path.display()
        );
    }

    drop(artifacts);
    std::fs::remove_dir_all(directory).expect("remove telemetry scratch directory");
}
