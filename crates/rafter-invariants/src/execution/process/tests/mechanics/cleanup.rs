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

/// Bound on waits for facts that are going to happen, such as a `sh -c 'exit 0'`
/// wrapper finishing. Reaching one of these means the machine is broken, not
/// that the property under test failed, so it is sized to be unreachable rather
/// than tight.
#[cfg(unix)]
const FIXTURE_LIVENESS_TIMEOUT: Duration = Duration::from_secs(30);

/// The deadline cleanup confirmation must *not* bound itself by.
///
/// Cleanup that ignored its own confirmation window would poll until this
/// instant, so "returned before it" is exactly the property and needs no
/// separate tolerance. Widening it makes the test stricter, never greener.
#[cfg(unix)]
const CLEANUP_LIFECYCLE_WINDOW: Duration = Duration::from_secs(10);

/// How long cleanup polls a target that has been forced to stay owned.
///
/// This window is meant to expire; the target never quiesces. A machine slow
/// enough to blow past it before cleanup even signals takes the "no budget
/// left" branch instead, which records the same ownership failure and
/// quarantines the same anchor — so neither outcome changes what the callers
/// below assert.
#[cfg(unix)]
const CLEANUP_CONFIRMATION_WINDOW: Duration = Duration::from_millis(20);

/// How long a fixture wrapper keeps running when the test needs it unfinished.
///
/// Assertions about "this call did not wait for the wrapper" bound themselves
/// by this rather than by a stopwatch reading, so a stalled machine cannot look
/// like a wait that blocked.
#[cfg(unix)]
const UNFINISHED_WRAPPER_LIFETIME: Duration = Duration::from_secs(30);

/// The absolute cleanup window that a drop must honour rather than restart.
///
/// It has to be short enough that the budget left at drop is smaller than a
/// fresh confirmation timeout — otherwise "restarted" and "honoured" look the
/// same — and long enough that it has not already expired when the drop runs.
#[cfg(unix)]
const LATE_CLEANUP_WINDOW: Duration = Duration::from_secs(2);

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
        &format!("sleep {}", UNFINISHED_WRAPPER_LIFETIME.as_secs()),
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
    // A wait that ignored its expired deadline would block until the wrapper
    // finished, so the wrapper's own lifetime is the bound that distinguishes
    // the two — unlike a stopwatch budget, which a stalled machine can blow
    // without the wait ever having blocked.
    assert!(started.elapsed() < UNFINISHED_WRAPPER_LIFETIME);
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
    // The wrapper must still be running when cleanup gives up and still be able
    // to exit afterwards. A sentinel makes both facts things the test decides,
    // where a `sleep` made them things the test hoped the clock would arrange.
    let sentinel = unique_test_path("late-cleanup-wrapper-release");
    let _ = std::fs::remove_file(&sentinel);
    let (mut process, _wrapper, target, reaper, _deadline) = managed_process_fixture(
        &format!(
            "while [ ! -e '{}' ]; do sleep 0.05; done",
            sentinel.display()
        ),
        LATE_CLEANUP_WINDOW,
        FIXTURE_LIVENESS_TIMEOUT,
        cleanup_failures.clone(),
        Some(process_observer()),
    );
    process
        .set_target_group(target)
        .expect("place fixture target in anchor group");
    force_next_cleanup_target_alive();

    let drop_started = Instant::now();
    drop(process);
    // Cleanup that restarted its window would have polled for a whole fresh
    // confirmation timeout instead of stopping at the deadline that was already
    // running. The bound is that refreshed window itself, so the check keeps its
    // meaning however long the machine stalls in between.
    assert!(
        drop_started.elapsed() < FIXTURE_LIVENESS_TIMEOUT,
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

    std::fs::create_dir_all(sentinel.parent().expect("sentinel parent"))
        .expect("create sentinel parent");
    std::fs::write(&sentinel, b"release").expect("release the quarantined wrapper");
    let reaping_deadline = Instant::now() + FIXTURE_LIVENESS_TIMEOUT;
    while reaper.snapshot().reaped < 2 && Instant::now() < reaping_deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(reaper.snapshot().reaped, 2);
    assert!(reaper.snapshot().failures.is_empty());
    assert!(take_signal_attempts().is_empty());
    std::fs::remove_file(&sentinel).expect("remove wrapper release sentinel");
}

#[cfg(unix)]
#[test]
fn failed_cleanup_is_monotonic_and_never_retried() {
    let cleanup_failures = CleanupFailures::default();
    let (mut process, _wrapper, target, reaper, _deadline) = managed_process_fixture(
        "exit 0",
        FIXTURE_LIVENESS_TIMEOUT,
        CLEANUP_CONFIRMATION_WINDOW,
        cleanup_failures.clone(),
        Some(process_observer()),
    );
    process
        .set_target_group(target)
        .expect("place fixture target in anchor group");
    // Only the forced-live anchor may reach the reaper. Waiting for the
    // wrapper's observed exit — rather than assuming a short script beats the
    // cleanup deadline — is what makes the adoption count below a fact.
    process
        .wait_until(Instant::now() + FIXTURE_LIVENESS_TIMEOUT)
        .expect("await fixture wrapper exit")
        .expect("the fixture wrapper script exits on its own");
    force_next_cleanup_target_alive();

    // Read the boundary before the deadline so no scheduling delay between the
    // two can order them the wrong way round.
    let cleanup_start = Instant::now();
    let deadline = Instant::now() + FIXTURE_LIVENESS_TIMEOUT;
    let first = process
        .cleanup_until(cleanup_start, deadline)
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
    let (mut process, _wrapper, target, reaper, _deadline) = managed_process_fixture(
        "exit 0",
        FIXTURE_LIVENESS_TIMEOUT,
        CLEANUP_CONFIRMATION_WINDOW,
        cleanup_failures.clone(),
        Some(process_observer()),
    );
    process
        .set_target_group(target)
        .expect("place fixture target in anchor group");
    // Only the forced-live anchor may reach the reaper, so establish the
    // wrapper's exit rather than assuming the cleanup deadline outran it.
    process
        .wait_until(Instant::now() + FIXTURE_LIVENESS_TIMEOUT)
        .expect("await fixture wrapper exit")
        .expect("the fixture wrapper script exits on its own");
    process.record_target_kill_for_test();
    force_next_cleanup_target_alive();

    let cleanup_start = Instant::now();
    let lifecycle_deadline = Instant::now() + CLEANUP_LIFECYCLE_WINDOW;
    let error = process
        .cleanup_until(cleanup_start, lifecycle_deadline)
        .expect_err("forced live ownership must expire");
    // Cleanup that ignored its own confirmation window would have polled the
    // forced-live target right up to the lifecycle deadline. Returning before
    // that instant is the property, and it stays true however slow the machine
    // is, unlike a fixed elapsed-time budget.
    assert!(
        Instant::now() < lifecycle_deadline,
        "cleanup confirmation ran to the lifecycle deadline instead of its own"
    );
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
