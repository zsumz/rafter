//! Process-group observation, signaling, and bounded observer scenarios.

use std::{
    path::Path,
    time::{Duration, Instant},
};

#[cfg(unix)]
use rustix::io::Errno;

#[cfg(unix)]
use super::super::super::model::DEFAULT_KILL_CONFIRMATION_TIMEOUT;
use super::super::super::{
    await_next_internal_completion_exit, bounded_internal_output,
    bounded_internal_output_with_reaper, confirm_process_group_absent_with,
    inject_next_internal_drain_error, parse_peak_rss, parse_process_group_observation,
    process_observer_path, NoSignalReaper, ProcessAnchorState, ProcessGroupObservation,
    ProcessGroupState, TargetLeaseState, TargetMemberState,
};
#[cfg(unix)]
use super::super::super::{
    before_next_wrapper_exit_observation, classify_process_group_probe, classify_signal_delivery,
    classify_target_quiescence_for_test, clear_signal_attempts, process_group_state,
    take_signal_attempts, CleanupFailures, SignalDelivery,
};
use super::super::support::unique_test_path;
#[cfg(unix)]
use super::super::support::{managed_process_fixture, process_observer};

/// How long a descendant would keep the observer waiting if the execution
/// deadline did not cut it off. Assertions bound themselves by this instead of
/// by a stopwatch reading, so "the deadline was enforced" stays distinguishable
/// from "the machine was slow".
const RUNAWAY_DESCENDANT_LIFETIME: Duration = Duration::from_secs(30);

#[test]
fn parses_platform_peak_rss() {
    let input = if cfg!(target_os = "macos") {
        b"  1048576  maximum resident set size\n".as_slice()
    } else {
        b"\tMaximum resident set size (kbytes): 1024\n".as_slice()
    };
    assert_eq!(parse_peak_rss(input), Some(1024));
}

#[test]
fn process_observer_is_absolute_and_platform_pinned() {
    let path = process_observer_path();
    assert!(path.is_absolute());
    if cfg!(target_os = "macos") {
        assert_eq!(path, Path::new("/bin/ps"));
    } else if cfg!(target_os = "linux") {
        assert_eq!(path, Path::new("/usr/bin/ps"));
    }
}

#[test]
fn internal_observer_kills_pipe_holding_descendants_within_its_deadline() {
    let started = Instant::now();
    let script = format!("(sleep {}) & exit 0", RUNAWAY_DESCENDANT_LIFETIME.as_secs());
    let error = bounded_internal_output("/bin/sh", &["-c", &script], Duration::from_millis(50))
        .expect_err("pipe-holding descendant must not outlive the observer deadline");
    assert!(
        error.to_string().contains("timed out"),
        "unexpected pipe-descendant classification: {error}"
    );
    assert!(started.elapsed() < RUNAWAY_DESCENDANT_LIFETIME);
}

#[test]
fn internal_observer_lifetime_lease_detects_a_silent_descendant() {
    let started = Instant::now();
    let script = format!(
        "(exec >/dev/null 2>/dev/null; sleep {}) & exit 0",
        RUNAWAY_DESCENDANT_LIFETIME.as_secs()
    );
    let error = bounded_internal_output("/bin/sh", &["-c", &script], Duration::from_millis(50))
        .expect_err("a silent descendant must retain the inherited lifetime lease");
    assert!(
        error.to_string().contains("timed out"),
        "unexpected silent-descendant classification: {error}"
    );
    assert!(started.elapsed() < RUNAWAY_DESCENDANT_LIFETIME);
}

#[test]
fn internal_observer_bounds_a_continuously_readable_pipe() {
    let started = Instant::now();
    let error = bounded_internal_output(
        "/bin/sh",
        &["-c", "while :; do printf 0123456789abcdef; done"],
        Duration::from_millis(50),
    )
    .expect_err("continuous output must not starve the observer deadline");
    assert!(
        error.to_string().contains("timed out") || error.to_string().contains("output limit"),
        "unexpected observer error: {error}"
    );
    assert!(started.elapsed() < RUNAWAY_DESCENDANT_LIFETIME);
}

#[cfg(unix)]
#[test]
fn internal_observer_rejects_a_clean_exit_classified_after_its_deadline() {
    clear_signal_attempts();
    // The scenario needs the child to have finished before the completion check
    // runs, so that a clean exit is what gets classified late. Holding the check
    // until the exit is observed states that; sleeping past a guess at how long
    // `/bin/sh` needs left a loaded machine killing the child mid-write and
    // asserting on output it never produced.
    await_next_internal_completion_exit();
    let error =
        bounded_internal_output("/bin/sh", &["-c", "printf late"], Duration::from_millis(10))
            .expect_err("late clean completion must retain output but classify as timed out");
    let message = error.to_string();
    assert!(
        message.contains("timed out"),
        "unexpected classification: {message}"
    );
    assert!(
        message.contains("stdout: late"),
        "output was not retained: {message}"
    );
    assert!(
        take_signal_attempts().is_empty(),
        "an already-complete child must be reaped without signaling"
    );
}

#[cfg(unix)]
#[test]
fn internal_observer_read_failure_retains_process_group_cleanup_ownership() {
    clear_signal_attempts();
    let process_group_path = unique_test_path("observer-process-group");
    let script = format!(
        "printf '%s' \"$$\" > '{}'; printf ready; sleep 5",
        process_group_path.display()
    );
    inject_next_internal_drain_error();
    let reaper = NoSignalReaper::start().expect("start observer cleanup reaper");

    let error = bounded_internal_output_with_reaper(
        "/bin/sh",
        &["-c", &script],
        Duration::from_secs(1),
        Duration::from_secs(1),
        reaper.clone(),
    )
    .expect_err("injected read failure must escape after cleanup");
    let message = error.to_string();
    assert!(message.contains("injected internal drain failure"));
    let process_group = std::fs::read_to_string(&process_group_path)
        .expect("launcher published its process group")
        .parse::<u32>()
        .expect("published process group is numeric");
    assert!(
        take_signal_attempts().contains(&(process_group, super::super::super::ProcessSignal::Kill))
    );
    if message.contains("transferred to no-signal reaper") {
        let snapshot = reaper.snapshot();
        assert!(
            snapshot.adopted_children.contains(&process_group),
            "fallback cleanup transferred a different child: {snapshot:?}"
        );
        assert!(snapshot.failures.is_empty());
    } else {
        assert_eq!(
            process_group_state(process_group).expect("probe observer process group"),
            ProcessGroupState::Absent
        );
    }
    std::fs::remove_file(process_group_path).expect("remove observer receipt");
}

#[test]
fn process_group_observation_combines_membership_and_rss() {
    assert_eq!(
        parse_process_group_observation(" 1 42 100 S\n 7 5 5 R+\n 2 42 23 D\n", 42, None)
            .expect("parse process inventory"),
        ProcessGroupObservation {
            target_members: TargetMemberState::Live,
            rss_kib: 123,
            anchor: None,
        }
    );
    assert_eq!(
        parse_process_group_observation(" 7 5 5 S\n", 42, None)
            .expect("parse absent process group"),
        ProcessGroupObservation {
            target_members: TargetMemberState::Quiescent,
            rss_kib: 0,
            anchor: None,
        }
    );
    assert_eq!(
        parse_process_group_observation("1 42 0 Z\n2 42 0 Z+\n", 42, None)
            .expect("zombies do not keep a process group alive"),
        ProcessGroupObservation {
            target_members: TargetMemberState::Quiescent,
            rss_kib: 0,
            anchor: None,
        }
    );
    assert_eq!(
        parse_process_group_observation("42 42 7 S\n43 42 11 R\n", 42, Some(42))
            .expect("separate anchor is excluded from target telemetry"),
        ProcessGroupObservation {
            target_members: TargetMemberState::Live,
            rss_kib: 11,
            anchor: Some(ProcessAnchorState::Alive),
        }
    );
    assert_eq!(
        parse_process_group_observation("43 42 11 R\n", 42, Some(42))
            .expect("record omitted anchor state"),
        ProcessGroupObservation {
            target_members: TargetMemberState::Live,
            rss_kib: 11,
            anchor: Some(ProcessAnchorState::Missing),
        }
    );
    assert!(parse_process_group_observation("42 42 100\n", 42, Some(42)).is_err());
    assert!(parse_process_group_observation("42 42 100 S extra\n", 42, Some(42)).is_err());
}

#[cfg(unix)]
#[test]
fn process_group_signal_distinguishes_absent_from_permission_denied() {
    assert_eq!(
        classify_signal_delivery(Err(Errno::SRCH)),
        Ok(SignalDelivery::GroupAbsent)
    );
    assert_eq!(classify_signal_delivery(Err(Errno::PERM)), Err(Errno::PERM));
    assert_eq!(classify_signal_delivery(Ok(())), Ok(SignalDelivery::Sent));
}

#[cfg(unix)]
#[test]
fn process_group_probe_treats_permission_denied_as_present() {
    assert_eq!(
        classify_process_group_probe(Err(Errno::PERM)),
        Ok(ProcessGroupState::Alive)
    );
    assert_eq!(
        classify_process_group_probe(Err(Errno::SRCH)),
        Ok(ProcessGroupState::Absent)
    );
    assert_eq!(
        classify_process_group_probe(Ok(())),
        Ok(ProcessGroupState::Alive)
    );
}

#[test]
fn group_absence_confirmation_is_fail_closed() {
    let error = confirm_process_group_absent_with(Duration::ZERO, || Ok(ProcessGroupState::Alive))
        .expect_err("a group that remains alive must fail confirmation");
    assert!(error.to_string().contains("absence was not observed"));
}

#[test]
fn target_quiescence_requires_bracketed_lease_and_inventory_agreement() {
    assert!(!classify_target_quiescence_for_test(
        TargetLeaseState::Held,
        TargetLeaseState::Released,
        true,
        TargetMemberState::Live,
    )
    .expect("lease release during observation is retried"));
    assert!(classify_target_quiescence_for_test(
        TargetLeaseState::Released,
        TargetLeaseState::Released,
        true,
        TargetMemberState::Quiescent,
    )
    .expect("released lease and empty inventory prove quiescence"));
    assert!(classify_target_quiescence_for_test(
        TargetLeaseState::Released,
        TargetLeaseState::Released,
        true,
        TargetMemberState::Live,
    )
    .is_err());
    assert!(classify_target_quiescence_for_test(
        TargetLeaseState::Held,
        TargetLeaseState::Held,
        true,
        TargetMemberState::Quiescent,
    )
    .is_err());
}

#[cfg(unix)]
#[test]
fn wrapper_exit_between_inventory_and_final_lease_probe_is_retried() {
    let cleanup_failures = CleanupFailures::default();
    let (mut process, wrapper, target, _reaper, lifecycle_deadline) = managed_process_fixture(
        "while :; do :; done",
        DEFAULT_KILL_CONFIRMATION_TIMEOUT,
        DEFAULT_KILL_CONFIRMATION_TIMEOUT,
        cleanup_failures.clone(),
        Some(process_observer()),
    );
    process
        .set_target_group(target)
        .expect("place fixture target in anchor group");
    before_next_wrapper_exit_observation(move || {
        let raw = i32::try_from(wrapper).expect("wrapper process ID fits i32");
        let pid = rustix::process::Pid::from_raw(raw).expect("wrapper process ID is positive");
        rustix::process::kill_process_group(pid, rustix::process::Signal::KILL)
            .expect("stop wrapper during the observation bracket");
        let deadline = Instant::now() + Duration::from_secs(2);
        while rustix::process::waitid(
            rustix::process::WaitId::Pid(pid),
            rustix::process::WaitIdOptions::EXITED
                | rustix::process::WaitIdOptions::NOHANG
                | rustix::process::WaitIdOptions::NOWAIT,
        )
        .expect("observe stopped wrapper")
        .is_none()
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            rustix::process::waitid(
                rustix::process::WaitId::Pid(pid),
                rustix::process::WaitIdOptions::EXITED
                    | rustix::process::WaitIdOptions::NOHANG
                    | rustix::process::WaitIdOptions::NOWAIT,
            )
            .expect("confirm stopped wrapper")
            .is_some(),
            "wrapper did not exit inside the observation bracket"
        );
    });

    let first = process
        .observe_target_members(lifecycle_deadline, lifecycle_deadline)
        .expect("wrapper exit inside the lease bracket is an ordinary race");
    assert!(
        first.into_quiescence().is_none(),
        "a lease transition inside one observation cannot prove quiescence"
    );
    let proof = process
        .observe_target_members(lifecycle_deadline, lifecycle_deadline)
        .expect("stable follow-up observation")
        .into_quiescence()
        .expect("stable released lease and empty inventory prove quiescence");
    process
        .release_target_anchor(proof, lifecycle_deadline)
        .expect("release target anchor after stable proof");
    assert!(process
        .wait_until(lifecycle_deadline)
        .expect("reap stopped wrapper")
        .is_some());
    process.disarm().expect("disarm fully reaped fixture");
    assert!(cleanup_failures.take().is_empty());
}
