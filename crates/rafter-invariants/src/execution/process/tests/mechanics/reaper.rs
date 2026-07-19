//! Direct-child identity and quarantine-worker scenarios.

use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use super::super::super::{
    clear_signal_attempts, fail_next_reaper_adoption, force_next_signal_group_absent,
    take_signal_attempts, CleanupFailures, DirectChild, ManagedInternalProcess, NoSignalReaper,
    ProcessLifetimeLease, ProcessSignal, SignalDelivery,
};

#[cfg(unix)]
#[test]
fn reaped_identity_cannot_signal_a_replacement_group() {
    let reaper = NoSignalReaper::start().expect("start replacement-identity reaper");
    let mut original_command = std::process::Command::new("sh");
    original_command.args(["-c", "exit 0"]).process_group(0);
    let original = original_command.spawn().expect("spawn original identity");
    let mut owned = DirectChild::new(original, reaper);
    assert!(owned
        .wait_until(Instant::now() + Duration::from_secs(2))
        .expect("reap original identity")
        .is_some());

    let mut replacement_command = std::process::Command::new("sh");
    replacement_command.args(["-c", "sleep 5"]).process_group(0);
    let mut replacement = replacement_command
        .spawn()
        .expect("spawn replacement identity");
    owned.replace_numeric_identity_for_test(replacement.id());
    clear_signal_attempts();

    assert_eq!(
        owned
            .signal_group(ProcessSignal::Kill)
            .expect("reaped ownership rejects signaling"),
        SignalDelivery::GroupAbsent
    );
    assert!(take_signal_attempts().is_empty());
    assert!(replacement.try_wait().expect("probe replacement").is_none());

    replacement.kill().expect("stop replacement fixture");
    while replacement
        .try_wait()
        .expect("reap replacement fixture")
        .is_none()
    {
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(unix)]
#[test]
fn leased_quarantine_reaps_by_handle_without_observing_replacement_identity() {
    let reaper = NoSignalReaper::start().expect("start leased replacement-identity reaper");
    let (lifetime, lifetime_writer) =
        ProcessLifetimeLease::create().expect("create leased-child lifetime");
    let mut original_command = std::process::Command::new("sh");
    original_command.args(["-c", "exit 0"]).process_group(0);
    let original = original_command.spawn().expect("spawn leased identity");
    let mut owned = DirectChild::new(original, reaper.clone());
    let exit_deadline = Instant::now() + Duration::from_secs(2);
    while !owned.exit_observed().expect("observe leased child exit")
        && Instant::now() < exit_deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }

    let mut replacement_command = std::process::Command::new("sh");
    replacement_command.args(["-c", "sleep 5"]).process_group(0);
    let mut replacement = replacement_command
        .spawn()
        .expect("spawn leased replacement identity");
    owned.replace_numeric_identity_for_test(replacement.id());
    clear_signal_attempts();

    assert!(owned
        .quarantine_leased(lifetime)
        .expect("transfer child and lease to reaper"));
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(reaper.snapshot().reaped, 0);
    drop(lifetime_writer);
    let reap_deadline = Instant::now() + Duration::from_secs(2);
    while reaper.snapshot().reaped < 1 && Instant::now() < reap_deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(reaper.snapshot().reaped, 1);
    assert!(take_signal_attempts().is_empty());
    assert!(replacement.try_wait().expect("probe replacement").is_none());

    replacement.kill().expect("stop leased replacement fixture");
    while replacement
        .try_wait()
        .expect("reap leased replacement fixture")
        .is_none()
    {
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(unix)]
#[test]
fn direct_child_wait_does_not_reap_after_its_deadline() {
    let reaper = NoSignalReaper::start().expect("start late-wait fixture reaper");
    let mut command = std::process::Command::new("sh");
    command.args(["-c", "exit 0"]).process_group(0);
    let child = command.spawn().expect("spawn late-wait fixture");
    let mut owned = DirectChild::new(child, reaper);
    let exit_deadline = Instant::now() + Duration::from_secs(2);
    while !owned.exit_observed().expect("observe late-wait child exit")
        && Instant::now() < exit_deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(owned
        .wait_until(Instant::now())
        .expect("expired wait does not observe completion")
        .is_none());
    assert!(owned
        .wait_until(Instant::now() + Duration::from_secs(2))
        .expect("future wait reaps completion")
        .is_some());
}

#[cfg(unix)]
#[test]
fn failed_leased_adoption_is_retried_before_drop_releases_ownership() {
    let reaper = NoSignalReaper::start().expect("start leased-adoption retry reaper");
    let (lifetime, lifetime_writer) =
        ProcessLifetimeLease::create().expect("create retry fixture lifetime");
    let mut command = std::process::Command::new("sh");
    command.args(["-c", "exit 0"]).process_group(0);
    let child = command.spawn().expect("spawn leased-adoption retry child");
    let failures = CleanupFailures::default();
    let process = ManagedInternalProcess::new(
        child,
        Instant::now(),
        failures.clone(),
        reaper.clone(),
        lifetime,
    );
    fail_next_reaper_adoption();

    drop(process);
    assert_eq!(reaper.snapshot().adopted, 1);
    assert_eq!(reaper.snapshot().reaped, 0);
    assert!(failures
        .take()
        .iter()
        .any(|failure| failure.contains("injected adoption failure")));

    drop(lifetime_writer);
    let reap_deadline = Instant::now() + Duration::from_secs(2);
    while reaper.snapshot().reaped < 1 && Instant::now() < reap_deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(reaper.snapshot().reaped, 1);
    assert!(reaper.snapshot().failures.is_empty());
}

#[cfg(unix)]
#[test]
fn owned_direct_child_rejects_an_absent_process_group() {
    let reaper = NoSignalReaper::start().expect("start absent-group fixture reaper");
    let mut command = std::process::Command::new("sh");
    command.args(["-c", "sleep 5"]).process_group(0);
    let child = command.spawn().expect("spawn owned process group");
    let mut owned = DirectChild::new(child, reaper);

    force_next_signal_group_absent();
    let error = owned
        .signal_group(ProcessSignal::Kill)
        .expect_err("owned direct child cannot treat an absent group as cleanup");
    assert!(error
        .to_string()
        .contains("owned direct-child process group"));

    assert_eq!(
        owned
            .signal_group(ProcessSignal::Kill)
            .expect("kill owned fixture after injected absence"),
        SignalDelivery::Sent
    );
    assert!(owned
        .wait_until(Instant::now() + Duration::from_secs(2))
        .expect("reap owned fixture")
        .is_some());
}

#[test]
fn no_signal_reaper_retries_a_transient_wait_error() {
    let reaper = NoSignalReaper::start().expect("start transient-error reaper");
    reaper.inject_next_wait_error();
    let child = std::process::Command::new("sh")
        .args(["-c", "exit 0"])
        .spawn()
        .expect("spawn transient-error fixture");
    let mut owned = DirectChild::new(child, reaper.clone());

    assert!(owned
        .quarantine("transient-error fixture")
        .expect("transfer fixture to reaper"));
    let deadline = Instant::now() + Duration::from_secs(2);
    while reaper.snapshot().reaped < 1 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let snapshot = reaper.snapshot();
    assert_eq!(snapshot.adopted, 1);
    assert_eq!(snapshot.reaped, 1);
    assert_eq!(snapshot.failures.len(), 1);
    assert!(snapshot.failures[0].contains("will retry"));
}
