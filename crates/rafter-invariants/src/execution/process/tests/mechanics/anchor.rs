//! Anchor startup diagnostics and ownership rollback scenarios.

use std::{
    fs::File,
    os::fd::AsFd,
    path::Path,
    time::{Duration, Instant},
};

use super::super::super::{
    base_environment, expire_next_anchor_readiness_classification, fail_next_anchor_startup,
    fail_next_process_lifetime_lease_creation, fail_next_reaper_adoption, process_group_state,
    retained_stderr_path, take_last_failed_anchor_startup_id, take_last_spawned_anchor_id,
    NoSignalReaper, ProcessGroupAnchor, ProcessGroupState, RuntimeExecutable,
};
use super::super::support::run_shell;

#[test]
fn anchor_startup_failure_retains_its_stderr() {
    fail_next_anchor_startup();
    let error = run_shell(
        "printf unreachable",
        &base_environment(),
        Path::new("."),
        Duration::from_secs(1),
        Duration::from_millis(20),
    )
    .expect_err("anchor startup failure must escape with retained diagnostics");
    let stderr_path = retained_stderr_path(error.as_ref())
        .expect("anchor startup error carries a typed retained stderr path");
    let deadline = Instant::now() + Duration::from_secs(2);
    let stderr = loop {
        let stderr = std::fs::read_to_string(&stderr_path).expect("read retained anchor stderr");
        if stderr.contains("injected anchor startup failure") || Instant::now() >= deadline {
            break stderr;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(stderr.contains("injected anchor startup failure"));
    assert_failed_anchor_disappears();
}

#[test]
fn anchor_startup_reports_initial_quarantine_failure_and_drop_retries() {
    fail_next_anchor_startup();
    fail_next_reaper_adoption();
    let error = run_shell(
        "printf unreachable",
        &base_environment(),
        Path::new("."),
        Duration::from_secs(1),
        Duration::from_millis(20),
    )
    .expect_err("anchor startup and its first quarantine attempt must both remain visible");
    let message = error.to_string();
    assert!(message.contains("await process-group anchor readiness"));
    assert!(message.contains("injected adoption failure"));
    assert_failed_anchor_disappears();
}

#[test]
fn anchor_readiness_observed_at_its_absolute_deadline_is_rejected() {
    let reaper = NoSignalReaper::start().expect("start anchor fixture reaper");
    let perl_path = Path::new("/usr/bin/perl");
    let perl = File::open(perl_path).expect("open anchor fixture Perl runtime");
    expire_next_anchor_readiness_classification();
    let error = ProcessGroupAnchor::spawn(
        RuntimeExecutable {
            path: perl_path,
            descriptor: perl.as_fd(),
        },
        Instant::now() + Duration::from_secs(2),
        reaper,
        std::process::Stdio::null(),
    )
    .expect_err("readiness classified at the absolute deadline must be rejected");
    assert!(error
        .to_string()
        .contains("process-group anchor readiness deadline expired"));
    assert_failed_anchor_disappears();
}

/// The target lifetime lease is created with the wrapper spawn that carries its
/// writer, which is after the anchor exists. So the launch owns an anchor when
/// the lease fails, and the guarantee is that it releases it rather than that it
/// never had one.
#[test]
fn lifetime_lease_failure_releases_the_anchor_it_would_have_carried() {
    let _ = take_last_spawned_anchor_id();
    fail_next_process_lifetime_lease_creation();
    let error = run_shell(
        "printf unreachable",
        &base_environment(),
        Path::new("."),
        Duration::from_secs(1),
        Duration::from_millis(20),
    )
    .expect_err("lifetime lease failure must abort launch");
    assert!(error
        .to_string()
        .contains("injected process lifetime lease creation failure"));
    let anchor = take_last_spawned_anchor_id().expect("the aborted launch recorded its anchor");
    assert_anchor_group_disappears(anchor);
}

fn assert_failed_anchor_disappears() {
    assert_anchor_group_disappears(
        take_last_failed_anchor_startup_id().expect("failed anchor identity was recorded"),
    );
}

fn assert_anchor_group_disappears(process_group: u32) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if process_group_state(process_group).expect("probe failed anchor group")
            == ProcessGroupState::Absent
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "failed anchor group {process_group} remained alive after rollback"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
