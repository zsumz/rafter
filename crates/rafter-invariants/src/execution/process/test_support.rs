//! Recorder-level process cleanup fixtures exposed only to producer tests.

#[cfg(unix)]
use std::os::unix::process::CommandExt;
#[cfg(unix)]
use std::{fs::File, os::fd::AsFd, path::Path, time::Duration};

#[cfg(unix)]
pub(crate) fn induce_fallback_cleanup_failure() -> Vec<String> {
    let reaper = super::NoSignalReaper::start().expect("start fallback fixture reaper");
    let perl_path = Path::new("/usr/bin/perl");
    let perl = File::open(perl_path).expect("open fallback fixture Perl runtime");
    let anchor = super::ProcessGroupAnchor::spawn(
        super::RuntimeExecutable {
            path: perl_path,
            descriptor: perl.as_fd(),
        },
        std::time::Instant::now() + Duration::from_secs(2),
        reaper.clone(),
        std::process::Stdio::null(),
    )
    .expect("spawn fallback fixture anchor");
    let mut command = std::process::Command::new("sh");
    command.args(["-c", "exit 0"]).process_group(0);
    let child = command.spawn().expect("spawn isolated process group");
    let cleanup_deadline = std::time::Instant::now()
        .checked_add(std::time::Duration::from_millis(20))
        .expect("fallback cleanup deadline");
    let cleanup_failures = super::managed::CleanupFailures::default();
    let target_group = anchor.id();
    let (target_lifetime, target_lifetime_writer) =
        super::TargetLifetimeLease::create().expect("create fallback target lifetime lease");
    drop(target_lifetime_writer);
    let mut process = super::managed::ManagedProcess::new(
        child,
        anchor,
        cleanup_deadline,
        super::model::DEFAULT_KILL_CONFIRMATION_TIMEOUT,
        cleanup_failures.clone(),
        None,
        reaper,
        target_lifetime,
    );
    process
        .set_target_group(target_group)
        .expect("place fallback fixture in anchor group");
    super::managed::force_next_cleanup_target_alive();
    drop(process);
    cleanup_failures.take()
}
