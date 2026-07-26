//! Deterministic internal-command fault and boundary controls.

use std::{
    error::Error,
    time::{Duration, Instant},
};

use super::super::ManagedInternalProcess;

/// Teardown allowance for bounded internal commands in tests.
///
/// This is deliberately *not* the execution timeout. The execution timeout is
/// the bound under test; reclaiming the child's process group and pipes
/// afterwards is teardown, and giving teardown the same tiny budget turns a
/// slow machine into a different error message instead of the one the test is
/// about. Expiry of this allowance is a harness failure, not a property.
const INTERNAL_CLEANUP_ALLOWANCE: Duration = Duration::from_secs(10);

thread_local! {
    static INJECT_NEXT_DRAIN_ERROR: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static AWAIT_NEXT_COMPLETION_EXIT: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

pub(crate) fn inject_next_internal_drain_error() {
    INJECT_NEXT_DRAIN_ERROR.with(|inject| inject.set(true));
}

/// Hold the next completion check until the child has actually exited.
///
/// Callers use this to reach the "clean exit classified after its deadline"
/// ordering. Waiting for the observed exit rather than sleeping for a fixed
/// duration keeps that ordering on a machine where the child needs longer than
/// some constant to run.
pub(crate) fn await_next_internal_completion_exit() {
    AWAIT_NEXT_COMPLETION_EXIT.with(|await_exit| await_exit.set(true));
}

pub(crate) fn bounded_internal_output(
    program: &str,
    arguments: &[&str],
    timeout: Duration,
) -> Result<std::process::Output, Box<dyn Error>> {
    bounded_internal_output_with_cleanup(program, arguments, timeout, INTERNAL_CLEANUP_ALLOWANCE)
}

pub(crate) fn bounded_internal_output_with_cleanup(
    program: &str,
    arguments: &[&str],
    execution_timeout: Duration,
    cleanup_timeout: Duration,
) -> Result<std::process::Output, Box<dyn Error>> {
    let reaper = super::super::NoSignalReaper::start()?;
    bounded_internal_output_with_reaper(
        program,
        arguments,
        execution_timeout,
        cleanup_timeout,
        reaper,
    )
}

pub(crate) fn bounded_internal_output_with_reaper(
    program: &str,
    arguments: &[&str],
    execution_timeout: Duration,
    cleanup_timeout: Duration,
    reaper: super::super::NoSignalReaper,
) -> Result<std::process::Output, Box<dyn Error>> {
    let execution_deadline = std::time::Instant::now()
        .checked_add(execution_timeout)
        .ok_or("internal command deadline overflow")?;
    let lifecycle_deadline = execution_deadline
        .checked_add(cleanup_timeout)
        .ok_or("internal command cleanup deadline overflow")?;
    super::bounded_internal_output_from(
        program,
        None,
        arguments,
        None,
        execution_deadline,
        lifecycle_deadline,
        reaper,
    )
}

pub(super) fn await_child_exit_if_requested(
    process: &mut ManagedInternalProcess,
    deadline: Instant,
) -> Result<(), Box<dyn Error>> {
    if !AWAIT_NEXT_COMPLETION_EXIT.with(|await_exit| await_exit.replace(false)) {
        return Ok(());
    }
    while !process.exit_observed()? {
        if Instant::now() >= deadline {
            return Err("internal command did not exit before its absolute deadline".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

pub(super) fn inject_drain_error_if_requested() -> std::io::Result<()> {
    INJECT_NEXT_DRAIN_ERROR.with(|inject| {
        if inject.replace(false) {
            Err(std::io::Error::other("injected internal drain failure"))
        } else {
            Ok(())
        }
    })
}
