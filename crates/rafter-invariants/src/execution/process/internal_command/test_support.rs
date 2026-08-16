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
    static INJECT_NEXT_DRAIN_ERROR: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
    static AWAIT_NEXT_COMPLETION_AFTER_DEADLINE: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

pub(crate) fn inject_next_internal_drain_error() {
    inject_next_internal_drain_errors(1);
}

/// Fails the next `count` internal drains, so a scenario can distinguish a
/// transient stall from a persistent one.
pub(crate) fn inject_next_internal_drain_errors(count: usize) {
    INJECT_NEXT_DRAIN_ERROR.with(|inject| inject.set(count));
}

/// Hold the next completion check until the child has exited and its execution
/// deadline has arrived.
///
/// Callers use this to reach the "clean exit classified after its deadline"
/// ordering. Waiting for both facts avoids guessing whether a fixed delay is
/// long enough for the child or accidentally classifying a fast child before
/// the deadline.
pub(crate) fn await_next_internal_completion_after_deadline() {
    AWAIT_NEXT_COMPLETION_AFTER_DEADLINE.with(|await_completion| await_completion.set(true));
}

pub(crate) fn bounded_internal_output(
    program: &str,
    arguments: &[&str],
    timeout: Duration,
) -> Result<std::process::Output, Box<dyn Error>> {
    bounded_internal_output_with_cleanup(program, arguments, timeout, INTERNAL_CLEANUP_ALLOWANCE)
}

fn bounded_internal_output_with_cleanup(
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

pub(super) fn await_completion_boundary_if_requested(
    process: &mut ManagedInternalProcess,
    execution_deadline: Instant,
    lifecycle_deadline: Instant,
) -> Result<(), Box<dyn Error>> {
    if !AWAIT_NEXT_COMPLETION_AFTER_DEADLINE
        .with(|await_completion| await_completion.replace(false))
    {
        return Ok(());
    }
    while !process.exit_observed()? {
        if Instant::now() >= lifecycle_deadline {
            return Err("internal command did not exit before its absolute deadline".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    while Instant::now() < execution_deadline {
        std::thread::sleep(
            Duration::from_millis(1)
                .min(execution_deadline.saturating_duration_since(Instant::now())),
        );
    }
    Ok(())
}

pub(super) fn inject_drain_error_if_requested() -> std::io::Result<()> {
    INJECT_NEXT_DRAIN_ERROR.with(|inject| {
        let remaining = inject.get();
        if remaining == 0 {
            return Ok(());
        }
        inject.set(remaining - 1);
        Err(std::io::Error::other("injected internal drain failure"))
    })
}
