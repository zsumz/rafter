//! Deterministic internal-command fault and boundary controls.

use std::{error::Error, time::Duration};

thread_local! {
    static INJECT_NEXT_DRAIN_ERROR: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
    static NEXT_COMPLETION_DELAY: std::cell::Cell<Option<Duration>> = const {
        std::cell::Cell::new(None)
    };
}

pub(crate) fn inject_next_internal_drain_error() {
    INJECT_NEXT_DRAIN_ERROR.with(|inject| inject.set(true));
}

pub(crate) fn delay_next_internal_completion_check(delay: Duration) {
    NEXT_COMPLETION_DELAY.with(|next| next.set(Some(delay)));
}

pub(crate) fn bounded_internal_output(
    program: &str,
    arguments: &[&str],
    timeout: Duration,
) -> Result<std::process::Output, Box<dyn Error>> {
    bounded_internal_output_with_cleanup(program, arguments, timeout, timeout)
}

pub(crate) fn bounded_internal_output_with_cleanup(
    program: &str,
    arguments: &[&str],
    execution_timeout: Duration,
    cleanup_timeout: Duration,
) -> Result<std::process::Output, Box<dyn Error>> {
    let execution_deadline = std::time::Instant::now()
        .checked_add(execution_timeout)
        .ok_or("internal command deadline overflow")?;
    let lifecycle_deadline = execution_deadline
        .checked_add(cleanup_timeout)
        .ok_or("internal command cleanup deadline overflow")?;
    let reaper = super::super::NoSignalReaper::start()?;
    super::bounded_internal_output_from(
        program,
        None,
        arguments,
        execution_deadline,
        lifecycle_deadline,
        reaper,
    )
}

pub(super) fn delay_completion_if_requested() {
    NEXT_COMPLETION_DELAY.with(|next| {
        if let Some(delay) = next.take() {
            std::thread::sleep(delay);
        }
    });
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
