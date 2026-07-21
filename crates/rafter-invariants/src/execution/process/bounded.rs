//! Standalone bounded execution over an already descriptor-bound command.

use std::{collections::BTreeMap, error::Error, time::Duration, time::Instant};

#[cfg(unix)]
use std::os::fd::BorrowedFd;

use super::{BoundCommand, FinalizationPolicy, ProcessDeadlines, ProcessOutput, TerminationPolicy};

const TERMINATION_GRACE: Duration = Duration::from_secs(30);
const KILL_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);
const FINALIZATION_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn run_bounded(
    command: &BoundCommand,
    environment: &BTreeMap<String, String>,
    timeout: Duration,
    lifecycle_deadline: Instant,
    inherited_descriptors: &[BorrowedFd<'_>],
) -> Result<ProcessOutput, Box<dyn Error>> {
    let (deadlines, termination, finalization) = policies(timeout, lifecycle_deadline)?;
    command.verify_path_bindings()?;
    let pending = command.run(
        environment,
        deadlines,
        termination,
        finalization,
        inherited_descriptors,
    )?;
    if let Err(error) = command.verify_path_bindings() {
        return Err(pending.retained_error(error));
    }
    pending.finalize()
}

fn policies(
    timeout: Duration,
    lifecycle_deadline: Instant,
) -> Result<(ProcessDeadlines, TerminationPolicy, FinalizationPolicy), &'static str> {
    policies_at(Instant::now(), timeout, lifecycle_deadline)
}

fn policies_at(
    now: Instant,
    timeout: Duration,
    lifecycle_deadline: Instant,
) -> Result<(ProcessDeadlines, TerminationPolicy, FinalizationPolicy), &'static str> {
    let termination_allowance = TERMINATION_GRACE
        .checked_add(KILL_CONFIRMATION_TIMEOUT)
        .ok_or("bounded process termination allowance overflow")?;
    let cleanup_allowance = KILL_CONFIRMATION_TIMEOUT;
    let fixed_allowance = KILL_CONFIRMATION_TIMEOUT
        .checked_add(termination_allowance)
        .and_then(|value| value.checked_add(cleanup_allowance))
        .and_then(|value| value.checked_add(FINALIZATION_TIMEOUT))
        .ok_or("bounded process fixed lifecycle allowance overflow")?;
    let target_timeout = lifecycle_deadline
        .checked_duration_since(now)
        .and_then(|remaining| remaining.checked_sub(fixed_allowance))
        .map(|remaining| remaining.min(timeout))
        .filter(|remaining| !remaining.is_zero())
        .ok_or("bounded process lifecycle deadline leaves no target execution budget")?;
    let execution_allowance = target_timeout
        .checked_add(KILL_CONFIRMATION_TIMEOUT)
        .ok_or("bounded process execution allowance overflow")?;
    let execution_window = now
        .checked_add(execution_allowance)
        .ok_or("bounded process execution deadline overflow")?;
    let cleanup_start = execution_window
        .checked_add(termination_allowance)
        .ok_or("bounded process cleanup boundary overflow")?;
    let finalization_start = cleanup_start
        .checked_add(cleanup_allowance)
        .ok_or("bounded process finalization boundary overflow")?;
    let lifecycle = finalization_start
        .checked_add(FINALIZATION_TIMEOUT)
        .ok_or("bounded process lifecycle deadline overflow")?;
    if lifecycle > lifecycle_deadline {
        return Err("bounded process lifecycle exceeds its outer deadline");
    }
    let deadlines = ProcessDeadlines::new(
        target_timeout,
        execution_window,
        cleanup_start,
        finalization_start,
        lifecycle,
    )?;
    let termination = TerminationPolicy {
        grace: TERMINATION_GRACE,
        publication_timeout: KILL_CONFIRMATION_TIMEOUT,
        kill_confirmation_timeout: KILL_CONFIRMATION_TIMEOUT,
    };
    Ok((
        deadlines,
        termination,
        FinalizationPolicy::bounded(FINALIZATION_TIMEOUT),
    ))
}

#[cfg(test)]
#[path = "bounded_tests.rs"]
mod tests;
