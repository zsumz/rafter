//! Deadline-bounded wrapper reaping and post-SIGKILL group confirmation.

use std::{
    error::Error,
    path::Path,
    process::ExitStatus,
    time::{Duration, Instant},
};

use super::model::ProcessSignal;
use super::{
    duration_ms, retained_error, retained_result, ManagedProcess, ProcessCompletion,
    SignalDelivery, PROCESS_POLL_INTERVAL,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn kill_process_group_after_grace(
    process: &mut ManagedProcess,
    term_signal_sent: bool,
    lifecycle_deadline: Instant,
    confirmation_timeout: Duration,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
) -> Result<ProcessCompletion, Box<dyn Error>> {
    let kill_signal_sent = match process.signal_target_group(ProcessSignal::Kill) {
        Ok(SignalDelivery::Sent | SignalDelivery::AlreadySent) => true,
        Ok(SignalDelivery::GroupAbsent) => false,
        Err(error) => {
            return Err(retained_error(
                error,
                stdout_path,
                stderr_path,
                Some(resource_path),
            ));
        }
    };
    let started = Instant::now();
    let confirmation_deadline = started
        .checked_add(confirmation_timeout)
        .ok_or("SIGKILL confirmation deadline overflow")?
        .min(lifecycle_deadline);
    let status = loop {
        let observation = process
            .observe_target_members(confirmation_deadline, lifecycle_deadline)
            .map_err(|error| {
                retained_error(error, stdout_path, stderr_path, Some(resource_path))
            })?;
        if let Some(proof) = observation.into_quiescence() {
            process
                .reap_target_anchor_after_kill(proof, confirmation_deadline)
                .map_err(|error| {
                    retained_error(error, stdout_path, stderr_path, Some(resource_path))
                })?;
            break await_wrapper_status(
                process,
                lifecycle_deadline,
                stdout_path,
                stderr_path,
                resource_path,
            )?;
        }
        let now = Instant::now();
        if now >= confirmation_deadline {
            let wrapper = if process.wrapper_exit_observed().unwrap_or(false) {
                "exited"
            } else {
                "alive"
            };
            return Err(retained_error(
                format!(
                    "SIGKILL confirmation expired after {} ms with process group {group_state} and resource wrapper {wrapper}",
                    duration_ms(started.elapsed()),
                    group_state = if observation.into_quiescence().is_some() {
                        "quiescent"
                    } else {
                        "live"
                    }
                ),
                stdout_path,
                stderr_path,
                Some(resource_path),
            ));
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL.min(confirmation_deadline.duration_since(now)));
    };
    Ok(ProcessCompletion {
        status,
        timed_out: true,
        term_signal_sent,
        kill_signal_sent,
    })
}

pub(super) fn await_wrapper_status(
    process: &mut ManagedProcess,
    deadline: Instant,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
) -> Result<ExitStatus, Box<dyn Error>> {
    let started = Instant::now();
    retained_result(
        process.wait_until(deadline),
        stdout_path,
        stderr_path,
        Some(resource_path),
    )?
    .ok_or_else(|| {
        retained_error(
            format!(
                "resource wrapper did not exit within {} ms",
                duration_ms(started.elapsed())
            ),
            stdout_path,
            stderr_path,
            Some(resource_path),
        )
    })
}
