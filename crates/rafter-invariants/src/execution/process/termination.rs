//! Timed process-group termination with retained diagnostic paths.

use std::{error::Error, path::Path, time::Instant};

use super::{
    diagnostics::retained_error,
    model::{ProcessSignal, PROCESS_POLL_INTERVAL},
    reaping::{await_wrapper_status, kill_process_group_after_grace},
    ManagedProcess, ProcessCompletion, SignalDelivery, TerminationPolicy,
};

pub(crate) fn terminate_after_timeout(
    process: &mut ManagedProcess,
    policy: TerminationPolicy,
    lifecycle_deadline: Instant,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
) -> Result<ProcessCompletion, Box<dyn Error>> {
    let observation = process
        .observe_target_members(lifecycle_deadline, lifecycle_deadline)
        .map_err(|error| retained_error(error, stdout_path, stderr_path, Some(resource_path)))?;
    if let Some(proof) = observation.into_quiescence() {
        process
            .release_target_anchor(proof, lifecycle_deadline)
            .map_err(|error| {
                retained_error(error, stdout_path, stderr_path, Some(resource_path))
            })?;
        let status = await_wrapper_status(
            process,
            lifecycle_deadline,
            stdout_path,
            stderr_path,
            resource_path,
        )?;
        return Ok(ProcessCompletion {
            status,
            timed_out: true,
            term_signal_sent: false,
            kill_signal_sent: false,
        });
    }
    let term_signal_sent = match process.signal_target_group(ProcessSignal::Term) {
        Ok(SignalDelivery::Sent | SignalDelivery::AlreadySent) => true,
        Ok(SignalDelivery::GroupAbsent) => {
            return Err(retained_error(
                "target process group was released before SIGTERM",
                stdout_path,
                stderr_path,
                Some(resource_path),
            ));
        }
        Err(error) => {
            return Err(retained_error(
                error,
                stdout_path,
                stderr_path,
                Some(resource_path),
            ));
        }
    };
    await_termination_after_term(
        process,
        term_signal_sent,
        policy,
        lifecycle_deadline,
        stdout_path,
        stderr_path,
        resource_path,
    )
}

#[allow(clippy::too_many_arguments)]
fn await_termination_after_term(
    process: &mut ManagedProcess,
    term_signal_sent: bool,
    policy: TerminationPolicy,
    lifecycle_deadline: Instant,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
) -> Result<ProcessCompletion, Box<dyn Error>> {
    let grace_started = Instant::now();
    let grace_deadline = grace_started
        .checked_add(policy.grace)
        .ok_or("termination grace deadline overflow")?
        .min(lifecycle_deadline);
    loop {
        if Instant::now() >= grace_deadline {
            return kill_process_group_after_grace(
                process,
                term_signal_sent,
                lifecycle_deadline,
                policy.kill_confirmation_timeout,
                stdout_path,
                stderr_path,
                resource_path,
            );
        }
        match process.try_observe_target_members(grace_deadline, lifecycle_deadline) {
            // The grace window closed before an observation could start, which
            // is the window ending -- the same thing the deadline check at the
            // top of this loop would have seen on the next pass. Escalate
            // exactly as real expiry does.
            Ok(None) => {
                return kill_process_group_after_grace(
                    process,
                    term_signal_sent,
                    lifecycle_deadline,
                    policy.kill_confirmation_timeout,
                    stdout_path,
                    stderr_path,
                    resource_path,
                );
            }
            Ok(Some(observation)) => {
                if let Some(proof) = observation.into_quiescence() {
                    process
                        .release_target_anchor(proof, lifecycle_deadline)
                        .map_err(|error| {
                            retained_error(error, stdout_path, stderr_path, Some(resource_path))
                        })?;
                    let status = await_wrapper_status(
                        process,
                        lifecycle_deadline,
                        stdout_path,
                        stderr_path,
                        resource_path,
                    )?;
                    return Ok(ProcessCompletion {
                        status,
                        timed_out: true,
                        term_signal_sent,
                        kill_signal_sent: false,
                    });
                }
            }
            Err(_) if Instant::now() >= grace_deadline => {
                return kill_process_group_after_grace(
                    process,
                    term_signal_sent,
                    lifecycle_deadline,
                    policy.kill_confirmation_timeout,
                    stdout_path,
                    stderr_path,
                    resource_path,
                );
            }
            Err(error) => {
                return Err(retained_error(
                    error,
                    stdout_path,
                    stderr_path,
                    Some(resource_path),
                ));
            }
        }
        let now = Instant::now();
        if now >= grace_deadline {
            return kill_process_group_after_grace(
                process,
                term_signal_sent,
                lifecycle_deadline,
                policy.kill_confirmation_timeout,
                stdout_path,
                stderr_path,
                resource_path,
            );
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL.min(grace_deadline.duration_since(now)));
    }
}
