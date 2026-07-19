//! Raw bounded-process collection and resource-receipt finalization.

use std::{error::Error, path::Path, time::Instant};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use super::{
    await_target_process_group, finalize_process_output, retained_error, retained_result,
    terminate_after_timeout, CleanupFailures, FinalizationPolicy, ManagedProcess,
    PendingProcessOutput, ProcessArtifacts, ProcessCompletion, ProcessDeadlines, TerminationPolicy,
    PROCESS_POLL_INTERVAL,
};

pub(super) fn finish_managed_process(
    mut process: ManagedProcess,
    result: Result<PendingProcessOutput, Box<dyn Error>>,
    deadlines: ProcessDeadlines,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
    cleanup_failures: &CleanupFailures,
) -> Result<PendingProcessOutput, Box<dyn Error>> {
    let mut outcome = match result {
        Ok(output) => process
            .disarm()
            .map(|()| output)
            .map_err(|error| retained_error(error, stdout_path, stderr_path, Some(resource_path))),
        Err(error) => {
            match process.cleanup_until(deadlines.cleanup_start, deadlines.finalization_start) {
                Ok(()) => Err(error),
                Err(cleanup_error) => Err(retained_error(
                    format!("{error}; subprocess cleanup failed: {cleanup_error}"),
                    stdout_path,
                    stderr_path,
                    Some(resource_path),
                )),
            }
        }
    };
    drop(process);
    let fallback_cleanup_failures = cleanup_failures.take();
    if !fallback_cleanup_failures.is_empty() {
        let detail = format!(
            "fallback subprocess cleanup failed: {}",
            fallback_cleanup_failures.join("; ")
        );
        outcome = match outcome {
            Ok(_) => Err(retained_error(
                detail,
                stdout_path,
                stderr_path,
                Some(resource_path),
            )),
            Err(error) => Err(retained_error(
                format!("{error}; {detail}"),
                stdout_path,
                stderr_path,
                Some(resource_path),
            )),
        };
    }
    outcome
}

#[allow(clippy::too_many_arguments)]
pub(super) fn collect_process_output(
    process: &mut ManagedProcess,
    target_group_ack: &mut UnixStream,
    started: Instant,
    deadlines: ProcessDeadlines,
    policy: TerminationPolicy,
    finalization: FinalizationPolicy,
    artifacts: &ProcessArtifacts,
) -> Result<super::ProcessOutput, Box<dyn Error>> {
    let stdout_path = artifacts.stdout_path();
    let stderr_path = artifacts.stderr_path();
    let resource_path = artifacts.resource_path();
    let publication_deadline = Instant::now()
        .checked_add(policy.publication_timeout)
        .ok_or("process-group publication deadline overflow")?
        .min(deadlines.execution_window);
    await_target_process_group(process, artifacts, target_group_ack, publication_deadline)?;
    let target_deadline = Instant::now()
        .checked_add(deadlines.target_timeout)
        .ok_or("target process deadline overflow")?
        .min(deadlines.execution_window);
    let mut peak_rss_kib = 0;
    let completion = loop {
        let observation = process
            .observe_target_members(deadlines.execution_window, deadlines.cleanup_start)
            .map_err(|error| {
                retained_error(error, &stdout_path, &stderr_path, Some(&resource_path))
            })?;
        peak_rss_kib = peak_rss_kib.max(observation.rss_kib());
        let quiescence = observation.into_quiescence();
        let wrapper_exited = process.wrapper_exit_observed().map_err(|error| {
            retained_error(error, &stdout_path, &stderr_path, Some(&resource_path))
        })?;
        if Instant::now() >= target_deadline {
            break terminate_after_timeout(
                process,
                policy,
                deadlines.cleanup_start,
                &stdout_path,
                &stderr_path,
                &resource_path,
            )?;
        }
        if let Some(proof) = quiescence.filter(|_| wrapper_exited) {
            process
                .release_target_anchor(proof, deadlines.cleanup_start)
                .map_err(|error| {
                    retained_error(error, &stdout_path, &stderr_path, Some(&resource_path))
                })?;
            let status = retained_result(
                process.try_wait(),
                &stdout_path,
                &stderr_path,
                Some(&resource_path),
            )?
            .ok_or_else(|| {
                retained_error(
                    "resource wrapper exit was observed without a waitable status",
                    &stdout_path,
                    &stderr_path,
                    Some(&resource_path),
                )
            })?;
            break ProcessCompletion {
                status,
                timed_out: false,
                term_signal_sent: false,
                kill_signal_sent: false,
            };
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL);
    };
    finalize_process_output(
        started,
        policy,
        finalization,
        peak_rss_kib,
        completion,
        deadlines.lifecycle,
        artifacts,
    )
}
