//! Raw bounded-process collection and resource-receipt finalization.

#[cfg(test)]
mod test_support;

use std::{error::Error, path::Path, time::Instant};

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use super::managed::TargetObservation;
use super::telemetry::{ObserverCommandFailure, PS_TELEMETRY_TIMEOUT};
use super::{
    await_target_process_group, finalize_process_output, retained_error, retained_result,
    terminate_after_timeout, CleanupFailures, FinalizationPolicy, ManagedProcess,
    PendingProcessOutput, ProcessArtifacts, ProcessCompletion, ProcessDeadlines, TerminationPolicy,
    PROCESS_POLL_INTERVAL,
};

#[cfg(test)]
pub(crate) use test_support::{
    await_next_target_stdout_prefix, hold_next_poll_until_the_execution_window_closes,
};
#[cfg(test)]
use test_support::{
    await_target_stdout_readiness, hold_poll_across_the_execution_window_if_requested,
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
    #[cfg(test)]
    await_target_stdout_readiness(artifacts, deadlines.execution_window)
        .map_err(|error| retained_error(error, &stdout_path, &stderr_path, Some(&resource_path)))?;
    let target_deadline = Instant::now()
        .checked_add(deadlines.target_timeout)
        .ok_or("target process deadline overflow")?
        .min(deadlines.execution_window);
    let mut peak_rss_kib = 0;
    let completion = loop {
        // The edge answered before the next observation is entered, the way the
        // grace loop answers its own. An observer entered after its window has
        // closed reports a stall -- correctly, because inside that call nothing
        // but the observer can have consumed the window -- and a run that had
        // merely reached its timeout would die of that stall instead. The poll
        // below is a hundred milliseconds and the observation is single-digit
        // ones, so the edge falls in the poll almost every time it falls
        // anywhere.
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
        let observed = observe_within_execution_window(process, deadlines).map_err(|error| {
            retained_error(error, &stdout_path, &stderr_path, Some(&resource_path))
        })?;
        let Some(observation) = observed else {
            // The execution window closed before an observation could start.
            // `target_deadline` never outlives that window, so it has closed
            // too: this is the timeout path below, reached without spending the
            // run on a `ps` that was never going to fit.
            break terminate_after_timeout(
                process,
                policy,
                deadlines.cleanup_start,
                &stdout_path,
                &stderr_path,
                &resource_path,
            )?;
        };
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
        // Clamped so the poll cannot carry the loop past the edge it is about
        // to check: the deadline check at the top of this loop is what decides
        // what a closed window means, and it can only decide it if the sleep
        // stops there.
        std::thread::sleep(
            PROCESS_POLL_INTERVAL.min(target_deadline.saturating_duration_since(Instant::now())),
        );
        #[cfg(test)]
        hold_poll_across_the_execution_window_if_requested(deadlines.execution_window);
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

/// Observes the target group, retrying one untruncated failure a single time.
///
/// The simulator's evidence is seed-deterministic, so a slow host cannot
/// corrupt what a run proves -- only the harness's ability to watch the process
/// tree while it proves it. Weekly run 31665042929 lost an eighty-four-minute
/// simulator run to one `ps` that took 140,062 ms to be noticed on a swapping
/// runner: a single transient stall, a destroyed run, and nothing learned about
/// the protocol. One fresh attempt costs another observation budget and keeps
/// the run; a second failure stays exactly as fatal as it has always been,
/// because a persistent inability to watch the tree is what failing closed is
/// for.
///
/// Only the execution-window loop retries. Termination and grace keep their
/// single attempt: there the grace clock is the authority over how long
/// anything may take, and that window's edge is already answered by reporting
/// it closed.
fn observe_within_execution_window(
    process: &mut ManagedProcess,
    deadlines: ProcessDeadlines,
) -> Result<Option<TargetObservation>, Box<dyn Error>> {
    match process.try_observe_target_members(deadlines.execution_window, deadlines.cleanup_start) {
        observed @ Ok(_) => observed,
        // Only the observer *command* failing is retried, and the observer says
        // so by type: a message prefix left the decision to whichever site
        // happened to format the string. An observation that ran and then
        // disagreed with itself -- a missing live anchor row, an anchor that
        // exited before release -- is a fail-closed property, and re-running it
        // would relax what the observation has to return.
        Err(stalled) if stalled.downcast_ref::<ObserverCommandFailure>().is_some() => {
            // Decided on what the window has left *after* the stall was
            // noticed, not on what it held before. A retry the window cannot
            // fit would be cut off for the same reason the first attempt was,
            // so it is not attempted; the original failure stands, which is
            // what keeps an observer that consumed a whole window fail-closed
            // rather than quietly reclassified.
            if deadlines
                .execution_window
                .saturating_duration_since(Instant::now())
                < PS_TELEMETRY_TIMEOUT
            {
                return Err(stalled);
            }
            process.try_observe_target_members(deadlines.execution_window, deadlines.cleanup_start)
        }
        failed @ Err(_) => failed,
    }
}
