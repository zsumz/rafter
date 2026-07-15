use super::{
    await_target_process_group, duration_ms, measurement_error, parse_peak_rss,
    process_group_rss_kib, process_group_state, retained_error, retained_result,
    take_fallback_cleanup_failures, terminate_after_timeout, CollectedProcessStatus, Duration,
    Error, Instant, InvocationReceipt, ManagedProcess, Path, ProcessGroupState, ProcessOutput,
    ProcessPolicy, TerminationReceipt, PROCESS_POLL_INTERVAL,
};

pub(super) fn finish_managed_process(
    mut process: ManagedProcess,
    result: Result<ProcessOutput, Box<dyn Error>>,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
) -> Result<ProcessOutput, Box<dyn Error>> {
    let mut outcome = match result {
        Ok(output) => {
            process.disarm();
            Ok(output)
        }
        Err(error) => match process.cleanup() {
            Ok(()) => Err(error),
            Err(cleanup_error) => Err(retained_error(
                format!("{error}; subprocess cleanup failed: {cleanup_error}"),
                stdout_path,
                stderr_path,
                Some(resource_path),
            )),
        },
    };
    drop(process);
    let fallback_cleanup_failures = take_fallback_cleanup_failures();
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
    invocation: InvocationReceipt,
    started: Instant,
    timeout: Duration,
    policy: ProcessPolicy,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
    process_group_path: &Path,
    reservation_path: &Path,
) -> Result<ProcessOutput, Box<dyn Error>> {
    let process_group = await_target_process_group(
        process,
        process_group_path,
        policy.kill_confirmation_timeout,
        stdout_path,
        stderr_path,
        resource_path,
    )?;
    let mut peak_rss_kib = 0;
    let completion = loop {
        peak_rss_kib = peak_rss_kib.max(process_group_rss_kib(process_group).map_err(|error| {
            retained_error(error, stdout_path, stderr_path, Some(resource_path))
        })?);
        let leader_status = retained_result(
            process.try_wait(),
            stdout_path,
            stderr_path,
            Some(resource_path),
        )?;
        let group_state = process_group_state(process_group).map_err(|error| {
            retained_error(error, stdout_path, stderr_path, Some(resource_path))
        })?;
        if group_state == ProcessGroupState::Absent {
            process.mark_target_absent();
            if let Some(status) = leader_status {
                break CollectedProcessStatus {
                    status,
                    timed_out: false,
                    term_signal_sent: false,
                    kill_signal_sent: false,
                };
            }
            if started.elapsed() >= timeout {
                return Err(retained_error(
                    "resource wrapper did not exit after the target process group disappeared",
                    stdout_path,
                    stderr_path,
                    Some(resource_path),
                ));
            }
            std::thread::sleep(PROCESS_POLL_INTERVAL);
            continue;
        }
        if started.elapsed() >= timeout {
            let termination = terminate_after_timeout(
                process,
                process_group,
                policy,
                &mut peak_rss_kib,
                stdout_path,
                stderr_path,
                resource_path,
            )?;
            break CollectedProcessStatus {
                status: termination.status,
                timed_out: termination.timed_out,
                term_signal_sent: termination.term_signal_sent,
                kill_signal_sent: termination.kill_signal_sent,
            };
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL);
    };
    finalize_process_output(
        invocation,
        started,
        policy.termination_grace,
        peak_rss_kib,
        completion,
        stdout_path,
        stderr_path,
        resource_path,
        process_group_path,
        reservation_path,
    )
}

#[allow(clippy::too_many_arguments)]
fn finalize_process_output(
    invocation: InvocationReceipt,
    started: Instant,
    grace: Duration,
    mut peak_rss_kib: u64,
    completion: CollectedProcessStatus,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
    process_group_path: &Path,
    reservation_path: &Path,
) -> Result<ProcessOutput, Box<dyn Error>> {
    let stdout = retained_result(
        crate::producer::filesystem::read_file(stdout_path),
        stdout_path,
        stderr_path,
        Some(resource_path),
    )?;
    let stderr = retained_result(
        crate::producer::filesystem::read_file(stderr_path),
        stdout_path,
        stderr_path,
        Some(resource_path),
    )?;
    let resource_telemetry = retained_result(
        crate::producer::filesystem::read_file(resource_path),
        stdout_path,
        stderr_path,
        Some(resource_path),
    )?;
    let authoritative_peak = parse_peak_rss(&resource_telemetry).ok_or_else(|| {
        measurement_error(
            "/usr/bin/time did not report maximum resident set size",
            stdout_path,
            stderr_path,
            resource_path,
        )
    })?;
    peak_rss_kib = peak_rss_kib.max(authoritative_peak);
    if peak_rss_kib == 0 {
        return Err(measurement_error(
            "resource measurement did not observe the process group",
            stdout_path,
            stderr_path,
            resource_path,
        ));
    }
    crate::producer::filesystem::remove_file(stdout_path)?;
    crate::producer::filesystem::remove_file(stderr_path)?;
    crate::producer::filesystem::remove_file(resource_path)?;
    crate::producer::filesystem::remove_file(process_group_path)?;
    crate::producer::filesystem::remove_file(reservation_path)?;
    Ok(ProcessOutput {
        invocation,
        status: completion.status,
        stdout,
        stderr,
        duration: started.elapsed(),
        peak_rss_kib,
        timed_out: completion.timed_out,
        termination: Some(TerminationReceipt {
            process_group: true,
            term_signal_sent: completion.term_signal_sent,
            grace_ms: duration_ms(grace),
            kill_signal_sent: completion.kill_signal_sent,
        }),
    })
}
