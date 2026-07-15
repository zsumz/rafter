use super::{
    duration_ms, fmt, kill_process_group, process_group_observation, process_group_rss_kib,
    Duration, Errno, Error, ExitStatus, Instant, ManagedProcess, Path, Pid, ProcessCleanupError,
    ProcessGroupState, ProcessPolicy, ProcessSignal, Signal, SignalDelivery, TimeoutTermination,
    PROCESS_POLL_INTERVAL,
};

pub(super) fn await_target_process_group(
    process: &mut ManagedProcess,
    process_group_path: &Path,
    timeout: Duration,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
) -> Result<u32, Box<dyn Error>> {
    let started = Instant::now();
    loop {
        if let Ok(source) = std::fs::read_to_string(process_group_path) {
            if let Ok(process_group) = source.trim().parse::<u32>() {
                if process_group > 0 && process_group != process.id() {
                    process.set_target_group(process_group);
                    return Ok(process_group);
                }
            }
        }
        if let Some(status) = retained_result(
            process.try_wait(),
            stdout_path,
            stderr_path,
            Some(resource_path),
        )? {
            return Err(retained_error(
                format!(
                    "resource wrapper exited {:?} before publishing the target process group",
                    status.code()
                ),
                stdout_path,
                stderr_path,
                Some(resource_path),
            ));
        }
        if started.elapsed() >= timeout {
            return Err(retained_error(
                format!(
                    "target launcher did not publish its process group within {} ms",
                    duration_ms(timeout)
                ),
                stdout_path,
                stderr_path,
                Some(resource_path),
            ));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

pub(super) fn terminate_after_timeout(
    process: &mut ManagedProcess,
    process_group: u32,
    policy: ProcessPolicy,
    peak_rss_kib: &mut u64,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
) -> Result<TimeoutTermination, Box<dyn Error>> {
    let leader_status = retained_result(
        process.try_wait(),
        stdout_path,
        stderr_path,
        Some(resource_path),
    )?;
    if process_group_state(process_group)
        .map_err(|error| retained_error(error, stdout_path, stderr_path, Some(resource_path)))?
        == ProcessGroupState::Absent
    {
        process.mark_target_absent();
        let status = match leader_status {
            Some(status) => status,
            None => retained_result(
                process.wait(),
                stdout_path,
                stderr_path,
                Some(resource_path),
            )?,
        };
        return Ok(TimeoutTermination {
            status,
            timed_out: false,
            term_signal_sent: false,
            kill_signal_sent: false,
        });
    }
    let term_signal_sent = match signal_process_group(process_group, ProcessSignal::Term) {
        Ok(SignalDelivery::Sent) => true,
        Ok(SignalDelivery::GroupAbsent) => {
            process.mark_target_absent();
            return Ok(TimeoutTermination {
                status: match leader_status {
                    Some(status) => status,
                    None => retained_result(
                        process.wait(),
                        stdout_path,
                        stderr_path,
                        Some(resource_path),
                    )?,
                },
                timed_out: false,
                term_signal_sent: false,
                kill_signal_sent: false,
            });
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
        process_group,
        leader_status,
        term_signal_sent,
        policy,
        peak_rss_kib,
        stdout_path,
        stderr_path,
        resource_path,
    )
}

#[allow(clippy::too_many_arguments)]
fn await_termination_after_term(
    process: &mut ManagedProcess,
    process_group: u32,
    mut leader_status: Option<ExitStatus>,
    term_signal_sent: bool,
    policy: ProcessPolicy,
    peak_rss_kib: &mut u64,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
) -> Result<TimeoutTermination, Box<dyn Error>> {
    let grace_started = Instant::now();
    loop {
        *peak_rss_kib =
            (*peak_rss_kib).max(process_group_rss_kib(process_group).map_err(|error| {
                retained_error(error, stdout_path, stderr_path, Some(resource_path))
            })?);
        if leader_status.is_none() {
            leader_status = retained_result(
                process.try_wait(),
                stdout_path,
                stderr_path,
                Some(resource_path),
            )?;
        }
        match process_group_state(process_group) {
            Ok(ProcessGroupState::Absent) => {
                process.mark_target_absent();
                let status = match leader_status {
                    Some(status) => status,
                    None => retained_result(
                        process.wait(),
                        stdout_path,
                        stderr_path,
                        Some(resource_path),
                    )?,
                };
                return Ok(TimeoutTermination {
                    status,
                    timed_out: true,
                    term_signal_sent,
                    kill_signal_sent: false,
                });
            }
            Ok(ProcessGroupState::Alive) => {}
            Err(error) => {
                return Err(retained_error(
                    error,
                    stdout_path,
                    stderr_path,
                    Some(resource_path),
                ));
            }
        }
        if grace_started.elapsed() >= policy.termination_grace {
            return kill_process_group_after_grace(
                process,
                process_group,
                leader_status,
                term_signal_sent,
                policy.kill_confirmation_timeout,
                stdout_path,
                stderr_path,
                resource_path,
            );
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

#[allow(clippy::too_many_arguments)]
fn kill_process_group_after_grace(
    process: &mut ManagedProcess,
    process_group: u32,
    leader_status: Option<ExitStatus>,
    term_signal_sent: bool,
    kill_confirmation_timeout: Duration,
    stdout_path: &Path,
    stderr_path: &Path,
    resource_path: &Path,
) -> Result<TimeoutTermination, Box<dyn Error>> {
    let kill_signal_sent = match signal_process_group(process_group, ProcessSignal::Kill) {
        Ok(SignalDelivery::Sent) => true,
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
    let status = match leader_status {
        Some(status) => status,
        None => retained_result(
            process.wait(),
            stdout_path,
            stderr_path,
            Some(resource_path),
        )?,
    };
    if let Err(error) = confirm_process_group_absent(process_group, kill_confirmation_timeout) {
        return Err(retained_error(
            error,
            stdout_path,
            stderr_path,
            Some(resource_path),
        ));
    }
    process.mark_target_absent();
    Ok(TimeoutTermination {
        status,
        timed_out: true,
        term_signal_sent,
        kill_signal_sent,
    })
}

#[cfg(test)]
pub(super) fn cleanup_error(
    error: impl fmt::Display,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Box<dyn Error> {
    retained_error(error, stdout_path, stderr_path, None)
}

pub(super) fn retained_error(
    error: impl fmt::Display,
    stdout_path: &Path,
    stderr_path: &Path,
    telemetry_path: Option<&Path>,
) -> Box<dyn Error> {
    Box::new(ProcessCleanupError {
        detail: error.to_string(),
        stdout_path: stdout_path.to_owned(),
        stderr_path: stderr_path.to_owned(),
        telemetry_path: telemetry_path.map(Path::to_owned),
    })
}

pub(super) fn measurement_error(
    error: impl fmt::Display,
    stdout_path: &Path,
    stderr_path: &Path,
    telemetry_path: &Path,
) -> Box<dyn Error> {
    retained_error(error, stdout_path, stderr_path, Some(telemetry_path))
}

pub(super) fn retained_result<T, E: fmt::Display>(
    result: Result<T, E>,
    stdout_path: &Path,
    stderr_path: &Path,
    telemetry_path: Option<&Path>,
) -> Result<T, Box<dyn Error>> {
    result.map_err(|error| retained_error(error, stdout_path, stderr_path, telemetry_path))
}

#[cfg(unix)]
fn process_group_pid(pid: u32) -> Result<Pid, Box<dyn Error>> {
    let pid = i32::try_from(pid).map_err(|_| format!("process group ID exceeds i32: {pid}"))?;
    Pid::from_raw(pid).ok_or_else(|| format!("process group ID must be positive: {pid}").into())
}

#[cfg(unix)]
pub(super) fn classify_signal_delivery(result: Result<(), Errno>) -> Result<SignalDelivery, Errno> {
    match result {
        Ok(()) => Ok(SignalDelivery::Sent),
        Err(Errno::SRCH) => Ok(SignalDelivery::GroupAbsent),
        Err(error) => Err(error),
    }
}

pub(super) fn process_group_state(pid: u32) -> Result<ProcessGroupState, Box<dyn Error>> {
    Ok(process_group_observation(pid)?.state)
}

#[cfg(unix)]
pub(super) fn signal_process_group(
    pid: u32,
    signal: ProcessSignal,
) -> Result<SignalDelivery, Box<dyn Error>> {
    let process_group = process_group_pid(pid)?;
    let unix_signal = match signal {
        ProcessSignal::Term => Signal::TERM,
        ProcessSignal::Kill => Signal::KILL,
    };
    let signal_name = match signal {
        ProcessSignal::Term => "SIGTERM",
        ProcessSignal::Kill => "SIGKILL",
    };
    classify_signal_delivery(kill_process_group(process_group, unix_signal))
        .map_err(|error| format!("send {signal_name} to process group {pid}: {error}").into())
}

#[cfg(not(unix))]
pub(super) fn signal_process_group(
    _pid: u32,
    _signal: ProcessSignal,
) -> Result<SignalDelivery, Box<dyn Error>> {
    Err("process-group cleanup requires Unix".into())
}

pub(super) fn confirm_process_group_absent(
    pid: u32,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    confirm_process_group_absent_with(timeout, || process_group_state(pid)).map_err(|error| {
        format!("confirm process group {pid} exited after SIGKILL: {error}").into()
    })
}

pub(super) fn confirm_process_group_absent_with(
    timeout: Duration,
    mut probe: impl FnMut() -> Result<ProcessGroupState, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    loop {
        match probe()? {
            ProcessGroupState::Absent => return Ok(()),
            ProcessGroupState::Alive if started.elapsed() >= timeout => {
                return Err(format!(
                    "process group remained alive for {} ms",
                    duration_ms(timeout)
                )
                .into());
            }
            ProcessGroupState::Alive => std::thread::sleep(PROCESS_POLL_INTERVAL),
        }
    }
}
