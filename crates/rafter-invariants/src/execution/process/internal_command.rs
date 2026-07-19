//! Bounded execution and output draining for internal process observers.

#[cfg(test)]
mod test_support;

use std::{
    error::Error,
    io::Read,
    process::{ChildStderr, ChildStdout, Command, Stdio},
    time::{Duration, Instant},
};

#[cfg(unix)]
use command_fds::{CommandFdExt, FdMapping};
#[cfg(unix)]
use std::os::{
    fd::{AsFd, AsRawFd},
    unix::process::CommandExt,
};

use super::{
    base_environment, duration_ms, CleanupFailures, ManagedInternalProcess, NoSignalReaper,
    ProcessLeaseState, ProcessLifetimeLease, RuntimeExecutable,
};

#[cfg(test)]
pub(crate) use test_support::{
    bounded_internal_output, bounded_internal_output_with_cleanup,
    delay_next_internal_completion_check, inject_next_internal_drain_error,
};

const INTERNAL_OUTPUT_MAX_BYTES: usize = 16 * 1024 * 1024;
const INTERNAL_POLL_INTERVAL: Duration = Duration::from_millis(1);

pub(super) fn bounded_internal_output_with_runtime(
    program: RuntimeExecutable<'_>,
    arguments: &[&str],
    execution_deadline: Instant,
    lifecycle_deadline: Instant,
    reaper: NoSignalReaper,
) -> Result<std::process::Output, Box<dyn Error>> {
    bounded_internal_output_from(
        &program.path.to_string_lossy(),
        Some(program),
        arguments,
        execution_deadline,
        lifecycle_deadline,
        reaper,
    )
}

fn bounded_internal_output_from(
    program: &str,
    runtime: Option<RuntimeExecutable<'_>>,
    arguments: &[&str],
    execution_deadline: Instant,
    lifecycle_deadline: Instant,
    reaper: NoSignalReaper,
) -> Result<std::process::Output, Box<dyn Error>> {
    let cleanup_failures = CleanupFailures::default();
    let result = bounded_internal_output_owned(
        program,
        runtime,
        arguments,
        execution_deadline,
        lifecycle_deadline,
        cleanup_failures.clone(),
        reaper,
    );
    let failures = cleanup_failures.take();
    if failures.is_empty() {
        return result;
    }
    let cleanup = format!(
        "fallback subprocess cleanup failed: {}",
        failures.join("; ")
    );
    match result {
        Ok(_) => Err(cleanup.into()),
        Err(error) => Err(format!("{error}; {cleanup}").into()),
    }
}

fn bounded_internal_output_owned(
    program: &str,
    runtime: Option<RuntimeExecutable<'_>>,
    arguments: &[&str],
    execution_deadline: Instant,
    lifecycle_deadline: Instant,
    cleanup_failures: CleanupFailures,
    reaper: NoSignalReaper,
) -> Result<std::process::Output, Box<dyn Error>> {
    let (mut process, mut stdout, mut stderr) = spawn_internal_process(
        program,
        runtime,
        arguments,
        lifecycle_deadline,
        cleanup_failures,
        reaper,
    )?;
    set_nonblocking(&stdout)
        .map_err(|error| format!("make internal stdout nonblocking: {error}"))?;
    set_nonblocking(&stderr)
        .map_err(|error| format!("make internal stderr nonblocking: {error}"))?;
    let started = Instant::now();
    let mut cleanup_deadline = None;
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    let mut stdout_eof = false;
    let mut stderr_eof = false;
    let mut timed_out = false;
    let mut overflowed = false;
    let status = loop {
        let active_deadline = cleanup_deadline.unwrap_or(execution_deadline);
        let mut drain_reached_deadline = false;
        if !stdout_eof {
            let drained = drain_nonblocking(&mut stdout, &mut stdout_bytes, active_deadline)?;
            stdout_eof = drained.eof;
            drain_reached_deadline |= drained.deadline_reached;
        }
        if !stderr_eof && !drain_reached_deadline {
            let drained = drain_nonblocking(&mut stderr, &mut stderr_bytes, active_deadline)?;
            stderr_eof = drained.eof;
            drain_reached_deadline |= drained.deadline_reached;
        }
        overflowed |= stdout_bytes.len() > INTERNAL_OUTPUT_MAX_BYTES
            || stderr_bytes.len() > INTERNAL_OUTPUT_MAX_BYTES;
        #[cfg(test)]
        test_support::delay_completion_if_requested();
        let exited = process.exit_observed()?;
        let lineage_released = process.lifetime_state()? == ProcessLeaseState::Released;
        let mut complete = exited && lineage_released && stdout_eof && stderr_eof;
        let now = Instant::now();
        if cleanup_deadline.is_none()
            && (now >= execution_deadline || drain_reached_deadline || overflowed)
        {
            timed_out = now >= execution_deadline;
            if exited && !complete {
                if !stdout_eof {
                    stdout_eof =
                        drain_nonblocking(&mut stdout, &mut stdout_bytes, lifecycle_deadline)?.eof;
                }
                if !stderr_eof {
                    stderr_eof =
                        drain_nonblocking(&mut stderr, &mut stderr_bytes, lifecycle_deadline)?.eof;
                }
                overflowed |= stdout_bytes.len() > INTERNAL_OUTPUT_MAX_BYTES
                    || stderr_bytes.len() > INTERNAL_OUTPUT_MAX_BYTES;
                complete = lineage_released && stdout_eof && stderr_eof;
            }
            if !complete {
                let _delivery = process.signal_kill()?;
            }
            cleanup_deadline = Some(lifecycle_deadline);
        }
        if complete {
            let acceptance_deadline = cleanup_deadline.unwrap_or(execution_deadline);
            let status =
                accept_internal_completion(&mut process, program, now, acceptance_deadline)?;
            break status;
        }
        if cleanup_deadline.is_some_and(|deadline| now >= deadline) {
            return Err(format!(
                "internal command {program} did not close its process group and output within {} ms after kill",
                duration_ms(started.elapsed())
            )
            .into());
        }
        let deadline = cleanup_deadline.unwrap_or(execution_deadline);
        std::thread::sleep(INTERNAL_POLL_INTERVAL.min(deadline.saturating_duration_since(now)));
    };
    process.disarm()?;
    if overflowed {
        return Err(format!(
            "internal command {program} exceeded the {INTERNAL_OUTPUT_MAX_BYTES}-byte output limit"
        )
        .into());
    }
    if timed_out {
        return Err(format!(
            "internal command {program} timed out after {} ms; stdout: {}; stderr: {}",
            duration_ms(started.elapsed()),
            String::from_utf8_lossy(&stdout_bytes).trim(),
            String::from_utf8_lossy(&stderr_bytes).trim()
        )
        .into());
    }
    Ok(std::process::Output {
        status,
        stdout: stdout_bytes,
        stderr: stderr_bytes,
    })
}

fn accept_internal_completion(
    process: &mut ManagedInternalProcess,
    program: &str,
    observed_at: Instant,
    deadline: Instant,
) -> Result<std::process::ExitStatus, Box<dyn Error>> {
    if observed_at >= deadline {
        return Err(
            format!("internal command {program} completed after its absolute deadline").into(),
        );
    }
    process
        .try_wait()?
        .ok_or_else(|| "observed internal command exit was not waitable".into())
}

fn spawn_internal_process(
    program: &str,
    runtime: Option<RuntimeExecutable<'_>>,
    arguments: &[&str],
    lifecycle_deadline: Instant,
    cleanup_failures: CleanupFailures,
    reaper: NoSignalReaper,
) -> Result<(ManagedInternalProcess, ChildStdout, ChildStderr), Box<dyn Error>> {
    let mut command = Command::new(program);
    command
        .args(arguments)
        .env_clear()
        .envs(base_environment())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let (lifetime, lifetime_writer) = ProcessLifetimeLease::create()?;
    #[cfg(unix)]
    {
        command.process_group(0);
        let mut mappings = vec![FdMapping {
            parent_fd: lifetime_writer.as_fd().try_clone_to_owned()?,
            child_fd: lifetime_writer.as_raw_fd(),
        }];
        if let Some(runtime) = runtime {
            mappings.push(FdMapping {
                parent_fd: runtime.descriptor.try_clone_to_owned()?,
                child_fd: runtime.descriptor.as_raw_fd(),
            });
        }
        command.fd_mappings(mappings)?;
    }
    let child = command
        .spawn()
        .map_err(|error| format!("spawn internal command {program}: {error}"))?;
    drop(lifetime_writer);
    let mut process = ManagedInternalProcess::new(
        child,
        lifecycle_deadline,
        cleanup_failures,
        reaper,
        lifetime,
    );
    let stdout = process
        .take_stdout()
        .ok_or("internal command omitted stdout")?;
    let stderr = process
        .take_stderr()
        .ok_or("internal command omitted stderr")?;
    Ok((process, stdout, stderr))
}

fn set_nonblocking(descriptor: &impl std::os::fd::AsFd) -> Result<(), Box<dyn Error>> {
    let flags = rustix::fs::fcntl_getfl(descriptor)?;
    rustix::fs::fcntl_setfl(descriptor, flags | rustix::fs::OFlags::NONBLOCK)?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DrainState {
    eof: bool,
    deadline_reached: bool,
}

fn drain_nonblocking(
    reader: &mut impl Read,
    bytes: &mut Vec<u8>,
    deadline: Instant,
) -> Result<DrainState, Box<dyn Error>> {
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        if Instant::now() >= deadline {
            return Ok(DrainState {
                eof: false,
                deadline_reached: true,
            });
        }
        match reader.read(buffer.as_mut()) {
            Ok(0) => {
                return Ok(DrainState {
                    eof: true,
                    deadline_reached: false,
                })
            }
            Ok(read) => {
                let retained = (INTERNAL_OUTPUT_MAX_BYTES + 1)
                    .saturating_sub(bytes.len())
                    .min(read);
                bytes.extend_from_slice(&buffer[..retained]);
                #[cfg(test)]
                test_support::inject_drain_error_if_requested()?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(DrainState {
                    eof: false,
                    deadline_reached: false,
                })
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
}
