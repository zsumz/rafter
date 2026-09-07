//! Bounded execution and output draining for internal process observers.

mod collect;
#[cfg(test)]
mod test_support;

use std::{
    error::Error,
    io::Read,
    path::Path,
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
    base_environment, duration_ms, spawn_leased_child, CleanupFailures, ManagedInternalProcess,
    NoSignalReaper, ProcessLeaseState, RuntimeExecutable,
};

use collect::{collect_internal_output, finalize_internal_output};

#[cfg(test)]
pub(crate) use test_support::{
    await_next_internal_completion_after_deadline, bounded_internal_output,
    bounded_internal_output_with_reaper, inject_next_internal_drain_error,
    inject_next_internal_drain_errors,
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
        None,
        execution_deadline,
        lifecycle_deadline,
        reaper,
    )
}

pub(super) fn bounded_identity_output(
    program: &str,
    arguments: &[&str],
    current_dir: &Path,
    timeout: Duration,
) -> Result<std::process::Output, Box<dyn Error>> {
    let started = Instant::now();
    let execution_deadline = started + timeout;
    let lifecycle_deadline = execution_deadline + Duration::from_secs(5);
    bounded_internal_output_from(
        program,
        None,
        arguments,
        Some(current_dir),
        execution_deadline,
        lifecycle_deadline,
        NoSignalReaper::start()?,
    )
}

fn bounded_internal_output_from(
    program: &str,
    runtime: Option<RuntimeExecutable<'_>>,
    arguments: &[&str],
    current_dir: Option<&Path>,
    execution_deadline: Instant,
    lifecycle_deadline: Instant,
    reaper: NoSignalReaper,
) -> Result<std::process::Output, Box<dyn Error>> {
    let request = InternalCommandRequest {
        program,
        runtime,
        arguments,
        current_dir,
        execution_deadline,
        lifecycle_deadline,
    };
    let cleanup_failures = CleanupFailures::default();
    let result = bounded_internal_output_owned(request, cleanup_failures.clone(), reaper);
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

#[derive(Clone, Copy)]
struct InternalCommandRequest<'a> {
    program: &'a str,
    runtime: Option<RuntimeExecutable<'a>>,
    arguments: &'a [&'a str],
    current_dir: Option<&'a Path>,
    execution_deadline: Instant,
    lifecycle_deadline: Instant,
}

fn bounded_internal_output_owned(
    request: InternalCommandRequest<'_>,
    cleanup_failures: CleanupFailures,
    reaper: NoSignalReaper,
) -> Result<std::process::Output, Box<dyn Error>> {
    let (mut process, mut stdout, mut stderr) = spawn_internal_process(
        request.program,
        request.runtime,
        request.arguments,
        request.current_dir,
        request.lifecycle_deadline,
        cleanup_failures,
        reaper,
    )?;
    set_nonblocking(&stdout)
        .map_err(|error| format!("make internal stdout nonblocking: {error}"))?;
    set_nonblocking(&stderr)
        .map_err(|error| format!("make internal stderr nonblocking: {error}"))?;
    let collected = collect_internal_output(&mut process, &mut stdout, &mut stderr, request)?;
    process.disarm()?;
    finalize_internal_output(request.program, collected)
}

fn spawn_internal_process(
    program: &str,
    runtime: Option<RuntimeExecutable<'_>>,
    arguments: &[&str],
    current_dir: Option<&Path>,
    lifecycle_deadline: Instant,
    cleanup_failures: CleanupFailures,
    reaper: NoSignalReaper,
) -> Result<(ManagedInternalProcess, ChildStdout, ChildStderr), Box<dyn Error>> {
    let (child, lifetime) = spawn_leased_child(|lifetime_writer| {
        let mut command = Command::new(program);
        command
            .args(arguments)
            .env_clear()
            .envs(base_environment())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(current_dir) = current_dir {
            command.current_dir(current_dir);
        }
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
        #[cfg(not(unix))]
        let _ = lifetime_writer;
        Ok(command)
    })
    .map_err(|error| format!("spawn internal command {program}: {error}"))?;
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
