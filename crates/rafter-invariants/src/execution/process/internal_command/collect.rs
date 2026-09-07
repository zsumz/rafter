//! Deadline-bounded collection of internal process output and exit status.
//!
//! Polls the drains and the lease together so the observed exit, the released
//! lineage, and both EOFs are one decision, then converts the collected state
//! into the output or the timeout the caller sees.

use std::{
    error::Error,
    process::{ChildStderr, ChildStdout},
    time::Instant,
};

use super::{
    drain_nonblocking, duration_ms, InternalCommandRequest, ManagedInternalProcess,
    ProcessLeaseState, INTERNAL_OUTPUT_MAX_BYTES, INTERNAL_POLL_INTERVAL,
};

#[cfg(test)]
use super::test_support;

pub(super) struct CollectedInternalOutput {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    timed_out: bool,
    overflowed: bool,
    started: Instant,
}

pub(super) fn collect_internal_output(
    process: &mut ManagedInternalProcess,
    stdout: &mut ChildStdout,
    stderr: &mut ChildStderr,
    request: InternalCommandRequest<'_>,
) -> Result<CollectedInternalOutput, Box<dyn Error>> {
    let started = Instant::now();
    let mut cleanup_deadline = None;
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    let mut stdout_eof = false;
    let mut stderr_eof = false;
    let mut timed_out = false;
    let mut overflowed = false;
    let status = loop {
        let active_deadline = cleanup_deadline.unwrap_or(request.execution_deadline);
        let mut drain_reached_deadline = false;
        if !stdout_eof {
            let drained = drain_nonblocking(stdout, &mut stdout_bytes, active_deadline)?;
            stdout_eof = drained.eof;
            drain_reached_deadline |= drained.deadline_reached;
        }
        if !stderr_eof && !drain_reached_deadline {
            let drained = drain_nonblocking(stderr, &mut stderr_bytes, active_deadline)?;
            stderr_eof = drained.eof;
            drain_reached_deadline |= drained.deadline_reached;
        }
        overflowed |= stdout_bytes.len() > INTERNAL_OUTPUT_MAX_BYTES
            || stderr_bytes.len() > INTERNAL_OUTPUT_MAX_BYTES;
        #[cfg(test)]
        test_support::await_completion_boundary_if_requested(
            process,
            request.execution_deadline,
            request.lifecycle_deadline,
        )?;
        let exited = process.exit_observed()?;
        let lineage_released = process.lifetime_state()? == ProcessLeaseState::Released;
        let mut complete = exited && lineage_released && stdout_eof && stderr_eof;
        let now = Instant::now();
        if cleanup_deadline.is_none()
            && (now >= request.execution_deadline || drain_reached_deadline || overflowed)
        {
            timed_out = now >= request.execution_deadline;
            if exited && !complete {
                if !stdout_eof {
                    stdout_eof =
                        drain_nonblocking(stdout, &mut stdout_bytes, request.lifecycle_deadline)?
                            .eof;
                }
                if !stderr_eof {
                    stderr_eof =
                        drain_nonblocking(stderr, &mut stderr_bytes, request.lifecycle_deadline)?
                            .eof;
                }
                overflowed |= stdout_bytes.len() > INTERNAL_OUTPUT_MAX_BYTES
                    || stderr_bytes.len() > INTERNAL_OUTPUT_MAX_BYTES;
                complete = lineage_released && stdout_eof && stderr_eof;
            }
            if !complete {
                let _delivery = process.signal_kill()?;
            }
            cleanup_deadline = Some(request.lifecycle_deadline);
        }
        if complete {
            let acceptance_deadline = cleanup_deadline.unwrap_or(request.execution_deadline);
            let status =
                accept_internal_completion(process, request.program, now, acceptance_deadline)?;
            break status;
        }
        if cleanup_deadline.is_some_and(|deadline| now >= deadline) {
            return Err(format!(
                "internal command {} did not close its process group and output within {} ms after kill",
                request.program,
                duration_ms(started.elapsed())
            )
            .into());
        }
        let deadline = cleanup_deadline.unwrap_or(request.execution_deadline);
        std::thread::sleep(INTERNAL_POLL_INTERVAL.min(deadline.saturating_duration_since(now)));
    };
    Ok(CollectedInternalOutput {
        status,
        stdout: stdout_bytes,
        stderr: stderr_bytes,
        timed_out,
        overflowed,
        started,
    })
}

pub(super) fn finalize_internal_output(
    program: &str,
    collected: CollectedInternalOutput,
) -> Result<std::process::Output, Box<dyn Error>> {
    if collected.overflowed {
        return Err(format!(
            "internal command {program} exceeded the {INTERNAL_OUTPUT_MAX_BYTES}-byte output limit"
        )
        .into());
    }
    if collected.timed_out {
        return Err(format!(
            "internal command {program} timed out after {} ms; stdout: {}; stderr: {}",
            duration_ms(collected.started.elapsed()),
            String::from_utf8_lossy(&collected.stdout).trim(),
            String::from_utf8_lossy(&collected.stderr).trim()
        )
        .into());
    }
    Ok(std::process::Output {
        status: collected.status,
        stdout: collected.stdout,
        stderr: collected.stderr,
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
