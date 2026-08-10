//! Provenance-bound adaptation from producer policy to neutral process execution.

use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path, time::Duration};

#[cfg(unix)]
use std::os::fd::BorrowedFd;

use crate::execution::process::ProcessDeadlines;

use super::{bind_invocation, bind_process_output, ProcessOutput, ProcessPolicy, ProcessSchedule};

pub(in crate::producer) fn timed_with_timeout(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_with_timeout_and_policy(
        program,
        arguments,
        environment,
        current_dir,
        timeout,
        ProcessPolicy::default(),
    )
}

#[cfg(test)]
pub(in crate::producer) fn timed_with_timeout_after_stdout_ready(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    execution_timeout: Duration,
    readiness_prefix: &[u8],
    startup_timeout: Duration,
) -> Result<ProcessOutput, Box<dyn Error>> {
    let readiness = crate::execution::process::await_next_target_stdout_prefix(
        readiness_prefix,
        startup_timeout,
    );
    let result = timed_with_timeout_and_policy_and_descriptors_after_bind(
        program,
        arguments,
        environment,
        current_dir,
        ProcessSchedule::standalone_after_readiness(
            execution_timeout,
            startup_timeout,
            ProcessPolicy::default(),
        )?,
        &[],
        || {},
        || {},
    );
    drop(readiness);
    result
}

pub(super) fn timed_with_timeout_and_policy(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    policy: ProcessPolicy,
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_with_schedule_and_descriptors(
        program,
        arguments,
        environment,
        current_dir,
        ProcessSchedule::standalone(timeout, policy)?,
        &[],
    )
}

pub(super) fn timed_with_schedule_and_descriptors(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    schedule: ProcessSchedule,
    inherited_descriptors: &[BorrowedFd<'_>],
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_with_timeout_and_policy_and_descriptors_after_bind(
        program,
        arguments,
        environment,
        current_dir,
        schedule,
        inherited_descriptors,
        || {},
        || {},
    )
}

#[cfg(test)]
pub(super) fn timed_with_timeout_and_policy_and_descriptors(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    policy: ProcessPolicy,
    inherited_descriptors: &[BorrowedFd<'_>],
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_with_schedule_and_descriptors(
        program,
        arguments,
        environment,
        current_dir,
        ProcessSchedule::standalone(timeout, policy)?,
        inherited_descriptors,
    )
}

#[cfg(test)]
pub(super) fn timed_with_timeout_after_bind(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    after_bind: impl FnOnce(),
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_with_timeout_and_policy_and_descriptors_after_bind(
        program,
        arguments,
        environment,
        current_dir,
        ProcessSchedule::standalone(timeout, ProcessPolicy::default())?,
        &[],
        after_bind,
        || {},
    )
}

#[cfg(test)]
pub(super) fn timed_with_timeout_after_run(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    after_run: impl FnOnce(),
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_with_timeout_and_policy_and_descriptors_after_bind(
        program,
        arguments,
        environment,
        current_dir,
        ProcessSchedule::standalone(timeout, ProcessPolicy::default())?,
        &[],
        || {},
        after_run,
    )
}

#[allow(clippy::too_many_arguments)]
fn timed_with_timeout_and_policy_and_descriptors_after_bind(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    schedule: ProcessSchedule,
    inherited_descriptors: &[BorrowedFd<'_>],
    after_bind: impl FnOnce(),
    after_run: impl FnOnce(),
) -> Result<ProcessOutput, Box<dyn Error>> {
    require_sound_process_execution()?;
    let invocation_binding = bind_invocation(program, arguments, environment, current_dir)?;
    let invocation = invocation_binding.receipt().clone();
    after_bind();
    invocation_binding.verify_path_bindings()?;
    let deadlines = ProcessDeadlines::new(
        schedule.execution_timeout,
        schedule.execution_window_deadline,
        schedule.cleanup_start_deadline,
        schedule.finalization_start_deadline,
        schedule.lifecycle_deadline,
    )?;
    let output = invocation_binding.run(
        environment,
        deadlines,
        schedule.policy.termination(),
        schedule.policy.finalization(),
        inherited_descriptors,
    )?;
    after_run();
    if let Err(error) = invocation_binding.verify_path_bindings() {
        return Err(output.retained_error(error));
    }
    let output = output.finalize()?;
    Ok(bind_process_output(invocation, output))
}

fn require_sound_process_execution() -> Result<(), Box<dyn Error>> {
    process_execution_policy(cfg!(target_os = "linux"), cfg!(test))
}

pub(super) fn process_execution_policy(
    descriptor_execution_supported: bool,
    test_only_fallback: bool,
) -> Result<(), Box<dyn Error>> {
    if descriptor_execution_supported || test_only_fallback {
        Ok(())
    } else {
        Err("evidence subprocess execution requires Linux descriptor-based executable launch; this host cannot atomically bind a hash to exec".into())
    }
}
