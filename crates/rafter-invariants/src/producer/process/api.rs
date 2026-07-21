//! Budget-aware process and identity-command entry points for evidence producers.

use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path, time::Duration};

#[cfg(unix)]
use std::os::fd::BorrowedFd;

use super::timed_with_schedule_and_descriptors;
use super::{
    active_process_timeout, active_total_process_timeout, base_environment,
    has_active_layer_budget, timed_with_timeout_and_policy, IdentityOutput, ProcessKind,
    ProcessOutput, ProcessPolicy,
};

const IDENTITY_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

pub(in crate::producer) fn timed_for(
    kind: ProcessKind,
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_for_with_cap(kind, program, arguments, environment, current_dir, None)
}

pub(in crate::producer) fn timed_for_with_cap(
    kind: ProcessKind,
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    requested_cap: Option<Duration>,
) -> Result<ProcessOutput, Box<dyn Error>> {
    let schedule = active_process_timeout(kind, requested_cap)?;
    timed_with_schedule_and_descriptors(program, arguments, environment, current_dir, schedule, &[])
}

#[cfg(unix)]
pub(in crate::producer) fn timed_for_with_cap_and_descriptors(
    kind: ProcessKind,
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    requested_cap: Option<Duration>,
    inherited_descriptors: &[BorrowedFd<'_>],
) -> Result<ProcessOutput, Box<dyn Error>> {
    let schedule = active_process_timeout(kind, requested_cap)?;
    timed_with_schedule_and_descriptors(
        program,
        arguments,
        environment,
        current_dir,
        schedule,
        inherited_descriptors,
    )
}

pub(in crate::producer) fn timed_with_optional_layer_budget(
    kind: ProcessKind,
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    requested_cap: Duration,
) -> Result<ProcessOutput, Box<dyn Error>> {
    if has_active_layer_budget() {
        return timed_for_with_cap(
            kind,
            program,
            arguments,
            environment,
            current_dir,
            Some(requested_cap),
        );
    }
    super::timed_with_timeout(program, arguments, environment, current_dir, requested_cap)
}

pub(crate) fn identity_command_in(
    program: &str,
    arguments: &[&str],
    current_dir: &Path,
) -> Result<IdentityOutput, Box<dyn Error>> {
    identity_command_with_timeout_in(
        program,
        arguments,
        current_dir,
        IDENTITY_COMMAND_TIMEOUT,
        false,
    )
}

pub(crate) fn identity_command_in_total_budget(
    program: &str,
    arguments: &[&str],
    current_dir: &Path,
) -> Result<IdentityOutput, Box<dyn Error>> {
    identity_command_with_timeout_in(
        program,
        arguments,
        current_dir,
        IDENTITY_COMMAND_TIMEOUT,
        true,
    )
}

#[cfg(test)]
pub(super) fn identity_command_with_timeout(
    program: &str,
    arguments: &[&str],
    timeout: Duration,
) -> Result<IdentityOutput, Box<dyn Error>> {
    identity_command_with_timeout_in(program, arguments, Path::new("."), timeout, false)
}

fn identity_command_with_timeout_in(
    program: &str,
    arguments: &[&str],
    current_dir: &Path,
    timeout: Duration,
    use_total_budget: bool,
) -> Result<IdentityOutput, Box<dyn Error>> {
    let arguments = arguments.iter().map(OsString::from).collect::<Vec<_>>();
    let environment = base_environment();
    if has_active_layer_budget() {
        let schedule = if use_total_budget {
            active_total_process_timeout(ProcessKind::Identity, Some(timeout))?
        } else {
            active_process_timeout(ProcessKind::Identity, Some(timeout))?
        };
        let output = timed_with_schedule_and_descriptors(
            program,
            &arguments,
            &environment,
            current_dir,
            schedule,
            &[],
        )?;
        return identity_output(program, output);
    }
    let output = timed_with_timeout_and_policy(
        program,
        &arguments,
        &environment,
        current_dir,
        timeout,
        ProcessPolicy::default(),
    )?;
    identity_output(program, output)
}

fn identity_output(program: &str, output: ProcessOutput) -> Result<IdentityOutput, Box<dyn Error>> {
    let stdout = String::from_utf8(output.stdout)?;
    let stderr = String::from_utf8(output.stderr)?;
    if output.timed_out || !output.status.success() {
        return Err(format!(
            "bounded identity command {program} failed with {:?} (timed_out={}): stdout: {}; stderr: {}",
            output.status.code(),
            output.timed_out,
            stdout.trim(),
            stderr.trim(),
        )
        .into());
    }
    Ok(IdentityOutput { stdout, stderr })
}
