//! Descriptor-bound process-group launch and bounded collection orchestration.

mod program;

use std::{
    error::Error,
    io::PipeWriter,
    os::unix::net::UnixStream,
    process::{Child, Command, Stdio},
    time::Instant,
};

#[cfg(unix)]
use command_fds::{CommandFdExt, FdMapping};
#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
#[cfg(unix)]
use std::os::unix::process::CommandExt;

use super::{
    base_environment,
    output::{collect_process_output, finish_managed_process},
    retained_error, spawn_leased_child, CleanupFailures, ManagedProcess, NoSignalReaper,
    PendingProcessOutput, ProcessArtifacts, ProcessGroupAnchor, ProcessObserver, ProcessRequest,
    TargetLifetimeLease,
};
#[cfg(test)]
pub(crate) use program::expose_next_target_lifetime_lease;
use program::{
    validate_target_environment, INHERITED_FDS_ENV, INHERITED_FD_MAX_ENV, RESOURCE_FD_ENV,
    RESOURCE_WRAPPER, TARGET_GROUP_ACK_FD_ENV, TARGET_GROUP_FD_ENV, TARGET_GROUP_ID_ENV,
    TARGET_GROUP_LAUNCHER, TARGET_LIFETIME_LEASE_FD_ENV, WORKING_DIRECTORY_FD_ENV,
};

pub(crate) fn run(request: &ProcessRequest<'_>) -> Result<PendingProcessOutput, Box<dyn Error>> {
    validate_target_environment(request.environment)?;
    let started = Instant::now();
    let reaper = NoSignalReaper::start()?;
    let cleanup_failures = CleanupFailures::default();
    let artifacts = ProcessArtifacts::allocate()?;
    let (mut target_group_ack, child_target_group_ack) = UnixStream::pair()?;
    let stdout_path = artifacts.stdout_path();
    let stderr_path = artifacts.stderr_path();
    let resource_path = artifacts.resource_path();
    let observer = ProcessObserver::capture(request.runtime.observer, reaper.clone())?;
    let anchor_readiness_deadline = Instant::now()
        .checked_add(request.termination.publication_timeout)
        .ok_or("process-group anchor readiness deadline overflow")?
        .min(request.deadlines.execution_window);
    let anchor_stderr = artifacts
        .stderr_file()
        .map_err(|error| retained_error(error, &stdout_path, &stderr_path, Some(&resource_path)))?;
    let mut anchor = ProcessGroupAnchor::spawn(
        request.runtime.perl,
        anchor_readiness_deadline,
        reaper.clone(),
        Stdio::from(anchor_stderr),
    )
    .map_err(|error| retained_error(error, &stdout_path, &stderr_path, Some(&resource_path)))?;
    let target_group = anchor.id();
    let (child, target_lifetime_lease) = match spawn_resource_wrapper(
        request,
        &artifacts,
        &child_target_group_ack,
        target_group,
    ) {
        Ok(spawned) => spawned,
        Err(error) => {
            let release = anchor.release(request.deadlines.cleanup_start);
            let detail = match release {
                Ok(status) if status.success() => error.to_string(),
                Ok(status) => format!(
                    "{error}; empty process-group anchor exited {:?} during launch rollback",
                    status.code()
                ),
                Err(cleanup) => {
                    format!("{error}; release empty process-group anchor during launch rollback: {cleanup}")
                }
            };
            return Err(retained_error(
                detail,
                &stdout_path,
                &stderr_path,
                Some(&resource_path),
            ));
        }
    };
    drop(child_target_group_ack);
    let mut process = ManagedProcess::new(
        child,
        anchor,
        request.deadlines.finalization_start,
        request.termination.kill_confirmation_timeout,
        cleanup_failures.clone(),
        Some(observer),
        reaper,
        target_lifetime_lease,
    );
    let result = collect_process_output(
        &mut process,
        &mut target_group_ack,
        started,
        request.deadlines,
        request.termination,
        request.finalization,
        &artifacts,
    );
    let result = result.and_then(|output| {
        artifacts.verify_path_bindings().map_err(|error| {
            retained_error(error, &stdout_path, &stderr_path, Some(&resource_path))
        })?;
        Ok(PendingProcessOutput::new(output, artifacts))
    });
    finish_managed_process(
        process,
        result,
        request.deadlines,
        &stdout_path,
        &stderr_path,
        &resource_path,
        &cleanup_failures,
    )
}

/// Spawn the resource wrapper and the lease that outlives its target lineage.
///
/// The lease is created here, and nowhere earlier, because its writer is
/// inheritable by any fork this process performs while it is open. Building the
/// command inside `spawn_leased_child` keeps that window to this one spawn.
fn spawn_resource_wrapper(
    request: &ProcessRequest<'_>,
    artifacts: &ProcessArtifacts,
    child_target_group_ack: &UnixStream,
    target_group: u32,
) -> Result<(Child, TargetLifetimeLease), Box<dyn Error>> {
    spawn_leased_child(|target_lifetime_writer| {
        build_resource_wrapper(
            request,
            artifacts,
            child_target_group_ack,
            target_lifetime_writer,
            target_group,
        )
    })
}

fn build_resource_wrapper(
    request: &ProcessRequest<'_>,
    artifacts: &ProcessArtifacts,
    child_target_group_ack: &UnixStream,
    target_lifetime_writer: &PipeWriter,
    target_group: u32,
) -> Result<Command, Box<dyn Error>> {
    let mut command = Command::new(request.runtime.perl.path);
    command
        .arg("-MPOSIX")
        .arg("-e")
        .arg(RESOURCE_WRAPPER)
        .arg(request.runtime.time.path);
    if cfg!(target_os = "macos") {
        command.arg("-l");
    } else if cfg!(target_os = "linux") {
        command.arg("-v");
    } else {
        return Err("peak RSS collection supports macOS and Linux".into());
    }
    let mut launcher_environment = base_environment();
    launcher_environment.insert(
        RESOURCE_FD_ENV.to_owned(),
        artifacts.resource_descriptor().as_raw_fd().to_string(),
    );
    launcher_environment.insert(
        TARGET_GROUP_FD_ENV.to_owned(),
        artifacts.process_group_descriptor().as_raw_fd().to_string(),
    );
    launcher_environment.insert(
        TARGET_GROUP_ACK_FD_ENV.to_owned(),
        child_target_group_ack.as_raw_fd().to_string(),
    );
    launcher_environment.insert(TARGET_GROUP_ID_ENV.to_owned(), target_group.to_string());
    launcher_environment.insert(
        TARGET_LIFETIME_LEASE_FD_ENV.to_owned(),
        target_lifetime_writer.as_raw_fd().to_string(),
    );
    #[cfg(unix)]
    let child_descriptors = child_descriptors(
        request,
        artifacts,
        child_target_group_ack.as_fd(),
        target_lifetime_writer.as_fd(),
        &mut launcher_environment,
    );
    let target_environment =
        program::target_environment(request.environment, target_lifetime_writer.as_raw_fd());
    command
        .arg(request.runtime.perl.path)
        .arg("-MPOSIX")
        .arg("-e")
        .arg(TARGET_GROUP_LAUNCHER)
        .arg(request.executable_path)
        .arg(request.program)
        .arg(target_environment.len().to_string())
        .args(
            target_environment
                .iter()
                .flat_map(|(name, value)| [name, value]),
        )
        .args(request.arguments)
        .env_clear()
        .envs(&launcher_environment)
        .stdout(Stdio::from(artifacts.stdout_file()?))
        .stderr(Stdio::from(artifacts.stderr_file()?));
    #[cfg(unix)]
    command.process_group(0);
    artifacts.verify_path_bindings()?;
    #[cfg(unix)]
    bind_child_descriptors(&mut command, &child_descriptors)?;
    Ok(command)
}

#[cfg(unix)]
fn child_descriptors<'a>(
    request: &'a ProcessRequest<'a>,
    artifacts: &'a ProcessArtifacts,
    target_group_ack: BorrowedFd<'a>,
    target_lifetime_writer: BorrowedFd<'a>,
    environment: &mut std::collections::BTreeMap<String, String>,
) -> Vec<BorrowedFd<'a>> {
    let mut descriptors = request.inherited_descriptors.to_vec();
    descriptors.push(request.executable_descriptor);
    descriptors.push(request.target_descriptor);
    descriptors.push(request.runtime.perl.descriptor);
    descriptors.push(request.runtime.time.descriptor);
    descriptors.push(request.runtime.observer.descriptor);
    descriptors.push(request.working_directory_descriptor);
    descriptors.extend(artifacts.child_descriptors());
    descriptors.push(target_group_ack);
    descriptors.push(target_lifetime_writer);
    environment.insert(
        WORKING_DIRECTORY_FD_ENV.to_owned(),
        request.working_directory_descriptor.as_raw_fd().to_string(),
    );
    descriptors.sort_unstable_by_key(AsRawFd::as_raw_fd);
    descriptors.dedup_by_key(|descriptor| descriptor.as_raw_fd());
    if let Some(maximum) = descriptors.iter().map(AsRawFd::as_raw_fd).max() {
        environment.insert(INHERITED_FD_MAX_ENV.to_owned(), maximum.to_string());
    }
    environment.insert(
        INHERITED_FDS_ENV.to_owned(),
        descriptors
            .iter()
            .map(|descriptor| descriptor.as_raw_fd().to_string())
            .collect::<Vec<_>>()
            .join(","),
    );
    descriptors
}

#[cfg(unix)]
fn bind_child_descriptors(
    command: &mut Command,
    descriptors: &[BorrowedFd<'_>],
) -> Result<(), Box<dyn Error>> {
    let mappings = descriptors
        .iter()
        .map(|descriptor| {
            Ok(FdMapping {
                parent_fd: descriptor.try_clone_to_owned()?,
                child_fd: descriptor.as_raw_fd(),
            })
        })
        .collect::<Result<Vec<_>, std::io::Error>>()?;
    command.fd_mappings(mappings)?;
    Ok(())
}
