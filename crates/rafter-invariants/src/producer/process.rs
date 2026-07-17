use std::{
    collections::BTreeMap,
    env,
    error::Error,
    ffi::OsString,
    fmt, fs,
    io::Read,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, MutexGuard,
    },
    time::{Duration, Instant},
};

#[cfg(unix)]
use command_fds::{CommandFdExt, FdMapping};
#[cfg(unix)]
use rustix::{
    io::Errno,
    process::{kill_process_group, Pid, Signal},
};
#[cfg(unix)]
use std::os::fd::{AsRawFd, BorrowedFd};
#[cfg(unix)]
use std::os::unix::process::CommandExt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::InvocationReceipt;

mod budget;
mod evidence;
mod managed;
mod output;
mod termination;

pub(super) use budget::{
    active_layer_deadlines, ensure_execution_deadline, ensure_total_deadline, LayerBudgetGuard,
    ProcessKind,
};
use budget::{
    active_process_timeout, active_total_process_timeout, has_active_layer_budget, ProcessPolicy,
};
#[cfg(test)]
use budget::{layer_budget, DEFAULT_KILL_CONFIRMATION_TIMEOUT};
use evidence::bind_invocation;
#[cfg(test)]
pub(crate) use evidence::expected_invocation;
#[cfg(test)]
use evidence::{allocate_telemetry_path, parse_process_group_observation};
pub(crate) use evidence::{base_environment, digest_environment, parse_combined_processes};
pub(super) use evidence::{
    combined_detector_log, combined_log, duration_ms, json_log, tla_json_log,
};
use evidence::{parse_peak_rss, process_group_observation, process_group_rss_kib, telemetry_path};
use managed::{
    take_fallback_cleanup_failures, CollectedProcessStatus, ManagedProcess, ProcessCleanupError,
    ProcessGroupObservation, ProcessGroupState, ProcessSignal, SignalDelivery, TimeoutTermination,
};
use output::{collect_process_output, finish_managed_process};
use termination::{
    await_target_process_group, confirm_process_group_absent, measurement_error,
    process_group_state, retained_error, retained_result, signal_process_group,
    terminate_after_timeout,
};
#[cfg(test)]
use termination::{classify_signal_delivery, cleanup_error, confirm_process_group_absent_with};

static TELEMETRY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static FALLBACK_CLEANUP_FAILURES: Mutex<Vec<String>> = Mutex::new(Vec::new());
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(100);
const IDENTITY_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const PS_TELEMETRY_TIMEOUT: Duration = Duration::from_secs(2);
const TARGET_GROUP_ENV: &str = "RAFTER_INVARIANT_TARGET_GROUP_FILE";
#[cfg(unix)]
const INHERITED_FD_MAX_ENV: &str = "RAFTER_INVARIANT_INHERITED_FD_MAX";
#[cfg(unix)]
const WORKING_DIRECTORY_FD_ENV: &str = "RAFTER_INVARIANT_WORKING_DIRECTORY_FD";
const TARGET_GROUP_LAUNCHER: &str = r#"
my $path = delete $ENV{'RAFTER_INVARIANT_TARGET_GROUP_FILE'};
my $inherited_fd_max = delete $ENV{'RAFTER_INVARIANT_INHERITED_FD_MAX'};
my $working_directory_fd = delete $ENV{'RAFTER_INVARIANT_WORKING_DIRECTORY_FD'};
$^F = $inherited_fd_max if defined($inherited_fd_max) && $inherited_fd_max > $^F;
POSIX::setpgid(0, 0) == 0 or die "setpgid: $!";
open(my $group, '>', $path) or die "open process-group receipt: $!";
print {$group} "$$\n" or die "write process-group receipt: $!";
close($group) or die "close process-group receipt: $!";
open(my $working_directory, '<&=', $working_directory_fd) or die "open working-directory descriptor: $!";
chdir($working_directory) or die "chdir working-directory descriptor: $!";
my $executable = shift @ARGV;
my $logical_program = shift @ARGV;
exec {$executable} $logical_program, @ARGV or die "exec $executable: $!";
"#;

#[derive(Debug)]
pub(super) struct ProcessOutput {
    pub invocation: InvocationReceipt,
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub duration: Duration,
    pub peak_rss_kib: u64,
    pub timed_out: bool,
    pub termination: Option<TerminationReceipt>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct IdentityOutput {
    pub stdout: String,
    pub stderr: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TerminationReceipt {
    pub process_group: bool,
    pub term_signal_sent: bool,
    pub grace_ms: u64,
    pub kill_signal_sent: bool,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessLog {
    pub schema_version: u32,
    pub label: String,
    pub invocation: InvocationReceipt,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination: Option<TerminationReceipt>,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProcessMetrics {
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LabeledProcess {
    pub label: String,
    pub invocation: InvocationReceipt,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub metrics: ProcessMetrics,
    pub stdout: String,
    pub stderr: String,
    pub detector_challenge: Option<String>,
}

impl ProcessLog {
    pub(crate) fn has_complete_invocation(&self) -> bool {
        let invocation = &self.invocation;
        !invocation.program.trim().is_empty()
            && invocation.program_sha256.len() == 64
            && invocation
                .program_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            && !invocation.arguments.is_empty()
            && Path::new(&invocation.current_dir).is_absolute()
            && digest_environment(&invocation.environment) == invocation.environment_sha256
            && invocation.environment_sha256.len() == 64
            && invocation
                .environment_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    }
}

pub(super) fn timed_for(
    kind: ProcessKind,
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_for_with_cap(kind, program, arguments, environment, current_dir, None)
}

pub(super) fn timed_for_with_cap(
    kind: ProcessKind,
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    requested_cap: Option<Duration>,
) -> Result<ProcessOutput, Box<dyn Error>> {
    let (timeout, policy) = active_process_timeout(kind, requested_cap)?;
    timed_with_timeout_and_policy(
        program,
        arguments,
        environment,
        current_dir,
        timeout,
        policy,
    )
}

#[cfg(target_os = "linux")]
pub(super) fn timed_for_with_cap_and_descriptors(
    kind: ProcessKind,
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    requested_cap: Option<Duration>,
    inherited_descriptors: &[BorrowedFd<'_>],
) -> Result<ProcessOutput, Box<dyn Error>> {
    let (timeout, policy) = active_process_timeout(kind, requested_cap)?;
    timed_with_timeout_and_policy_and_descriptors(
        program,
        arguments,
        environment,
        current_dir,
        timeout,
        policy,
        inherited_descriptors,
    )
}

pub(super) fn timed_with_optional_layer_budget(
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
    timed_with_timeout(program, arguments, environment, current_dir, requested_cap)
}

pub(super) fn timed_with_timeout(
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

pub(crate) fn identity_command(
    program: &str,
    arguments: &[&str],
) -> Result<IdentityOutput, Box<dyn Error>> {
    identity_command_in(program, arguments, Path::new("."))
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
fn identity_command_with_timeout(
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
    let (timeout, policy) = if has_active_layer_budget() {
        if use_total_budget {
            active_total_process_timeout(ProcessKind::Identity, Some(timeout))?
        } else {
            active_process_timeout(ProcessKind::Identity, Some(timeout))?
        }
    } else {
        (timeout, ProcessPolicy::default())
    };
    let output = timed_with_timeout_and_policy(
        program,
        &arguments,
        &environment,
        current_dir,
        timeout,
        policy,
    )?;
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

#[cfg(test)]
fn timed_with_timeout_and_grace(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    grace: Duration,
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_with_timeout_and_policy(
        program,
        arguments,
        environment,
        current_dir,
        timeout,
        ProcessPolicy {
            termination_grace: grace,
            ..ProcessPolicy::default()
        },
    )
}

fn timed_with_timeout_and_policy(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    policy: ProcessPolicy,
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_with_timeout_and_policy_and_descriptors(
        program,
        arguments,
        environment,
        current_dir,
        timeout,
        policy,
        &[],
    )
}

fn timed_with_timeout_and_policy_and_descriptors(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    policy: ProcessPolicy,
    inherited_descriptors: &[BorrowedFd<'_>],
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_with_timeout_and_policy_and_descriptors_after_bind(
        program,
        arguments,
        environment,
        current_dir,
        timeout,
        policy,
        inherited_descriptors,
        || {},
    )
}

#[cfg(test)]
fn timed_with_timeout_after_bind(
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
        timeout,
        ProcessPolicy::default(),
        &[],
        after_bind,
    )
}

#[allow(clippy::too_many_arguments)]
fn timed_with_timeout_and_policy_and_descriptors_after_bind(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    policy: ProcessPolicy,
    inherited_descriptors: &[BorrowedFd<'_>],
    after_bind: impl FnOnce(),
) -> Result<ProcessOutput, Box<dyn Error>> {
    require_sound_process_execution()?;
    let prior_cleanup_failures = take_fallback_cleanup_failures();
    if !prior_cleanup_failures.is_empty() {
        return Err(format!(
            "prior fallback subprocess cleanup failed: {}",
            prior_cleanup_failures.join("; ")
        )
        .into());
    }
    let invocation_binding = bind_invocation(program, arguments, environment, current_dir)?;
    let invocation = invocation_binding.receipt().clone();
    after_bind();
    let started = Instant::now();
    let (telemetry_directory, telemetry_path, reservation_path) = telemetry_path()?;
    let output_prefix = telemetry_path.with_extension("");
    let stdout_path = output_prefix.with_extension("stdout");
    let stderr_path = output_prefix.with_extension("stderr");
    let resource_path = output_prefix.with_extension("time");
    let process_group_path = output_prefix.with_extension("pgid");
    let stdout_file = super::filesystem::create_new_file(&stdout_path)?;
    let stderr_file = super::filesystem::create_new_file(&stderr_path)?;
    let mut command = Command::new("/usr/bin/time");
    command.arg("-o").arg(&resource_path);
    if cfg!(target_os = "macos") {
        command.arg("-l");
    } else if cfg!(target_os = "linux") {
        command.arg("-v");
    } else {
        return Err("peak RSS collection supports macOS and Linux".into());
    }
    let mut launcher_environment = environment.clone();
    launcher_environment.insert(
        TARGET_GROUP_ENV.to_owned(),
        process_group_path.to_string_lossy().into_owned(),
    );
    let mut child_descriptors = inherited_descriptors.to_vec();
    #[cfg(unix)]
    {
        child_descriptors.push(invocation_binding.executable_descriptor());
        child_descriptors.push(invocation_binding.current_dir_descriptor());
        launcher_environment.insert(
            WORKING_DIRECTORY_FD_ENV.to_owned(),
            invocation_binding
                .current_dir_descriptor()
                .as_raw_fd()
                .to_string(),
        );
    }
    child_descriptors.sort_unstable_by_key(AsRawFd::as_raw_fd);
    child_descriptors.dedup_by_key(|descriptor| descriptor.as_raw_fd());
    #[cfg(unix)]
    if let Some(maximum) = child_descriptors.iter().map(AsRawFd::as_raw_fd).max() {
        launcher_environment.insert(INHERITED_FD_MAX_ENV.to_owned(), maximum.to_string());
    }
    command
        .arg("/usr/bin/perl")
        .arg("-MPOSIX")
        .arg("-e")
        .arg(TARGET_GROUP_LAUNCHER)
        .arg(invocation_binding.executable_path())
        .arg(program)
        .args(arguments)
        .env_clear()
        .envs(&launcher_environment)
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));
    #[cfg(unix)]
    command.process_group(0);
    telemetry_directory.verify_path_binding()?;
    invocation_binding.verify_path_bindings()?;
    #[cfg(unix)]
    let child = spawn_with_descriptors(&mut command, &child_descriptors)
        .map_err(|error| retained_error(error, &stdout_path, &stderr_path, Some(&resource_path)))?;
    #[cfg(not(unix))]
    let child = command
        .spawn()
        .map_err(|error| retained_error(error, &stdout_path, &stderr_path, Some(&resource_path)))?;
    let mut process = ManagedProcess::new(child, policy.kill_confirmation_timeout);
    let result = collect_process_output(
        &mut process,
        invocation,
        started,
        timeout,
        policy,
        &stdout_path,
        &stderr_path,
        &resource_path,
        &process_group_path,
        &reservation_path,
    );
    telemetry_directory.verify_path_binding()?;
    let result = result.and_then(|output| {
        invocation_binding.verify_path_bindings()?;
        Ok(output)
    });
    finish_managed_process(process, result, &stdout_path, &stderr_path, &resource_path)
}

fn require_sound_process_execution() -> Result<(), Box<dyn Error>> {
    process_execution_policy(cfg!(target_os = "linux"), cfg!(test))
}

fn process_execution_policy(
    descriptor_execution_supported: bool,
    test_only_fallback: bool,
) -> Result<(), Box<dyn Error>> {
    if descriptor_execution_supported || test_only_fallback {
        Ok(())
    } else {
        Err("evidence subprocess execution requires Linux descriptor-based executable launch; this host cannot atomically bind a hash to exec".into())
    }
}

#[cfg(unix)]
fn spawn_with_descriptors(
    command: &mut Command,
    descriptors: &[BorrowedFd<'_>],
) -> Result<Child, Box<dyn Error>> {
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
    Ok(command.spawn()?)
}

#[cfg(test)]
mod tests;
