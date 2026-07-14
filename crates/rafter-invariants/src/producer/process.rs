use std::{
    collections::BTreeMap,
    env,
    error::Error,
    ffi::OsString,
    fmt,
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

#[cfg(unix)]
use rustix::{
    io::Errno,
    process::{kill_process_group, test_kill_process_group, Pid, Signal},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::InvocationReceipt;

static TELEMETRY_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(100);
const KILL_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcessGroupState {
    Alive,
    Absent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SignalDelivery {
    Sent,
    GroupAbsent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProcessSignal {
    Term,
    Kill,
}

#[derive(Debug)]
struct ProcessCleanupError {
    detail: String,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl fmt::Display for ProcessCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; retained subprocess stdout at {} and stderr at {}",
            self.detail,
            self.stdout_path.display(),
            self.stderr_path.display()
        )
    }
}

impl Error for ProcessCleanupError {}

#[derive(Debug)]
struct TimeoutTermination {
    status: ExitStatus,
    timed_out: bool,
    term_signal_sent: bool,
    kill_signal_sent: bool,
}

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

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct LabeledInvocation {
    pub label: String,
    pub invocation: InvocationReceipt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProcessMetrics {
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct LabeledProcess {
    pub label: String,
    pub invocation: InvocationReceipt,
    pub metrics: ProcessMetrics,
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

pub(super) fn timed(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
) -> Result<ProcessOutput, Box<dyn Error>> {
    let invocation = expected_invocation(program, arguments, environment, current_dir)?;
    let started = Instant::now();
    let telemetry_path = telemetry_path()?;
    let mut command = Command::new("/usr/bin/time");
    command.arg("-o").arg(&telemetry_path);
    if cfg!(target_os = "macos") {
        command.arg("-l");
    } else if cfg!(target_os = "linux") {
        command.arg("-v");
    } else {
        return Err("peak RSS collection supports macOS and Linux".into());
    }
    let output = command
        .arg(program)
        .args(arguments)
        .env_clear()
        .envs(environment)
        .current_dir(&invocation.current_dir)
        .output()?;
    let telemetry = std::fs::read(&telemetry_path)?;
    std::fs::remove_file(&telemetry_path)?;
    let peak_rss_kib = parse_peak_rss(&telemetry)
        .ok_or("/usr/bin/time did not report maximum resident set size")?;
    Ok(ProcessOutput {
        invocation,
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
        duration: started.elapsed(),
        peak_rss_kib,
        timed_out: false,
        termination: None,
    })
}

pub(super) fn timed_with_timeout(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
) -> Result<ProcessOutput, Box<dyn Error>> {
    timed_with_timeout_and_grace(
        program,
        arguments,
        environment,
        current_dir,
        timeout,
        Duration::from_secs(30),
    )
}

fn timed_with_timeout_and_grace(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    grace: Duration,
) -> Result<ProcessOutput, Box<dyn Error>> {
    let invocation = expected_invocation(program, arguments, environment, current_dir)?;
    let started = Instant::now();
    let output_prefix = telemetry_path()?.with_extension("");
    let stdout_path = output_prefix.with_extension("stdout");
    let stderr_path = output_prefix.with_extension("stderr");
    let stdout_file = File::create(&stdout_path)?;
    let stderr_file = File::create(&stderr_path)?;
    let mut command = Command::new(program);
    command
        .args(arguments)
        .env_clear()
        .envs(environment)
        .current_dir(&invocation.current_dir)
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn()?;
    let mut peak_rss_kib = 0;
    let (status, timed_out, term_signal_sent, kill_signal_sent) = loop {
        peak_rss_kib = peak_rss_kib.max(process_rss_kib(child.id()).unwrap_or_default());
        if let Some(status) = child.try_wait()? {
            break (status, false, false, false);
        }
        if started.elapsed() >= timeout {
            let termination = terminate_after_timeout(
                &mut child,
                grace,
                &mut peak_rss_kib,
                &stdout_path,
                &stderr_path,
            )?;
            break (
                termination.status,
                termination.timed_out,
                termination.term_signal_sent,
                termination.kill_signal_sent,
            );
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL);
    };
    let stdout = std::fs::read(&stdout_path)?;
    let stderr = std::fs::read(&stderr_path)?;
    std::fs::remove_file(stdout_path)?;
    std::fs::remove_file(stderr_path)?;
    if peak_rss_kib == 0 {
        return Err("process RSS polling did not observe the child".into());
    }
    Ok(ProcessOutput {
        invocation,
        status,
        stdout,
        stderr,
        duration: started.elapsed(),
        peak_rss_kib,
        timed_out,
        termination: Some(TerminationReceipt {
            process_group: true,
            term_signal_sent,
            grace_ms: duration_ms(grace),
            kill_signal_sent,
        }),
    })
}

fn terminate_after_timeout(
    child: &mut std::process::Child,
    grace: Duration,
    peak_rss_kib: &mut u64,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<TimeoutTermination, Box<dyn Error>> {
    if let Some(status) = cleanup_result(child.try_wait(), stdout_path, stderr_path)? {
        return Ok(TimeoutTermination {
            status,
            timed_out: false,
            term_signal_sent: false,
            kill_signal_sent: false,
        });
    }
    let process_group = child.id();
    let term_signal_sent = match signal_process_group(process_group, ProcessSignal::Term) {
        Ok(SignalDelivery::Sent) => true,
        Ok(SignalDelivery::GroupAbsent) => {
            return Ok(TimeoutTermination {
                status: cleanup_result(child.wait(), stdout_path, stderr_path)?,
                timed_out: false,
                term_signal_sent: false,
                kill_signal_sent: false,
            });
        }
        Err(error) => return Err(cleanup_error(error, stdout_path, stderr_path)),
    };
    let grace_started = Instant::now();
    let mut leader_status = None;
    loop {
        *peak_rss_kib = (*peak_rss_kib).max(process_rss_kib(process_group).unwrap_or_default());
        if leader_status.is_none() {
            leader_status = cleanup_result(child.try_wait(), stdout_path, stderr_path)?;
        }
        match process_group_state(process_group) {
            Ok(ProcessGroupState::Absent) => {
                let status = match leader_status {
                    Some(status) => status,
                    None => cleanup_result(child.wait(), stdout_path, stderr_path)?,
                };
                return Ok(TimeoutTermination {
                    status,
                    timed_out: true,
                    term_signal_sent,
                    kill_signal_sent: false,
                });
            }
            Ok(ProcessGroupState::Alive) => {}
            Err(error) => return Err(cleanup_error(error, stdout_path, stderr_path)),
        }
        if grace_started.elapsed() >= grace {
            return kill_process_group_after_grace(
                child,
                process_group,
                leader_status,
                term_signal_sent,
                stdout_path,
                stderr_path,
            );
        }
        std::thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

fn kill_process_group_after_grace(
    child: &mut std::process::Child,
    process_group: u32,
    leader_status: Option<ExitStatus>,
    term_signal_sent: bool,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<TimeoutTermination, Box<dyn Error>> {
    let kill_signal_sent = match signal_process_group(process_group, ProcessSignal::Kill) {
        Ok(SignalDelivery::Sent) => true,
        Ok(SignalDelivery::GroupAbsent) => false,
        Err(error) => return Err(cleanup_error(error, stdout_path, stderr_path)),
    };
    let status = match leader_status {
        Some(status) => status,
        None => cleanup_result(child.wait(), stdout_path, stderr_path)?,
    };
    if let Err(error) = confirm_process_group_absent(process_group, KILL_CONFIRMATION_TIMEOUT) {
        return Err(cleanup_error(error, stdout_path, stderr_path));
    }
    Ok(TimeoutTermination {
        status,
        timed_out: true,
        term_signal_sent,
        kill_signal_sent,
    })
}

fn cleanup_error(
    error: impl fmt::Display,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Box<dyn Error> {
    Box::new(ProcessCleanupError {
        detail: error.to_string(),
        stdout_path: stdout_path.to_owned(),
        stderr_path: stderr_path.to_owned(),
    })
}

fn cleanup_result<T>(
    result: std::io::Result<T>,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<T, Box<dyn Error>> {
    result.map_err(|error| cleanup_error(error, stdout_path, stderr_path))
}

#[cfg(unix)]
fn process_group_pid(pid: u32) -> Result<Pid, Box<dyn Error>> {
    let pid = i32::try_from(pid).map_err(|_| format!("process group ID exceeds i32: {pid}"))?;
    Pid::from_raw(pid).ok_or_else(|| format!("process group ID must be positive: {pid}").into())
}

#[cfg(unix)]
fn classify_process_group_probe(result: Result<(), Errno>) -> Result<ProcessGroupState, Errno> {
    match result {
        Ok(()) => Ok(ProcessGroupState::Alive),
        Err(Errno::SRCH) => Ok(ProcessGroupState::Absent),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn classify_signal_delivery(result: Result<(), Errno>) -> Result<SignalDelivery, Errno> {
    match result {
        Ok(()) => Ok(SignalDelivery::Sent),
        Err(Errno::SRCH) => Ok(SignalDelivery::GroupAbsent),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn process_group_state(pid: u32) -> Result<ProcessGroupState, Box<dyn Error>> {
    let process_group = process_group_pid(pid)?;
    classify_process_group_probe(test_kill_process_group(process_group))
        .map_err(|error| format!("probe process group {pid}: {error}").into())
}

#[cfg(not(unix))]
fn process_group_state(_pid: u32) -> Result<ProcessGroupState, Box<dyn Error>> {
    Err("process-group cleanup requires Unix".into())
}

#[cfg(unix)]
fn signal_process_group(pid: u32, signal: ProcessSignal) -> Result<SignalDelivery, Box<dyn Error>> {
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
fn signal_process_group(
    _pid: u32,
    _signal: ProcessSignal,
) -> Result<SignalDelivery, Box<dyn Error>> {
    Err("process-group cleanup requires Unix".into())
}

fn confirm_process_group_absent(pid: u32, timeout: Duration) -> Result<(), Box<dyn Error>> {
    confirm_process_group_absent_with(timeout, || process_group_state(pid)).map_err(|error| {
        format!("confirm process group {pid} exited after SIGKILL: {error}").into()
    })
}

fn confirm_process_group_absent_with(
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

pub(crate) fn expected_invocation(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
) -> Result<InvocationReceipt, Box<dyn Error>> {
    let arguments = arguments
        .iter()
        .map(|argument| {
            argument
                .to_str()
                .map(str::to_owned)
                .ok_or("subprocess argument is not UTF-8")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let current_dir = fs::canonicalize(current_dir)?
        .into_os_string()
        .into_string()
        .map_err(|_| "subprocess working directory is not UTF-8")?;
    Ok(InvocationReceipt {
        program: program.to_owned(),
        program_sha256: executable_sha256(program, environment)?,
        arguments,
        current_dir,
        environment: environment.clone(),
        environment_sha256: digest_environment(environment),
    })
}

fn executable_sha256(
    program: &str,
    environment: &BTreeMap<String, String>,
) -> Result<String, Box<dyn Error>> {
    let path = if Path::new(program).components().count() > 1 {
        fs::canonicalize(program)?
    } else {
        environment
            .get("PATH")
            .and_then(|path| {
                env::split_paths(path)
                    .map(|directory| directory.join(program))
                    .find(|candidate| candidate.is_file())
            })
            .ok_or_else(|| format!("subprocess program is not present on PATH: {program}"))?
    };
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

pub(crate) fn digest_environment(environment: &BTreeMap<String, String>) -> String {
    let encoded = environment
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("\0");
    format!("{:x}", Sha256::digest(encoded))
}

pub(crate) fn parse_combined_invocations(source: &str) -> Result<Vec<LabeledInvocation>, String> {
    parse_combined_processes(source).map(|processes| {
        processes
            .into_iter()
            .map(|process| LabeledInvocation {
                label: process.label,
                invocation: process.invocation,
            })
            .collect()
    })
}

pub(crate) fn parse_combined_processes(source: &str) -> Result<Vec<LabeledProcess>, String> {
    let processes = source
        .split("schema_version: 2\n")
        .skip(1)
        .map(|section| {
            let mut lines = section.lines();
            let label = lines
                .next()
                .and_then(|line| line.strip_prefix("label: "))
                .ok_or_else(|| "combined process log omitted label".to_owned())?
                .to_owned();
            let invocation = lines
                .next()
                .and_then(|line| line.strip_prefix("invocation: "))
                .ok_or_else(|| "combined process log omitted invocation".to_owned())?;
            let invocation = serde_json::from_str(invocation)
                .map_err(|error| format!("parse combined process invocation: {error}"))?;
            let _exit_code = lines
                .next()
                .and_then(|line| line.strip_prefix("exit_code: "))
                .ok_or_else(|| "combined process log omitted exit code".to_owned())?;
            let _timed_out = lines
                .next()
                .and_then(|line| line.strip_prefix("timed_out: "))
                .ok_or_else(|| "combined process log omitted timeout status".to_owned())?;
            let duration_ms = metric_line(&mut lines, "duration_ms: ")?;
            let peak_rss_kib = metric_line(&mut lines, "peak_rss_kib: ")?;
            if peak_rss_kib == 0 {
                return Err("combined process log omitted peak RSS".to_owned());
            }
            Ok(LabeledProcess {
                label,
                invocation,
                metrics: ProcessMetrics {
                    duration_ms,
                    peak_rss_kib,
                },
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if processes.is_empty() {
        return Err("combined process log contained no process receipt".to_owned());
    }
    Ok(processes)
}

fn metric_line<'a>(lines: &mut impl Iterator<Item = &'a str>, prefix: &str) -> Result<u64, String> {
    lines
        .next()
        .and_then(|line| line.strip_prefix(prefix))
        .ok_or_else(|| format!("combined process log omitted {prefix}"))?
        .parse()
        .map_err(|error| format!("parse combined process metric {prefix}: {error}"))
}

pub(crate) fn base_environment() -> BTreeMap<String, String> {
    const ALLOWED: &[&str] = &[
        "CARGO_HOME",
        "DEVELOPER_DIR",
        "HOME",
        "PATH",
        "RUSTUP_HOME",
        "SDKROOT",
        "SYSTEMROOT",
    ];
    ALLOWED
        .iter()
        .filter_map(|name| env::var(name).ok().map(|value| ((*name).to_owned(), value)))
        .collect()
}

fn telemetry_path() -> Result<PathBuf, Box<dyn Error>> {
    let directory = Path::new("target/rafter-invariants/telemetry");
    std::fs::create_dir_all(directory)?;
    let sequence = TELEMETRY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    Ok(directory.join(format!("{}-{sequence}.time", std::process::id())))
}

fn parse_peak_rss(stderr: &[u8]) -> Option<u64> {
    let stderr = String::from_utf8_lossy(stderr);
    if cfg!(target_os = "macos") {
        stderr.lines().find_map(|line| {
            line.trim()
                .strip_suffix("  maximum resident set size")
                .and_then(|bytes| bytes.trim().parse::<u64>().ok())
                .map(|bytes| bytes.div_ceil(1024))
        })
    } else {
        stderr.lines().find_map(|line| {
            line.trim()
                .strip_prefix("Maximum resident set size (kbytes):")
                .and_then(|kib| kib.trim().parse::<u64>().ok())
        })
    }
}

fn process_rss_kib(pid: u32) -> Option<u64> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().parse().ok())
        .flatten()
}

pub(super) fn combined_log(
    label: &str,
    output: &ProcessOutput,
) -> Result<Vec<u8>, serde_json::Error> {
    let invocation = serde_json::to_string(&output.invocation)?;
    Ok(format!(
        "schema_version: 2\nlabel: {label}\ninvocation: {invocation}\nexit_code: {:?}\ntimed_out: {}\nduration_ms: {}\npeak_rss_kib: {}\n\n--- stdout ---\n{}\n--- stderr ---\n{}",
        output.status.code(),
        output.timed_out,
        output.duration.as_millis(),
        output.peak_rss_kib,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .into_bytes())
}

pub(super) fn json_log(label: &str, output: &ProcessOutput) -> Result<Vec<u8>, Box<dyn Error>> {
    Ok(serde_json::to_vec_pretty(&ProcessLog {
        schema_version: 2,
        label: label.to_owned(),
        invocation: output.invocation.clone(),
        exit_code: output.status.code(),
        timed_out: output.timed_out,
        termination: None,
        duration_ms: duration_ms(output.duration),
        peak_rss_kib: output.peak_rss_kib,
        stdout: String::from_utf8(output.stdout.clone())?,
        stderr: String::from_utf8(output.stderr.clone())?,
    })?)
}

pub(super) fn tla_json_log(label: &str, output: &ProcessOutput) -> Result<Vec<u8>, Box<dyn Error>> {
    Ok(serde_json::to_vec_pretty(&ProcessLog {
        schema_version: 3,
        label: label.to_owned(),
        invocation: output.invocation.clone(),
        exit_code: output.status.code(),
        timed_out: output.timed_out,
        termination: output.termination.clone(),
        duration_ms: duration_ms(output.duration),
        peak_rss_kib: output.peak_rss_kib,
        stdout: String::from_utf8(output.stdout.clone())?,
        stderr: String::from_utf8(output.stderr.clone())?,
    })?)
}

pub(super) fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        path::{Path, PathBuf},
        time::Duration,
    };

    #[cfg(unix)]
    use rustix::io::Errno;

    use super::{
        cleanup_error, combined_log, confirm_process_group_absent_with, digest_environment,
        json_log, parse_combined_processes, parse_peak_rss, process_rss_kib, timed_with_timeout,
        timed_with_timeout_and_grace, ProcessGroupState, ProcessLog,
    };

    #[cfg(unix)]
    use super::{classify_process_group_probe, classify_signal_delivery, SignalDelivery};

    #[test]
    fn parses_platform_peak_rss() {
        let input = if cfg!(target_os = "macos") {
            b"  1048576  maximum resident set size\n".as_slice()
        } else {
            b"\tMaximum resident set size (kbytes): 1024\n".as_slice()
        };
        assert_eq!(parse_peak_rss(input), Some(1024));
    }

    #[test]
    fn environment_digest_binds_the_exact_sorted_map() {
        let environment = BTreeMap::from([
            ("Z".to_owned(), "last".to_owned()),
            ("A".to_owned(), "first".to_owned()),
        ]);
        assert_eq!(
            digest_environment(&environment),
            "45f7a365bc34bcfbb88705678cd819fd3c0a5ccb9b6a72dc65e6506f4211c6fc"
        );
    }

    #[cfg(unix)]
    #[test]
    fn process_group_probe_distinguishes_absent_from_permission_denied() {
        assert_eq!(
            classify_process_group_probe(Err(Errno::SRCH)),
            Ok(ProcessGroupState::Absent)
        );
        assert_eq!(
            classify_process_group_probe(Err(Errno::PERM)),
            Err(Errno::PERM)
        );
        assert_eq!(
            classify_process_group_probe(Ok(())),
            Ok(ProcessGroupState::Alive)
        );
    }

    #[cfg(unix)]
    #[test]
    fn process_group_signal_distinguishes_absent_from_permission_denied() {
        assert_eq!(
            classify_signal_delivery(Err(Errno::SRCH)),
            Ok(SignalDelivery::GroupAbsent)
        );
        assert_eq!(classify_signal_delivery(Err(Errno::PERM)), Err(Errno::PERM));
        assert_eq!(classify_signal_delivery(Ok(())), Ok(SignalDelivery::Sent));
    }

    #[test]
    fn group_absence_confirmation_is_fail_closed() {
        let error =
            confirm_process_group_absent_with(Duration::ZERO, || Ok(ProcessGroupState::Alive))
                .expect_err("a group that remains alive must fail confirmation");
        assert!(error.to_string().contains("remained alive"));
    }

    #[test]
    fn cleanup_errors_name_retained_telemetry() {
        let error = cleanup_error(
            "permission denied",
            Path::new("target/telemetry.stdout"),
            Path::new("target/telemetry.stderr"),
        );
        let message = error.to_string();
        assert!(message.contains("permission denied"));
        assert!(message.contains("target/telemetry.stdout"));
        assert!(message.contains("target/telemetry.stderr"));
    }

    #[test]
    fn timed_child_is_killed_at_its_soft_timeout() {
        if process_rss_kib(std::process::id()).is_none() {
            return;
        }
        let environment = super::base_environment();
        let output = timed_with_timeout(
            "sleep",
            &[OsString::from("5")],
            &environment,
            Path::new("."),
            Duration::from_millis(10),
        )
        .expect("timed child produces telemetry");

        assert!(output.timed_out);
        assert!(!output.status.success());
        assert!(output.duration < Duration::from_secs(2));
        assert!(output.peak_rss_kib > 0);
        assert_eq!(output.invocation.program, "sleep");
        assert_eq!(output.invocation.arguments, ["5"]);
        assert_eq!(
            output.invocation.current_dir,
            std::fs::canonicalize(".")
                .expect("working directory canonicalizes")
                .to_string_lossy()
                .into_owned()
        );
        assert_eq!(
            output.invocation.environment_sha256,
            digest_environment(&environment)
        );

        let plain = String::from_utf8(combined_log("timeout", &output).expect("log serializes"))
            .expect("plain process log is UTF-8");
        assert!(plain.starts_with("schema_version: 2\nlabel: timeout\ninvocation: {"));
        assert!(plain.contains("\"program\":\"sleep\""));
        let parsed = parse_combined_processes(&plain).expect("combined metrics parse");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].metrics.peak_rss_kib, output.peak_rss_kib);
        let structured: ProcessLog = serde_json::from_slice(
            &json_log("timeout", &output).expect("structured process log serializes"),
        )
        .expect("structured process log parses");
        assert_eq!(structured.schema_version, 2);
        assert!(structured.termination.is_none());
        assert_eq!(structured.invocation, output.invocation);
    }

    #[test]
    fn timeout_escalates_from_group_term_to_group_kill() {
        if process_rss_kib(std::process::id()).is_none() {
            return;
        }
        let output = timed_with_timeout_and_grace(
            "sh",
            &[
                OsString::from("-c"),
                OsString::from("trap '' TERM; while :; do :; done"),
            ],
            &super::base_environment(),
            Path::new("."),
            Duration::from_millis(10),
            Duration::from_millis(20),
        )
        .expect("stubborn process group is killed");
        let termination = output.termination.expect("termination receipt");
        assert!(output.timed_out);
        assert!(termination.process_group);
        assert!(termination.term_signal_sent);
        assert!(termination.kill_signal_sent);
        assert_eq!(termination.grace_ms, 20);
    }

    #[test]
    fn timeout_term_cleans_up_descendants_without_escalation() {
        if process_rss_kib(std::process::id()).is_none() {
            return;
        }
        let marker = unique_test_path("descendant-term");
        let mut environment = super::base_environment();
        environment.insert(
            "MARKER_PATH".to_owned(),
            marker.to_string_lossy().into_owned(),
        );
        let output = timed_with_timeout_and_grace(
            "sh",
            &[
                OsString::from("-c"),
                OsString::from(
                    "(trap 'printf term > \"$MARKER_PATH\"; exit 0' TERM; while :; do sleep 1; done) & wait",
                ),
            ],
            &environment,
            Path::new("."),
            Duration::from_millis(200),
            Duration::from_secs(2),
        )
        .expect("TERM cleans up the leader and descendant process");
        let termination = output.termination.expect("termination receipt");
        assert!(output.timed_out);
        assert!(termination.term_signal_sent);
        assert!(!termination.kill_signal_sent);
        assert_eq!(
            std::fs::read_to_string(&marker).expect("descendant records TERM"),
            "term"
        );
        std::fs::remove_file(marker).expect("remove TERM marker");
    }

    #[test]
    fn timeout_kills_descendants_after_the_group_leader_exits() {
        if process_rss_kib(std::process::id()).is_none() {
            return;
        }
        let output = timed_with_timeout_and_grace(
            "sh",
            &[
                OsString::from("-c"),
                OsString::from("trap 'exit 0' TERM; (trap '' TERM; while :; do :; done) & wait"),
            ],
            &super::base_environment(),
            Path::new("."),
            Duration::from_millis(10),
            Duration::from_millis(20),
        )
        .expect("surviving descendant is killed with its process group");
        let termination = output.termination.expect("termination receipt");
        assert!(output.timed_out);
        assert!(termination.term_signal_sent);
        assert!(termination.kill_signal_sent);
    }

    #[test]
    fn structured_process_log_rejects_unknown_fields() {
        let source = r#"{
            "schema_version": 2,
            "label": "model-check",
            "invocation": {
                "program": "java",
                "program_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "arguments": ["-jar", "tla2tools.jar"],
                "current_dir": "/workspace/rafter",
                "environment": {},
                "environment_sha256": "0000000000000000000000000000000000000000000000000000000000000000"
            },
            "exit_code": 0,
            "timed_out": false,
            "termination": null,
            "duration_ms": 1,
            "peak_rss_kib": 1,
            "stdout": "",
            "stderr": "",
            "trusted": true
        }"#;
        assert!(serde_json::from_str::<ProcessLog>(source).is_err());
    }

    fn unique_test_path(label: &str) -> PathBuf {
        let sequence = super::TELEMETRY_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "rafter-invariants-{label}-{}-{sequence}",
            std::process::id()
        ))
    }
}
