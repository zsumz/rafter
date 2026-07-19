use super::{
    env, fs, AtomicU64, BTreeMap, Command, CommandExt, Digest, Duration, Error, Instant,
    InvocationReceipt, LabeledProcess, Ordering, OsString, Path, PathBuf, ProcessGroupObservation,
    ProcessGroupState, ProcessLog, ProcessMetrics, ProcessOutput, Read, Sha256, Stdio,
    PS_TELEMETRY_TIMEOUT, TELEMETRY_SEQUENCE,
};

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};

use crate::execution::filesystem::{self as producer_fs, ChildDirectory, HeldDirectory, HeldFile};

pub(super) struct BoundInvocation {
    receipt: InvocationReceipt,
    executable: BoundExecutable,
    current_dir: HeldDirectory,
    child_current_dir: ChildDirectory,
}

impl BoundInvocation {
    pub(super) fn receipt(&self) -> &InvocationReceipt {
        &self.receipt
    }

    #[cfg(test)]
    pub(super) fn into_receipt(self) -> InvocationReceipt {
        self.receipt
    }

    pub(super) fn executable_path(&self) -> &Path {
        &self.executable.path
    }

    #[cfg(unix)]
    pub(super) fn executable_descriptor(&self) -> BorrowedFd<'_> {
        self.executable.descriptor.as_fd()
    }

    #[cfg(unix)]
    pub(super) fn current_dir_descriptor(&self) -> BorrowedFd<'_> {
        self.child_current_dir.descriptor()
    }

    pub(super) fn verify_path_bindings(&self) -> Result<(), Box<dyn Error>> {
        if let Some(held) = &self.executable.held {
            held.verify_path_binding()?;
        }
        self.current_dir.verify_path_binding()?;
        Ok(())
    }
}

struct BoundExecutable {
    #[cfg(unix)]
    descriptor: OwnedFd,
    path: PathBuf,
    held: Option<HeldFile>,
}

#[cfg(test)]
pub(crate) fn expected_invocation(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
) -> Result<InvocationReceipt, Box<dyn Error>> {
    Ok(bind_invocation(program, arguments, environment, current_dir)?.into_receipt())
}

pub(super) fn bind_invocation(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
) -> Result<BoundInvocation, Box<dyn Error>> {
    let arguments = arguments
        .iter()
        .map(|argument| {
            argument
                .to_str()
                .map(str::to_owned)
                .ok_or("subprocess argument is not UTF-8")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let current_dir = HeldDirectory::open(current_dir)?;
    let current_dir_receipt = current_dir
        .external_path()
        .into_os_string()
        .into_string()
        .map_err(|_| "subprocess working directory is not UTF-8")?;
    let (executable, program_sha256) = bind_executable(program, environment)?;
    let receipt = InvocationReceipt {
        program: program.to_owned(),
        program_sha256,
        arguments,
        current_dir: current_dir_receipt,
        environment: environment.clone(),
        environment_sha256: digest_environment(environment),
    };
    let child_current_dir = current_dir.bind_for_child()?;
    Ok(BoundInvocation {
        receipt,
        executable,
        current_dir,
        child_current_dir,
    })
}

fn bind_executable(
    program: &str,
    environment: &BTreeMap<String, String>,
) -> Result<(BoundExecutable, String), Box<dyn Error>> {
    let (file, held, executable_path) = if Path::new(program).components().count() > 1 {
        let path = PathBuf::from(program);
        let workspace_path =
            !path.is_absolute()
                || path.starts_with(std::env::current_dir().map_err(|error| {
                    format!("resolve workspace for executable evidence: {error}")
                })?);
        if workspace_path {
            let held = producer_fs::hold_file(&path)?;
            let file = held.try_clone_std()?;
            let executable_path = held.external_path();
            (file, Some(held), executable_path)
        } else {
            let path = fs::canonicalize(path)?;
            (fs::File::open(&path)?, None, path)
        }
    } else {
        let path = environment
            .get("PATH")
            .and_then(|path| {
                env::split_paths(path)
                    .map(|directory| directory.join(program))
                    .find(|candidate| candidate.is_file())
            })
            .ok_or_else(|| format!("subprocess program is not present on PATH: {program}"))?;
        let path = fs::canonicalize(path)?;
        (fs::File::open(&path)?, None, path)
    };
    if !file.metadata()?.is_file() {
        return Err(format!("subprocess program is not a regular file: {program}").into());
    }
    let program_sha256 = sha256_file(&file)?;
    #[cfg(unix)]
    {
        let descriptor = rustix::io::fcntl_dupfd_cloexec(&file, 3)?;
        #[cfg(target_os = "linux")]
        let path = PathBuf::from(format!("/proc/self/fd/{}", descriptor.as_raw_fd()));
        #[cfg(target_os = "linux")]
        let _ = executable_path;
        #[cfg(not(target_os = "linux"))]
        let path = executable_path;
        Ok((
            BoundExecutable {
                descriptor,
                path,
                held,
            },
            program_sha256,
        ))
    }
    #[cfg(not(unix))]
    Err("descriptor-bound subprocess execution requires Unix".into())
}

fn sha256_file(file: &fs::File) -> Result<String, Box<dyn Error>> {
    let mut file = file.try_clone()?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

pub(crate) fn digest_environment(environment: &BTreeMap<String, String>) -> String {
    let encoded = environment
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("\0");
    format!("{:x}", Sha256::digest(encoded))
}

pub(crate) fn parse_combined_processes(source: &str) -> Result<Vec<LabeledProcess>, String> {
    let mut remaining = source;
    let mut processes = Vec::new();
    while !remaining.is_empty() {
        let (header, payload) = remaining
            .split_once("\n\n")
            .ok_or_else(|| "combined process log omitted framed payload".to_owned())?;
        let mut lines = header.lines();
        let schema_version = lines
            .next()
            .and_then(|line| line.strip_prefix("schema_version: "))
            .and_then(|version| version.parse::<u32>().ok())
            .filter(|version| matches!(version, 3 | 4))
            .ok_or_else(|| "combined process log used an unsupported schema".to_owned())?;
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
        let detector_challenge = if schema_version == 4 {
            let challenge = lines
                .next()
                .and_then(|line| line.strip_prefix("detector_challenge: "))
                .filter(|challenge| {
                    challenge.len() == 64
                        && challenge
                            .bytes()
                            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                })
                .ok_or_else(|| {
                    "combined process log has a malformed detector challenge".to_owned()
                })?;
            Some(challenge.to_owned())
        } else {
            None
        };
        let exit_code = lines
            .next()
            .and_then(|line| line.strip_prefix("exit_code: "))
            .ok_or_else(|| "combined process log omitted exit code".to_owned())?;
        let exit_code = parse_exit_code(exit_code)?;
        let timed_out = lines
            .next()
            .and_then(|line| line.strip_prefix("timed_out: "))
            .ok_or_else(|| "combined process log omitted timeout status".to_owned())?;
        let timed_out = timed_out
            .parse::<bool>()
            .map_err(|error| format!("parse combined process timeout status: {error}"))?;
        let duration_ms = metric_line(&mut lines, "duration_ms: ")?;
        let peak_rss_kib = metric_line(&mut lines, "peak_rss_kib: ")?;
        let stdout_bytes = metric_line(&mut lines, "stdout_bytes: ")?;
        let stderr_bytes = metric_line(&mut lines, "stderr_bytes: ")?;
        if lines.next().is_some() {
            return Err("combined process log contained unknown header fields".to_owned());
        }
        if peak_rss_kib == 0 {
            return Err("combined process log omitted peak RSS".to_owned());
        }
        let stdout_bytes = usize::try_from(stdout_bytes)
            .map_err(|_| "combined process stdout length exceeds usize".to_owned())?;
        let stderr_bytes = usize::try_from(stderr_bytes)
            .map_err(|_| "combined process stderr length exceeds usize".to_owned())?;
        let payload_bytes = stdout_bytes
            .checked_add(stderr_bytes)
            .ok_or_else(|| "combined process payload length overflowed".to_owned())?;
        let framed = payload.get(..payload_bytes).ok_or_else(|| {
            "combined process payload was truncated or not UTF-8 framed".to_owned()
        })?;
        let stdout = framed
            .get(..stdout_bytes)
            .ok_or_else(|| "combined process stdout boundary was not UTF-8".to_owned())?;
        let stderr = framed
            .get(stdout_bytes..)
            .ok_or_else(|| "combined process stderr boundary was not UTF-8".to_owned())?;
        remaining = payload
            .get(payload_bytes..)
            .ok_or_else(|| "combined process payload boundary was invalid".to_owned())?;
        processes.push(LabeledProcess {
            label,
            invocation,
            exit_code,
            timed_out,
            metrics: ProcessMetrics {
                duration_ms,
                peak_rss_kib,
            },
            stdout: stdout.to_owned(),
            stderr: stderr.to_owned(),
            detector_challenge,
        });
    }
    if processes.is_empty() {
        return Err("combined process log contained no process receipt".to_owned());
    }
    Ok(processes)
}

fn parse_exit_code(source: &str) -> Result<Option<i32>, String> {
    if source == "None" {
        return Ok(None);
    }
    source
        .strip_prefix("Some(")
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| "combined process log has malformed exit code".to_owned())?
        .parse::<i32>()
        .map(Some)
        .map_err(|error| format!("parse combined process exit code: {error}"))
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

pub(super) fn telemetry_path() -> Result<(HeldDirectory, PathBuf, PathBuf), Box<dyn Error>> {
    let directory = Path::new("target/rafter-invariants/telemetry");
    let directory = HeldDirectory::create_all(directory)?;
    let (path, reservation) = allocate_telemetry_path(
        &directory.external_path(),
        std::process::id(),
        &TELEMETRY_SEQUENCE,
    )?;
    Ok((directory, path, reservation))
}

pub(super) fn allocate_telemetry_path(
    directory: &Path,
    process_id: u32,
    sequence: &AtomicU64,
) -> Result<(PathBuf, PathBuf), Box<dyn Error>> {
    loop {
        let sequence = sequence.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!("{process_id}-{sequence}.time"));
        let prefix = path.with_extension("");
        let reservation = prefix.with_extension("reserve");
        match producer_fs::create_new_file(&reservation) {
            Ok(_) => {
                let collision = ["stdout", "stderr", "time", "pgid"].iter().try_fold(
                    false,
                    |collision, extension| {
                        Ok::<_, Box<dyn Error>>(
                            collision
                                || producer_fs::path_exists(&prefix.with_extension(extension))?,
                        )
                    },
                )?;
                if collision {
                    producer_fs::remove_file(&reservation)?;
                    continue;
                }
                return Ok((path, reservation));
            }
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists) => {}
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn parse_peak_rss(stderr: &[u8]) -> Option<u64> {
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

pub(super) fn process_group_rss_kib(process_group: u32) -> Result<u64, Box<dyn Error>> {
    Ok(process_group_observation(process_group)?.rss_kib)
}

pub(super) fn process_group_observation(
    process_group: u32,
) -> Result<ProcessGroupObservation, Box<dyn Error>> {
    let output = bounded_internal_output(
        "ps",
        &["-e", "-o", "pgid=,rss=,stat="],
        PS_TELEMETRY_TIMEOUT,
    )?;
    if !output.status.success() {
        return Err(format!(
            "sample process-group RSS with ps exited {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    parse_process_group_observation(&String::from_utf8_lossy(&output.stdout), process_group)
}

fn bounded_internal_output(
    program: &str,
    arguments: &[&str],
    timeout: Duration,
) -> Result<std::process::Output, Box<dyn Error>> {
    let mut command = Command::new(program);
    command
        .args(arguments)
        .env_clear()
        .envs(base_environment())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn()?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or("internal command omitted stdout")?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or("internal command omitted stderr")?;
    let stdout_reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).map(|_| bytes)
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).map(|_| bytes)
    });
    let started = Instant::now();
    let (status, timed_out) = loop {
        if let Some(status) = child.try_wait()? {
            break (status, false);
        }
        if started.elapsed() >= timeout {
            child.kill()?;
            break (child.wait()?, true);
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stdout = stdout_reader
        .join()
        .map_err(|_| "internal command stdout reader panicked")??;
    let stderr = stderr_reader
        .join()
        .map_err(|_| "internal command stderr reader panicked")??;
    if timed_out {
        return Err(format!(
            "internal command {program} timed out after {} ms; stdout: {}; stderr: {}",
            duration_ms(timeout),
            String::from_utf8_lossy(&stdout).trim(),
            String::from_utf8_lossy(&stderr).trim()
        )
        .into());
    }
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

pub(super) fn parse_process_group_observation(
    source: &str,
    process_group: u32,
) -> Result<ProcessGroupObservation, Box<dyn Error>> {
    let mut observation = ProcessGroupObservation {
        state: ProcessGroupState::Absent,
        rss_kib: 0,
    };
    for line in source.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let pgid = fields
            .next()
            .ok_or("ps RSS row omitted process-group ID")?
            .parse::<u32>()?;
        let rss = fields
            .next()
            .ok_or("ps RSS row omitted resident-set size")?
            .parse::<u64>()?;
        let state = fields.next().ok_or("ps RSS row omitted process state")?;
        if fields.next().is_some() {
            return Err("ps RSS row contained unexpected fields".into());
        }
        // A zombie retains its process-group ID until its parent reaps it, but
        // it cannot execute, fork, hold descriptors, or survive a signal.
        if pgid == process_group && !state.starts_with('Z') {
            observation.state = ProcessGroupState::Alive;
            observation.rss_kib = observation
                .rss_kib
                .checked_add(rss)
                .ok_or("process-group RSS sum overflowed u64")?;
        }
    }
    Ok(observation)
}

pub(in crate::producer) fn combined_log(
    label: &str,
    output: &ProcessOutput,
) -> Result<Vec<u8>, serde_json::Error> {
    let invocation = serde_json::to_string(&output.invocation)?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Ok(format!(
        "schema_version: 3\nlabel: {label}\ninvocation: {invocation}\nexit_code: {:?}\ntimed_out: {}\nduration_ms: {}\npeak_rss_kib: {}\nstdout_bytes: {}\nstderr_bytes: {}\n\n{}{}",
        output.status.code(),
        output.timed_out,
        duration_ms(output.duration),
        output.peak_rss_kib,
        stdout.len(),
        stderr.len(),
        stdout,
        stderr,
    )
    .into_bytes())
}

pub(in crate::producer) fn combined_detector_log(
    label: &str,
    output: &ProcessOutput,
    detector_challenge: &str,
) -> Result<Vec<u8>, serde_json::Error> {
    let invocation = serde_json::to_string(&output.invocation)?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    Ok(format!(
        "schema_version: 4\nlabel: {label}\ninvocation: {invocation}\ndetector_challenge: {detector_challenge}\nexit_code: {:?}\ntimed_out: {}\nduration_ms: {}\npeak_rss_kib: {}\nstdout_bytes: {}\nstderr_bytes: {}\n\n{}{}",
        output.status.code(),
        output.timed_out,
        duration_ms(output.duration),
        output.peak_rss_kib,
        stdout.len(),
        stderr.len(),
        stdout,
        stderr,
    )
    .into_bytes())
}

pub(in crate::producer) fn json_log(
    label: &str,
    output: &ProcessOutput,
) -> Result<Vec<u8>, Box<dyn Error>> {
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

pub(in crate::producer) fn tla_json_log(
    label: &str,
    output: &ProcessOutput,
) -> Result<Vec<u8>, Box<dyn Error>> {
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

pub(in crate::producer) fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
