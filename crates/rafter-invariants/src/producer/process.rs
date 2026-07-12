use std::{
    collections::BTreeMap,
    env,
    error::Error,
    ffi::OsString,
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::InvocationReceipt;

static TELEMETRY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(super) struct ProcessOutput {
    pub invocation: InvocationReceipt,
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub duration: Duration,
    pub peak_rss_kib: u64,
    pub timed_out: bool,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessLog {
    pub schema_version: u32,
    pub label: String,
    pub invocation: InvocationReceipt,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
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
    })
}

pub(super) fn timed_with_timeout(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
) -> Result<ProcessOutput, Box<dyn Error>> {
    let invocation = expected_invocation(program, arguments, environment, current_dir)?;
    let started = Instant::now();
    let output_prefix = telemetry_path()?.with_extension("");
    let stdout_path = output_prefix.with_extension("stdout");
    let stderr_path = output_prefix.with_extension("stderr");
    let stdout_file = File::create(&stdout_path)?;
    let stderr_file = File::create(&stderr_path)?;
    let mut child = Command::new(program)
        .args(arguments)
        .env_clear()
        .envs(environment)
        .current_dir(&invocation.current_dir)
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()?;
    let mut peak_rss_kib = 0;
    let (status, timed_out) = loop {
        peak_rss_kib = peak_rss_kib.max(process_rss_kib(child.id()).unwrap_or_default());
        if let Some(status) = child.try_wait()? {
            break (status, false);
        }
        if started.elapsed() >= timeout {
            child.kill()?;
            break (child.wait()?, true);
        }
        std::thread::sleep(Duration::from_millis(100));
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
    })
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

pub(crate) fn parse_combined_invocations(
    source: &str,
) -> Result<Vec<LabeledInvocation>, serde_json::Error> {
    source
        .split("schema_version: 2\n")
        .skip(1)
        .map(|section| {
            let mut lines = section.lines();
            let label = lines
                .next()
                .and_then(|line| line.strip_prefix("label: "))
                .unwrap_or_default()
                .to_owned();
            let invocation = lines
                .next()
                .and_then(|line| line.strip_prefix("invocation: "))
                .unwrap_or_default();
            Ok(LabeledInvocation {
                label,
                invocation: serde_json::from_str(invocation)?,
            })
        })
        .collect()
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
    use std::{collections::BTreeMap, ffi::OsString, path::Path, time::Duration};

    use super::{
        combined_log, digest_environment, json_log, parse_peak_rss, process_rss_kib,
        timed_with_timeout, ProcessLog,
    };

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
        let structured: ProcessLog = serde_json::from_slice(
            &json_log("timeout", &output).expect("structured process log serializes"),
        )
        .expect("structured process log parses");
        assert_eq!(structured.schema_version, 2);
        assert_eq!(structured.invocation, output.invocation);
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
            "duration_ms": 1,
            "peak_rss_kib": 1,
            "stdout": "",
            "stderr": "",
            "trusted": true
        }"#;
        assert!(serde_json::from_str::<ProcessLog>(source).is_err());
    }
}
