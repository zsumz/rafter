use std::{
    collections::BTreeMap,
    env,
    error::Error,
    ffi::OsString,
    fs::File,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

static TELEMETRY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(super) struct ProcessOutput {
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
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub stdout: String,
    pub stderr: String,
}

pub(super) fn timed(
    program: &str,
    arguments: &[OsString],
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
) -> Result<ProcessOutput, Box<dyn Error>> {
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
        .current_dir(current_dir)
        .output()?;
    let telemetry = std::fs::read(&telemetry_path)?;
    std::fs::remove_file(&telemetry_path)?;
    let peak_rss_kib = parse_peak_rss(&telemetry)
        .ok_or("/usr/bin/time did not report maximum resident set size")?;
    Ok(ProcessOutput {
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
        .current_dir(current_dir)
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
        status,
        stdout,
        stderr,
        duration: started.elapsed(),
        peak_rss_kib,
        timed_out,
    })
}

pub(super) fn base_environment() -> BTreeMap<String, String> {
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

pub(super) fn combined_log(label: &str, output: &ProcessOutput) -> Vec<u8> {
    format!(
        "label: {label}\nexit_code: {:?}\ntimed_out: {}\nduration_ms: {}\npeak_rss_kib: {}\n\n--- stdout ---\n{}\n--- stderr ---\n{}",
        output.status.code(),
        output.timed_out,
        output.duration.as_millis(),
        output.peak_rss_kib,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .into_bytes()
}

pub(super) fn json_log(label: &str, output: &ProcessOutput) -> Result<Vec<u8>, Box<dyn Error>> {
    Ok(serde_json::to_vec_pretty(&ProcessLog {
        schema_version: 1,
        label: label.to_owned(),
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

    use super::{parse_peak_rss, process_rss_kib, timed_with_timeout, ProcessLog};

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
    fn timed_child_is_killed_at_its_soft_timeout() {
        if process_rss_kib(std::process::id()).is_none() {
            return;
        }
        let output = timed_with_timeout(
            "sleep",
            &[OsString::from("5")],
            &BTreeMap::new(),
            Path::new("."),
            Duration::from_millis(10),
        )
        .expect("timed child produces telemetry");

        assert!(output.timed_out);
        assert!(!output.status.success());
        assert!(output.duration < Duration::from_secs(2));
        assert!(output.peak_rss_kib > 0);
    }

    #[test]
    fn structured_process_log_rejects_unknown_fields() {
        let source = r#"{
            "schema_version": 1,
            "label": "model-check",
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
