use std::{
    collections::BTreeMap,
    env,
    error::Error,
    ffi::OsString,
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

static TELEMETRY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(super) struct ProcessOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub duration: Duration,
    pub peak_rss_kib: u64,
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

pub(super) fn combined_log(label: &str, output: &ProcessOutput) -> Vec<u8> {
    format!(
        "label: {label}\nexit_code: {:?}\nduration_ms: {}\npeak_rss_kib: {}\n\n--- stdout ---\n{}\n--- stderr ---\n{}",
        output.status.code(),
        output.duration.as_millis(),
        output.peak_rss_kib,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
    .into_bytes()
}

pub(super) fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::parse_peak_rss;

    #[test]
    fn parses_platform_peak_rss() {
        let input = if cfg!(target_os = "macos") {
            b"  1048576  maximum resident set size\n".as_slice()
        } else {
            b"\tMaximum resident set size (kbytes): 1024\n".as_slice()
        };
        assert_eq!(parse_peak_rss(input), Some(1024));
    }
}
