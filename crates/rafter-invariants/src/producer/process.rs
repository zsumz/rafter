use std::{
    collections::BTreeMap,
    env,
    error::Error,
    ffi::OsString,
    fmt,
    fs::{self, OpenOptions},
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
use std::os::unix::process::CommandExt;

#[cfg(unix)]
use rustix::{
    io::Errno,
    process::{kill_process_group, Pid, Signal},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::InvocationReceipt;

mod budget;
mod evidence;
mod managed;
mod output;
mod termination;

use budget::{active_process_timeout, ProcessPolicy};
#[cfg(test)]
use budget::{layer_budget, DEFAULT_KILL_CONFIRMATION_TIMEOUT};
pub(super) use budget::{LayerBudgetGuard, ProcessKind};
#[cfg(test)]
use evidence::{allocate_telemetry_path, parse_process_group_observation};
pub(crate) use evidence::{
    base_environment, digest_environment, expected_invocation, parse_combined_processes,
};
pub(super) use evidence::{combined_log, duration_ms, json_log, tla_json_log};
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
const TARGET_GROUP_LAUNCHER: &str = r#"
my $path = delete $ENV{'RAFTER_INVARIANT_TARGET_GROUP_FILE'};
POSIX::setpgid(0, 0) == 0 or die "setpgid: $!";
open(my $group, '>', $path) or die "open process-group receipt: $!";
print {$group} "$$\n" or die "write process-group receipt: $!";
close($group) or die "close process-group receipt: $!";
exec {$ARGV[0]} @ARGV or die "exec $ARGV[0]: $!";
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
    identity_command_with_timeout(program, arguments, IDENTITY_COMMAND_TIMEOUT)
}

fn identity_command_with_timeout(
    program: &str,
    arguments: &[&str],
    timeout: Duration,
) -> Result<IdentityOutput, Box<dyn Error>> {
    let arguments = arguments.iter().map(OsString::from).collect::<Vec<_>>();
    let output = timed_with_timeout_and_policy(
        program,
        &arguments,
        &base_environment(),
        Path::new("."),
        timeout,
        ProcessPolicy::default(),
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
    let prior_cleanup_failures = take_fallback_cleanup_failures();
    if !prior_cleanup_failures.is_empty() {
        return Err(format!(
            "prior fallback subprocess cleanup failed: {}",
            prior_cleanup_failures.join("; ")
        )
        .into());
    }
    let invocation = expected_invocation(program, arguments, environment, current_dir)?;
    let started = Instant::now();
    let (telemetry_path, reservation_path) = telemetry_path()?;
    let output_prefix = telemetry_path.with_extension("");
    let stdout_path = output_prefix.with_extension("stdout");
    let stderr_path = output_prefix.with_extension("stderr");
    let resource_path = output_prefix.with_extension("time");
    let process_group_path = output_prefix.with_extension("pgid");
    let stdout_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stdout_path)?;
    let stderr_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stderr_path)?;
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
    command
        .arg("/usr/bin/perl")
        .arg("-MPOSIX")
        .arg("-e")
        .arg(TARGET_GROUP_LAUNCHER)
        .arg(program)
        .args(arguments)
        .env_clear()
        .envs(&launcher_environment)
        .current_dir(&invocation.current_dir)
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));
    #[cfg(unix)]
    command.process_group(0);
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
    finish_managed_process(process, result, &stdout_path, &stderr_path, &resource_path)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        ffi::OsString,
        path::{Path, PathBuf},
        sync::atomic::AtomicU64,
        time::{Duration, Instant},
    };

    #[cfg(unix)]
    use std::os::unix::process::CommandExt;

    #[cfg(unix)]
    use rustix::io::Errno;

    use super::{
        allocate_telemetry_path, cleanup_error, combined_log, confirm_process_group_absent_with,
        digest_environment, identity_command_with_timeout, json_log, layer_budget,
        parse_combined_processes, parse_peak_rss, timed_for, timed_with_timeout,
        timed_with_timeout_and_grace, ProcessGroupState, ProcessKind, ProcessLog,
        DEFAULT_KILL_CONFIRMATION_TIMEOUT,
    };

    #[cfg(unix)]
    use super::{
        classify_signal_delivery, process_group_state, take_fallback_cleanup_failures,
        ManagedProcess, SignalDelivery,
    };
    use super::{parse_process_group_observation, ProcessGroupObservation};

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
    fn layer_budget_consumes_validated_runner_durations_without_profile_tables() {
        let runner = crate::RunnerContract {
            producer: "fixture".to_owned(),
            command: Vec::new(),
            configuration: BTreeMap::from([
                ("layer_timeout".to_owned(), "17m".to_owned()),
                ("finalization_reserve".to_owned(), "2m".to_owned()),
                ("compile_timeout".to_owned(), "73s".to_owned()),
                ("discovery_timeout".to_owned(), "11s".to_owned()),
                ("execution_timeout".to_owned(), "13s".to_owned()),
                ("termination_grace".to_owned(), "7s".to_owned()),
                ("kill_confirmation_timeout".to_owned(), "3s".to_owned()),
                ("receipt_finalization_allowance".to_owned(), "4s".to_owned()),
            ]),
            minimum_observed_checks: 1,
            require_peak_rss: true,
        };
        let budget = layer_budget("arbitrary-profile", "tests", &runner)
            .expect("manifest-driven producer budget")
            .expect("non-TLA layer has a scoped budget");
        let remaining = budget
            .finalization_deadline
            .checked_duration_since(Instant::now())
            .expect("deadline remains in the future");
        assert!(remaining <= Duration::from_secs(15 * 60));
        assert!(remaining > Duration::from_secs(14 * 60 + 59));
        assert_eq!(budget.finalization_reserve, Duration::from_secs(2 * 60));
        assert_eq!(budget.compile_timeout, Some(Duration::from_secs(73)));
        assert_eq!(budget.discovery_timeout, Some(Duration::from_secs(11)));
        assert_eq!(budget.execution_timeout, Some(Duration::from_secs(13)));
        assert_eq!(budget.policy.termination_grace, Duration::from_secs(7));
        assert_eq!(
            budget.policy.kill_confirmation_timeout,
            Duration::from_secs(3)
        );
        assert_eq!(
            budget.policy.receipt_finalization_allowance,
            Duration::from_secs(4)
        );
        assert!(layer_budget("pr", "tla", &runner)
            .expect("TLA remains explicit")
            .is_none());
        assert!(layer_budget("pr", "unknown", &runner).is_err());

        let mut drifted = runner;
        drifted.configuration.remove("termination_grace");
        assert!(layer_budget("pr", "tests", &drifted).is_err());
    }

    #[test]
    fn implicit_producer_process_without_a_layer_budget_fails_closed() {
        let error = timed_for(
            ProcessKind::SimulatorExecution,
            "printf",
            &[OsString::from("unreachable")],
            &super::base_environment(),
            Path::new("."),
        )
        .expect_err("unscoped producer subprocess must not start");
        assert!(error.to_string().contains("outside an active layer budget"));
    }

    #[test]
    #[cfg(unix)]
    fn identity_command_timeout_is_finite_and_retains_diagnostics() {
        let error = identity_command_with_timeout(
            "sh",
            &["-c", "printf identity-started; sleep 5"],
            Duration::from_millis(50),
        )
        .expect_err("stalled identity command must time out")
        .to_string();
        assert!(error.contains("timed_out=true"));
        assert!(error.contains("identity-started"));
    }

    #[test]
    fn process_group_observation_combines_membership_and_rss() {
        assert_eq!(
            parse_process_group_observation(" 42 100\n 7 5\n 42 23\n", 42)
                .expect("parse process inventory"),
            ProcessGroupObservation {
                state: ProcessGroupState::Alive,
                rss_kib: 123,
            }
        );
        assert_eq!(
            parse_process_group_observation(" 7 5\n", 42).expect("parse absent process group"),
            ProcessGroupObservation {
                state: ProcessGroupState::Absent,
                rss_kib: 0,
            }
        );
        assert!(parse_process_group_observation("42 100 extra\n", 42).is_err());
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

    #[cfg(unix)]
    #[test]
    fn managed_process_drop_kills_and_reaps_an_armed_group() {
        let mut command = std::process::Command::new("sh");
        command
            .args(["-c", "trap '' TERM; while :; do :; done"])
            .process_group(0);
        let child = command.spawn().expect("spawn isolated process group");
        let process_group = child.id();
        let mut process = ManagedProcess::new(child, DEFAULT_KILL_CONFIRMATION_TIMEOUT);
        process.set_target_group(process_group);

        drop(process);

        assert_eq!(
            process_group_state(process_group).expect("probe cleaned process group"),
            ProcessGroupState::Absent
        );
    }

    #[cfg(unix)]
    #[test]
    fn fallback_cleanup_failure_is_loud_and_rejected_by_the_next_producer_process() {
        const CHILD_ENV: &str = "RAFTER_TEST_CROSS_THREAD_CLEANUP_FAILURE";
        if std::env::var_os(CHILD_ENV).is_none() {
            let output = std::process::Command::new(
                std::env::current_exe().expect("resolve invariant test executable"),
            )
            .args([
                "--exact",
                "producer::process::tests::fallback_cleanup_failure_is_loud_and_rejected_by_the_next_producer_process",
                "--nocapture",
            ])
            .env(CHILD_ENV, "1")
            .output()
            .expect("run isolated cleanup-failure test");
            assert!(
                output.status.success(),
                "isolated cleanup-failure test failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        assert!(take_fallback_cleanup_failures().is_empty());
        std::thread::spawn(|| {
            let mut command = std::process::Command::new("sh");
            command.args(["-c", "exit 0"]).process_group(0);
            let child = command.spawn().expect("spawn isolated process group");
            let mut process = ManagedProcess::new(child, DEFAULT_KILL_CONFIRMATION_TIMEOUT);
            process.set_target_group(u32::MAX);
            drop(process);
        })
        .join()
        .expect("cleanup worker exits normally");

        let error = timed_with_timeout(
            "printf",
            &[OsString::from("unreachable")],
            &super::base_environment(),
            Path::new("."),
            Duration::from_secs(1),
        )
        .expect_err("the next process must surface the fallback cleanup diagnostic");
        assert!(error
            .to_string()
            .contains("prior fallback subprocess cleanup failed"));
        assert!(take_fallback_cleanup_failures().is_empty());
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
    fn telemetry_allocation_never_reuses_stale_process_receipts() {
        let directory = unique_test_path("telemetry-collision");
        std::fs::create_dir_all(&directory).expect("create telemetry scratch directory");
        let first_sequence = AtomicU64::new(0);
        let (first_path, first_reservation) =
            allocate_telemetry_path(&directory, 42, &first_sequence)
                .expect("reserve first telemetry path");
        let first_prefix = first_path.with_extension("");
        std::fs::remove_file(first_reservation).expect("release simulated crashed reservation");
        std::fs::write(first_prefix.with_extension("stdout"), b"stale")
            .expect("retain stale stdout receipt");

        let reused_process_sequence = AtomicU64::new(0);
        let (second_path, second_reservation) =
            allocate_telemetry_path(&directory, 42, &reused_process_sequence)
                .expect("skip stale telemetry path");
        assert_ne!(second_path, first_path);
        assert_eq!(second_path, directory.join("42-1.time"));

        std::fs::remove_file(second_reservation).expect("release second reservation");
        std::fs::remove_dir_all(directory).expect("remove telemetry scratch directory");
    }

    #[test]
    fn short_lived_children_always_produce_resource_telemetry() {
        for iteration in 0..32 {
            let output = timed_with_timeout(
                "sh",
                &[OsString::from("-c"), OsString::from("printf short")],
                &super::base_environment(),
                Path::new("."),
                Duration::from_secs(2),
            )
            .unwrap_or_else(|error| panic!("short child {iteration} lost telemetry: {error}"));
            assert!(output.status.success());
            assert!(!output.timed_out);
            assert_eq!(output.stdout, b"short");
            assert!(output.peak_rss_kib > 0);
        }
    }

    #[test]
    fn timed_child_is_killed_at_its_soft_timeout() {
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
        assert!(plain.starts_with("schema_version: 3\nlabel: timeout\ninvocation: {"));
        assert!(plain.contains("\"program\":\"sleep\""));
        let parsed = parse_combined_processes(&plain).expect("combined metrics parse");
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].timed_out);
        assert_ne!(parsed[0].exit_code, Some(0));
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
    fn timed_out_process_transcript_retains_output_and_timeout_classification() {
        let output = timed_with_timeout(
            "sh",
            &[
                OsString::from("-c"),
                OsString::from("printf retained-before-timeout; sleep 5"),
            ],
            &super::base_environment(),
            Path::new("."),
            Duration::from_millis(10),
        )
        .expect("timed-out process returns a replayable receipt");
        assert!(output.timed_out);
        assert_eq!(output.stdout, b"retained-before-timeout");

        let directory = unique_test_path("timeout-artifact");
        let bytes = combined_log("timeout-retention", &output).expect("frame timeout transcript");
        let artifact = crate::producer::artifact::write(
            &directory,
            Path::new("timeout.log"),
            "test-log",
            &bytes,
        )
        .expect("persist timeout transcript");
        let retained = std::fs::read_to_string(&artifact.path).expect("read timeout transcript");
        let [parsed] = parse_combined_processes(&retained)
            .expect("parse retained timeout transcript")
            .try_into()
            .expect("one retained process");
        assert!(parsed.timed_out);
        assert_eq!(parsed.stdout, "retained-before-timeout");
        assert_eq!(artifact.size_bytes, bytes.len() as u64);
        std::fs::remove_dir_all(directory).expect("remove timeout artifact directory");
    }

    #[test]
    fn combined_processes_preserve_failed_and_timed_out_semantic_statuses() {
        let source = concat!(
            "schema_version: 3\n",
            "label: test\n",
            "invocation: {\"program\":\"cargo\",\"program_sha256\":\"0000000000000000000000000000000000000000000000000000000000000000\",\"arguments\":[\"test\"],\"current_dir\":\"/workspace\",\"environment\":{},\"environment_sha256\":\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\"}\n",
            "exit_code: Some(0)\n",
            "timed_out: false\n",
            "duration_ms: 1\n",
            "peak_rss_kib: 1\n",
            "stdout_bytes: 2\n",
            "stderr_bytes: 0\n",
            "\n",
            "ok",
        );
        let parsed = parse_combined_processes(source).expect("successful receipt parses");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].stdout, "ok");
        assert_eq!(parsed[0].stderr, "");
        let failed = parse_combined_processes(&source.replace("Some(0)", "Some(1)"))
            .expect("failed semantic receipt remains parseable");
        assert_eq!(failed[0].exit_code, Some(1));
        let timed_out = parse_combined_processes(&source.replace("false", "true"))
            .expect("timed-out semantic receipt remains parseable");
        assert!(timed_out[0].timed_out);
        assert!(parse_combined_processes(&source.replace("Some(0)", "0")).is_err());
        assert!(
            parse_combined_processes(&source.replace("stdout_bytes: 2", "stdout_bytes: 20"))
                .is_err()
        );
        assert!(parse_combined_processes(&format!("{source}trailing junk")).is_err());
    }

    #[test]
    fn length_framing_preserves_process_log_tokens_inside_stdout() {
        let payload = "schema_version: 3\n\nstdout_bytes: 999\n--- stderr ---";
        let output = timed_with_timeout(
            "printf",
            &[OsString::from("%s"), OsString::from(payload)],
            &super::base_environment(),
            Path::new("."),
            Duration::from_secs(2),
        )
        .expect("capture adversarial stdout");
        let log = String::from_utf8(combined_log("framing", &output).expect("serialize log"))
            .expect("combined log is UTF-8");
        let [parsed] = parse_combined_processes(&log)
            .expect("length framing ignores payload tokens")
            .try_into()
            .expect("one process receipt");
        assert_eq!(parsed.stdout, payload);
        assert_eq!(parsed.stderr, "");
    }

    #[test]
    fn timeout_escalates_from_group_term_to_group_kill() {
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
    fn successful_leader_exit_does_not_hide_a_live_descendant() {
        let output = timed_with_timeout_and_grace(
            "sh",
            &[
                OsString::from("-c"),
                OsString::from("(trap '' TERM; sleep 5) & exit 0"),
            ],
            &super::base_environment(),
            Path::new("."),
            Duration::from_millis(50),
            Duration::from_millis(20),
        )
        .expect("the surviving descendant is detected and killed");
        let termination = output.termination.expect("termination receipt");
        assert!(output.timed_out);
        assert!(termination.term_signal_sent);
        assert!(termination.kill_signal_sent);
        assert!(output.duration < Duration::from_secs(2));
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
