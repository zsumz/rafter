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
    allocate_telemetry_path, cleanup_error, combined_detector_log, combined_log,
    confirm_process_group_absent_with, identity_command_with_timeout, json_log, layer_budget,
    parse_peak_rss, timed_for, timed_with_timeout, timed_with_timeout_after_bind,
    timed_with_timeout_and_grace, ProcessGroupState, ProcessKind,
    DEFAULT_KILL_CONFIRMATION_TIMEOUT,
};
use crate::evidence::format::process::{parse_combined_processes, ProcessLog};
use crate::provenance::invocation::digest_environment;

#[cfg(target_os = "linux")]
use super::{
    classify_signal_delivery, process_group_state, take_fallback_cleanup_failures,
    timed_with_timeout_and_policy_and_descriptors, ManagedProcess, ProcessPolicy, SignalDelivery,
};
#[cfg(all(unix, not(target_os = "linux")))]
use super::{
    classify_signal_delivery, process_group_state, take_fallback_cleanup_failures, ManagedProcess,
    SignalDelivery,
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
        digest_environment(&environment).expect("valid environment"),
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
        simulator_checks: BTreeMap::new(),
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
    let mut tla_runner = runner.clone();
    tla_runner.configuration.remove("layer_timeout");
    tla_runner
        .configuration
        .insert("total_timeout".to_owned(), "19m".to_owned());
    let tla_budget = layer_budget("pr", "tla", &tla_runner)
        .expect("TLA budget parses")
        .expect("TLA has a scoped whole-layer budget");
    let tla_execution = tla_budget
        .finalization_deadline
        .checked_duration_since(Instant::now())
        .expect("TLA execution deadline remains in the future");
    let tla_total = tla_budget
        .total_deadline
        .checked_duration_since(Instant::now())
        .expect("TLA total deadline remains in the future");
    assert!(tla_execution <= Duration::from_secs(17 * 60));
    assert!(tla_execution > Duration::from_secs(16 * 60 + 59));
    assert!(tla_total <= Duration::from_secs(19 * 60));
    assert!(tla_total > Duration::from_secs(18 * 60 + 59));
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
        parse_process_group_observation(" 42 100 S\n 7 5 R+\n 42 23 D\n", 42)
            .expect("parse process inventory"),
        ProcessGroupObservation {
            state: ProcessGroupState::Alive,
            rss_kib: 123,
        }
    );
    assert_eq!(
        parse_process_group_observation(" 7 5 S\n", 42).expect("parse absent process group"),
        ProcessGroupObservation {
            state: ProcessGroupState::Absent,
            rss_kib: 0,
        }
    );
    assert_eq!(
        parse_process_group_observation("42 0 Z\n42 0 Z+\n", 42)
            .expect("zombies do not keep a process group alive"),
        ProcessGroupObservation {
            state: ProcessGroupState::Absent,
            rss_kib: 0,
        }
    );
    assert!(parse_process_group_observation("42 100\n", 42).is_err());
    assert!(parse_process_group_observation("42 100 S extra\n", 42).is_err());
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
    let error = confirm_process_group_absent_with(Duration::ZERO, || Ok(ProcessGroupState::Alive))
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
    let (first_path, first_reservation) = allocate_telemetry_path(&directory, 42, &first_sequence)
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
fn telemetry_paths_remain_valid_from_a_nested_child_working_directory() {
    let working_directory = unique_test_path("nested-child-working-directory");
    std::fs::create_dir_all(&working_directory).expect("create nested working directory");

    let output = timed_with_timeout(
        "sh",
        &[
            OsString::from("-c"),
            OsString::from("printf nested; printf nested-err >&2"),
        ],
        &super::base_environment(),
        &working_directory,
        Duration::from_secs(2),
    )
    .expect("nested child produces absolute-path telemetry");

    assert!(
        output.status.success(),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!output.timed_out);
    assert_eq!(output.stdout, b"nested");
    assert_eq!(output.stderr, b"nested-err");
    assert!(output.peak_rss_kib > 0);
    assert_eq!(
        output.invocation.current_dir,
        std::fs::canonicalize(&working_directory)
            .expect("nested working directory canonicalizes")
            .to_string_lossy()
    );
    std::fs::remove_dir_all(working_directory).expect("remove nested working directory");
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
        digest_environment(&environment).expect("valid environment")
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
    assert!(parsed[0].detector_challenge.is_none());
    let challenge = "5a".repeat(32);
    let detector = String::from_utf8(
        combined_detector_log("timeout", &output, &challenge).expect("detector log serializes"),
    )
    .expect("detector process log is UTF-8");
    let detector = parse_combined_processes(&detector).expect("detector process log parses");
    assert_eq!(
        detector[0].detector_challenge.as_deref(),
        Some(challenge.as_str())
    );
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

    let unique = unique_test_path("timeout-artifact");
    let directory = PathBuf::from("target/rafter-invariants/test-artifacts").join(
        unique
            .file_name()
            .expect("timeout artifact path has a file name"),
    );
    let bytes = combined_log("timeout-retention", &output).expect("frame timeout transcript");
    let artifact =
        crate::producer::artifact::write(&directory, Path::new("timeout.log"), "test-log", &bytes)
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
    assert!(parsed[0].detector_challenge.is_none());
    let challenge = "5a".repeat(32);
    let detector_source = source
        .replacen("schema_version: 3", "schema_version: 4", 1)
        .replacen(
            "\nexit_code:",
            &format!("\ndetector_challenge: {challenge}\nexit_code:"),
            1,
        );
    let detector =
        parse_combined_processes(&detector_source).expect("detector process receipt parses");
    assert_eq!(
        detector[0].detector_challenge.as_deref(),
        Some(challenge.as_str())
    );
    assert!(
        parse_combined_processes(&detector_source.replace(&challenge, &"A".repeat(64))).is_err()
    );
    let failed = parse_combined_processes(&source.replace("Some(0)", "Some(1)"))
        .expect("failed semantic receipt remains parseable");
    assert_eq!(failed[0].exit_code, Some(1));
    let timed_out = parse_combined_processes(&source.replace("false", "true"))
        .expect("timed-out semantic receipt remains parseable");
    assert!(timed_out[0].timed_out);
    assert!(parse_combined_processes(&source.replace("Some(0)", "0")).is_err());
    assert!(
        parse_combined_processes(&source.replace("stdout_bytes: 2", "stdout_bytes: 20")).is_err()
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
fn production_process_policy_rejects_hosts_without_descriptor_exec() {
    assert!(super::process_execution_policy(true, false).is_ok());
    assert!(super::process_execution_policy(false, true).is_ok());
    let error = super::process_execution_policy(false, false)
        .expect_err("production execution without descriptor exec must fail closed");
    assert!(error
        .to_string()
        .contains("requires Linux descriptor-based executable launch"));
}

#[cfg(unix)]
#[test]
fn working_directory_replacement_is_rejected_after_descriptor_bound_execution() {
    let root = unique_test_path("working-directory-binding");
    let moved = root.with_extension("original");
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&moved);
    std::fs::create_dir_all(&root).expect("create original working directory");

    let error = timed_with_timeout_after_bind(
        "/bin/pwd",
        &[],
        &super::base_environment(),
        &root,
        Duration::from_secs(2),
        || {
            std::fs::rename(&root, &moved).expect("move bound working directory");
            std::fs::create_dir_all(&root).expect("install replacement working directory");
        },
    )
    .expect_err("receipt must reject a working-directory path replacement");
    assert!(error
        .to_string()
        .contains("producer directory changed after it was opened"));

    std::fs::remove_dir_all(root).expect("remove replacement working directory");
    std::fs::remove_dir_all(moved).expect("remove original working directory");
}

#[cfg(target_os = "linux")]
#[test]
fn executable_receipt_and_launch_share_the_same_open_file_after_path_replacement() {
    let root = std::env::temp_dir().join(format!(
        "rafter-invariants-executable-binding-{}-{}",
        std::process::id(),
        super::TELEMETRY_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let moved = root.with_extension("original");
    let _ = std::fs::remove_file(&root);
    let _ = std::fs::remove_file(&moved);
    std::fs::copy("/bin/echo", &root).expect("copy original executable");

    let output = timed_with_timeout_after_bind(
        root.to_str().expect("UTF-8 executable path"),
        &[OsString::from("bound-executable")],
        &super::base_environment(),
        Path::new("."),
        Duration::from_secs(2),
        || {
            std::fs::rename(&root, &moved).expect("move bound executable");
            std::fs::copy("/usr/bin/false", &root).expect("install replacement executable");
        },
    )
    .expect("launch uses the descriptor opened before path replacement");

    assert!(
        output.status.success(),
        "stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"bound-executable\n");
    let original = super::expected_invocation(
        moved.to_str().expect("UTF-8 moved executable path"),
        &[OsString::from("bound-executable")],
        &super::base_environment(),
        Path::new("."),
    )
    .expect("hash moved original executable");
    assert_eq!(output.invocation.program_sha256, original.program_sha256);

    std::fs::remove_file(root).expect("remove replacement executable");
    std::fs::remove_file(moved).expect("remove original executable");
}

#[cfg(target_os = "linux")]
#[test]
fn inherited_directory_binding_survives_path_replacement_through_launcher_chain() {
    use std::os::unix::fs::symlink;

    let repository_test_path = |label| {
        PathBuf::from("target/rafter-invariants/process-tests").join(
            unique_test_path(label)
                .file_name()
                .expect("temporary fixture has a file name"),
        )
    };
    let root = repository_test_path("held-directory-binding");
    let moved = repository_test_path("held-directory-binding-original");
    let external = repository_test_path("held-directory-binding-external");
    let _ = std::fs::remove_file(&root);
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&moved);
    let _ = std::fs::remove_dir_all(&external);
    std::fs::create_dir_all(&root).expect("create held directory fixture");
    std::fs::write(root.join("checkpoint"), b"held-inode").expect("write held checkpoint fixture");
    std::fs::create_dir_all(&external).expect("create replacement fixture");
    std::fs::write(external.join("checkpoint"), b"replacement")
        .expect("write replacement checkpoint fixture");

    let held = crate::execution::filesystem::HeldDirectory::open(&root)
        .expect("hold checkpoint directory");
    let binding = held.bind_for_child().expect("bind directory for child");
    let mut environment = super::base_environment();
    environment.insert(
        "BOUND_DIRECTORY".to_owned(),
        binding.path().to_string_lossy().into_owned(),
    );

    std::fs::rename(&root, &moved).expect("move original checkpoint directory");
    let external = std::fs::canonicalize(&external).expect("canonical replacement directory");
    symlink(&external, &root).expect("replace checkpoint path with external symlink");

    let output = timed_with_timeout_and_policy_and_descriptors(
        "sh",
        &[
            OsString::from("-c"),
            OsString::from("cat \"$BOUND_DIRECTORY/checkpoint\""),
        ],
        &environment,
        Path::new("."),
        Duration::from_secs(2),
        ProcessPolicy::default(),
        &[binding.descriptor()],
    )
    .expect("launch child through inherited descriptor");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"held-inode");

    std::fs::remove_file(&root).expect("remove replacement symlink");
    std::fs::remove_dir_all(&moved).expect("remove original fixture");
    std::fs::remove_dir_all(&external).expect("remove replacement fixture");
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
    PathBuf::from("target/rafter-invariants/process-tests").join(format!(
        "rafter-invariants-{label}-{}-{sequence}",
        std::process::id()
    ))
}
