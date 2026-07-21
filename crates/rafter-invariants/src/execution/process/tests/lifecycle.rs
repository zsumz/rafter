//! Complete launch, telemetry, timeout, escalation, and fallback-cleanup scenarios.

use std::{collections::BTreeSet, path::Path, time::Duration};

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

#[cfg(target_os = "linux")]
use crate::execution::filesystem::HeldDirectory;

use super::{
    super::{
        base_environment, clear_signal_attempts, expose_next_target_lifetime_lease,
        induce_fallback_cleanup_failure, take_signal_attempts, FinalizationPolicy, ProcessSignal,
    },
    support::{
        run_shell, run_shell_with_artifact_paths, run_shell_with_finalization, unique_test_path,
    },
};

#[test]
fn receipt_output_over_the_reviewed_limit_is_retained_and_rejected() {
    let finalization = FinalizationPolicy::bounded(Duration::from_secs(5)).with_output_limits(4, 4);
    let error = run_shell_with_finalization(
        "printf 12345",
        &base_environment(),
        Path::new("."),
        Duration::from_secs(2),
        Duration::from_millis(20),
        finalization,
    )
    .expect_err("oversized stdout must fail finalization");
    let error = error.to_string();
    assert!(error.contains("exceeding the 4-byte limit"));
    assert!(error.contains("retained subprocess stdout"));
    assert!(error.contains("resource telemetry"));
}

#[test]
fn expired_receipt_finalization_deadline_is_retained_and_rejected() {
    let error = run_shell_with_finalization(
        "printf bounded",
        &base_environment(),
        Path::new("."),
        Duration::from_secs(2),
        Duration::from_millis(20),
        FinalizationPolicy::bounded(Duration::ZERO),
    )
    .expect_err("expired finalization deadline must fail closed");
    let error = error.to_string();
    assert!(error.contains("deadline expired during process receipt finalization"));
    assert!(error.contains("retained subprocess stdout"));
    assert!(error.contains("resource telemetry"));
}

#[test]
fn fallback_cleanup_failure_is_scoped_to_its_owning_execution() {
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let cleanup_barrier = barrier.clone();
    let cleanup = std::thread::spawn(move || {
        cleanup_barrier.wait();
        induce_fallback_cleanup_failure()
    });

    barrier.wait();
    let output = run_shell(
        "printf independent",
        &base_environment(),
        Path::new("."),
        Duration::from_secs(1),
        Duration::from_millis(20),
    )
    .expect("another execution cannot inherit a foreign cleanup failure");
    assert_eq!(output.stdout, b"independent");
    let failures = cleanup.join().expect("cleanup worker exits normally");
    assert!(failures
        .iter()
        .any(|failure| failure.contains("remained owned after emergency cleanup")));
}

#[test]
fn short_lived_children_always_produce_resource_telemetry() {
    for iteration in 0..32 {
        let output = run_shell(
            "printf short",
            &base_environment(),
            Path::new("."),
            Duration::from_secs(2),
            Duration::from_millis(20),
        )
        .unwrap_or_else(|error| panic!("short child {iteration} lost telemetry: {error}"));
        assert!(output.status.success());
        assert!(!output.timed_out);
        assert_eq!(output.stdout, b"short");
        assert!(output.peak_rss_kib > 0);
    }
}

#[test]
fn successful_execution_retains_one_complete_replay_set() {
    let (output, artifacts) = run_shell_with_artifact_paths(
        "printf replay-out; printf replay-err >&2",
        &base_environment(),
        Path::new("."),
        Duration::from_secs(2),
        Duration::from_millis(20),
    )
    .expect("complete replayable execution");
    assert!(output.status.success());

    let retained = artifacts.all();
    assert!(retained.iter().all(|path| path.is_file()));
    let prefixes = retained
        .iter()
        .map(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.rsplit_once('.'))
                .expect("artifact filename and extension")
                .0
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(prefixes.len(), 1, "artifact prefixes: {prefixes:?}");
    let extensions = retained
        .iter()
        .map(|path| {
            path.extension()
                .and_then(|extension| extension.to_str())
                .expect("artifact extension")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        extensions,
        ["pgid", "reserve", "stderr", "stdout", "time"]
            .into_iter()
            .collect()
    );
}

#[test]
fn target_environment_is_isolated_from_launcher_control() {
    let mut environment = base_environment();
    environment.insert("PERL5OPT".to_owned(), "-MNo::Such::Module".to_owned());
    let output = run_shell(
        "test \"$PERL5OPT\" = '-MNo::Such::Module'; printf isolated",
        &environment,
        Path::new("."),
        Duration::from_secs(2),
        Duration::from_millis(20),
    )
    .expect("launcher ignores target-scoped Perl configuration");
    assert!(output.status.success());
    assert_eq!(output.stdout, b"isolated");
}

#[test]
fn reserved_launcher_environment_keys_are_rejected() {
    let mut environment = base_environment();
    environment.insert(
        "RAFTER_INVARIANT_RESOURCE_FD".to_owned(),
        "forged".to_owned(),
    );
    let error = run_shell(
        "printf unreachable",
        &environment,
        Path::new("."),
        Duration::from_secs(2),
        Duration::from_millis(20),
    )
    .expect_err("reserved launcher key must fail before spawn");
    assert!(error
        .to_string()
        .contains("target environment uses reserved launcher key"));
}

#[cfg(target_os = "linux")]
#[test]
fn unrelated_parent_capability_is_closed_before_target_exec() {
    let unrelated = unique_test_path("unrelated-descriptor");
    std::fs::create_dir_all(&unrelated).expect("create unrelated descriptor fixture");
    let held = HeldDirectory::open(&unrelated).expect("hold unrelated directory");
    let child_capability = held.bind_for_child().expect("bind unrelated directory");
    rustix::io::fcntl_setfd(child_capability.descriptor(), rustix::io::FdFlags::empty())
        .expect("simulate unrelated non-CLOEXEC parent capability");
    let mut environment = base_environment();
    environment.insert(
        "UNRELATED_FD".to_owned(),
        child_capability.descriptor().as_raw_fd().to_string(),
    );
    environment.insert(
        "UNRELATED_PATH".to_owned(),
        std::fs::canonicalize(&unrelated)
            .expect("canonicalize unrelated directory")
            .to_string_lossy()
            .into_owned(),
    );

    let output = run_shell(
        r#"observed=$(/usr/bin/readlink "/proc/self/fd/$UNRELATED_FD" 2>/dev/null || true)
case "$observed" in
  *"$UNRELATED_PATH"*) exit 71 ;;
esac
printf confined"#,
        &environment,
        Path::new("."),
        Duration::from_secs(2),
        Duration::from_millis(20),
    )
    .expect("run with an unrelated parent capability");
    assert!(output.status.success());
    assert_eq!(output.stdout, b"confined");

    drop(child_capability);
    std::fs::remove_dir_all(unrelated).expect("remove unrelated descriptor fixture");
}

#[test]
fn telemetry_paths_remain_valid_from_a_nested_child_working_directory() {
    let working_directory = unique_test_path("nested-child-working-directory");
    std::fs::create_dir_all(&working_directory).expect("create nested working directory");

    let output = run_shell(
        "printf nested; printf nested-err >&2",
        &base_environment(),
        &working_directory,
        Duration::from_secs(2),
        Duration::from_millis(20),
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
    std::fs::remove_dir_all(working_directory).expect("remove nested working directory");
}

#[test]
fn timeout_escalates_from_group_term_to_group_kill() {
    clear_signal_attempts();
    let output = run_shell(
        "trap '' TERM; while :; do :; done",
        &base_environment(),
        Path::new("."),
        Duration::from_millis(10),
        Duration::from_millis(20),
    )
    .expect("stubborn process group is killed");
    assert!(output.timed_out);
    assert!(output.termination.process_group);
    assert!(output.termination.term_signal_sent);
    assert!(output.termination.kill_signal_sent);
    assert_eq!(output.termination.grace, Duration::from_millis(20));
    assert!(
        output.peak_rss_kib > 0,
        "timeout telemetry remained authoritative"
    );
    let attempts = take_signal_attempts();
    let term_groups = attempts
        .iter()
        .filter_map(|(group, signal)| (*signal == ProcessSignal::Term).then_some(*group))
        .collect::<Vec<_>>();
    assert_eq!(
        term_groups.len(),
        1,
        "target timeout must send exactly one SIGTERM: {attempts:?}"
    );
    let target_group = term_groups[0];
    let target_attempts = attempts
        .iter()
        .filter_map(|(group, signal)| (*group == target_group).then_some(*signal))
        .collect::<Vec<_>>();
    assert_eq!(
        target_attempts,
        [ProcessSignal::Term, ProcessSignal::Kill],
        "target timeout signal attempts: {attempts:?}"
    );
}

#[test]
fn timeout_term_cleans_up_descendants_without_escalation() {
    let marker = unique_test_path("descendant-term");
    let mut environment = base_environment();
    environment.insert(
        "MARKER_PATH".to_owned(),
        marker.to_string_lossy().into_owned(),
    );
    let output = run_shell(
        "(trap 'printf term > \"$MARKER_PATH\"; exit 0' TERM; while :; do sleep 1; done) & wait",
        &environment,
        Path::new("."),
        Duration::from_millis(200),
        Duration::from_secs(2),
    )
    .expect("TERM cleans up the leader and descendant process");
    assert!(output.timed_out);
    assert!(output.termination.term_signal_sent);
    assert!(!output.termination.kill_signal_sent);
    assert_eq!(
        std::fs::read_to_string(&marker).expect("descendant records TERM"),
        "term"
    );
    std::fs::remove_file(marker).expect("remove TERM marker");
}

#[test]
fn timeout_kills_descendants_after_the_group_leader_exits() {
    let output = run_shell(
        "trap 'exit 0' TERM; (trap '' TERM; while :; do :; done) & wait",
        &base_environment(),
        Path::new("."),
        Duration::from_millis(10),
        Duration::from_millis(20),
    )
    .expect("surviving descendant is killed with its process group");
    assert!(output.timed_out);
    assert!(output.termination.term_signal_sent);
    assert!(output.termination.kill_signal_sent);
}

#[test]
fn successful_leader_exit_does_not_hide_a_live_descendant() {
    let output = run_shell(
        "(trap '' TERM; sleep 5) & exit 0",
        &base_environment(),
        Path::new("."),
        Duration::from_millis(50),
        Duration::from_millis(20),
    )
    .expect("the surviving descendant is detected and killed");
    assert!(output.timed_out);
    assert!(output.termination.term_signal_sent);
    assert!(output.termination.kill_signal_sent);
    assert!(output.duration < Duration::from_secs(2));
}

#[test]
fn target_lifetime_lease_release_with_a_live_descendant_is_a_harness_error() {
    let marker = unique_test_path("released-target-lifetime-lease");
    std::fs::create_dir_all(marker.parent().expect("marker parent")).expect("create marker parent");
    let _ = std::fs::remove_file(&marker);
    expose_next_target_lifetime_lease();
    let script = format!(
        r#"(eval "exec ${{RAFTER_TEST_TARGET_LIFETIME_LEASE_FD}}>&-"; printf ready > '{}'; sleep 5) & while [ ! -f '{}' ]; do :; done; exit 0"#,
        marker.display(),
        marker.display(),
    );
    let outcome = run_shell(
        &script,
        &base_environment(),
        Path::new("."),
        Duration::from_secs(1),
        Duration::from_millis(20),
    );
    std::fs::remove_file(marker).expect("remove live-descendant marker");
    let error =
        outcome.expect_err("a descendant that discards the lifetime lease must fail closed");
    assert!(error
        .to_string()
        .contains("target lifetime lease was released while the process observer reported live target members"));
}
