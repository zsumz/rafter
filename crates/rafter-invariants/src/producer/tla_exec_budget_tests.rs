//! Shared TLA+ execution-budget and finalization-reserve scenarios.

use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

use serde_json::Value;

use super::{
    complete_main_execution, configured_budget_duration, maximum_qualification_time,
    mutation_suite_timeout, probe_timeout, process, DetectorProbes, ExecutionBudget,
    MainCompletion, TlcRun, FINALIZATION_RESERVE_KEY, QUALIFICATION_PHASE_COUNT, TOTAL_TIMEOUT_KEY,
};

fn pr_budget() -> BTreeMap<String, String> {
    BTreeMap::from([
        (TOTAL_TIMEOUT_KEY.to_owned(), "338m".to_owned()),
        (FINALIZATION_RESERVE_KEY.to_owned(), "2m".to_owned()),
    ])
}

#[test]
fn shared_pr_budget_reduces_the_main_timeout_and_preserves_the_reserve() {
    let started = std::time::Instant::now();
    let budget = ExecutionBudget::at("pr", &pr_budget(), started).expect("valid PR budget");

    assert_eq!(
        budget.phase_timeout_at(started, probe_timeout("pr")),
        Some(probe_timeout("pr"))
    );
    assert_eq!(
        budget.phase_timeout_at(
            started + Duration::from_secs(11 * 60),
            Duration::from_secs(325 * 60),
        ),
        Some(Duration::from_secs(325 * 60))
    );
    assert_eq!(
        budget.phase_timeout_at(
            started + Duration::from_secs(336 * 60),
            Duration::from_secs(300 * 60),
        ),
        None
    );
}

#[test]
fn execution_budget_uses_the_layer_guard_clock_started_before_tla_setup() {
    let (_, manifest) = crate::tests::loaded();
    let runner = &manifest.profiles["pr"].runners["tla"];
    let _guard =
        process::LayerBudgetGuard::enter("pr", "tla", runner).expect("install TLA layer budget");
    let expected =
        process::active_layer_deadlines("pr", "tla").expect("read active TLA layer deadlines");

    let observed = ExecutionBudget::from_configuration("pr", &runner.configuration)
        .expect("bind TLA execution to the active layer clock");

    assert_eq!(observed.execution_deadline, expected.0);
    assert_eq!(observed.total_deadline, expected.1);
}

#[test]
fn every_budget_requires_a_paired_reserve_and_time_for_the_main_run() {
    let started = std::time::Instant::now();
    assert!(ExecutionBudget::at("pr", &BTreeMap::new(), started).is_err());
    assert!(ExecutionBudget::at(
        "pr",
        &BTreeMap::from([(TOTAL_TIMEOUT_KEY.to_owned(), "120m".to_owned())]),
        started,
    )
    .is_err());
    assert!(ExecutionBudget::at(
        "pr",
        &BTreeMap::from([
            (TOTAL_TIMEOUT_KEY.to_owned(), "9m".to_owned()),
            (FINALIZATION_RESERVE_KEY.to_owned(), "2m".to_owned()),
        ]),
        started,
    )
    .is_err());
    assert!(ExecutionBudget::at("nightly", &BTreeMap::new(), started).is_err());
}

#[test]
fn scheduled_profiles_share_a_bounded_execution_deadline() {
    let started = std::time::Instant::now();
    let budget = ExecutionBudget::at(
        "weekly",
        &BTreeMap::from([
            (TOTAL_TIMEOUT_KEY.to_owned(), "350m".to_owned()),
            (FINALIZATION_RESERVE_KEY.to_owned(), "10m".to_owned()),
        ]),
        started,
    )
    .expect("weekly profile has a bounded shared deadline");
    assert_eq!(
        budget.phase_timeout_at(
            started + Duration::from_secs(339 * 60),
            probe_timeout("weekly"),
        ),
        Some(Duration::from_secs(60))
    );
    assert_eq!(
        budget.phase_timeout_at(
            started + Duration::from_secs(340 * 60),
            probe_timeout("weekly"),
        ),
        None
    );
}

#[test]
fn pr_qualification_caps_make_room_for_frontier_exhaustion_without_changing_scheduled_caps() {
    assert_eq!(QUALIFICATION_PHASE_COUNT, 12);
    assert_eq!(probe_timeout("pr"), Duration::from_secs(15));
    assert_eq!(mutation_suite_timeout("pr"), Duration::from_secs(4 * 60));
    assert_eq!(
        maximum_qualification_time("pr"),
        Some(Duration::from_secs(7 * 60))
    );
    assert_eq!(probe_timeout("nightly"), Duration::from_secs(2 * 60));
    assert_eq!(
        mutation_suite_timeout("nightly"),
        Duration::from_secs(8 * 60)
    );
    assert_eq!(
        maximum_qualification_time("weekly"),
        Some(Duration::from_secs(32 * 60))
    );
}

#[test]
fn workflow_caps_cover_the_exact_tla_phase_inventory() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest: Value = serde_json::from_str(
        &fs::read_to_string(root.join("verification/raft-invariant-profiles.json"))
            .expect("read invariant profile manifest"),
    )
    .expect("parse invariant profile manifest");

    for (profile, workflow, step_name, following_capped_step) in [
        (
            "pr",
            ".github/workflows/ci.yml",
            "Produce TLA+ evidence",
            None,
        ),
        (
            "nightly",
            ".github/workflows/nightly.yml",
            "Produce source-bound nightly TLA+ evidence",
            None,
        ),
        (
            "weekly",
            ".github/workflows/weekly.yml",
            "Produce source-bound weekly TLA+ evidence",
            None,
        ),
    ] {
        assert_profile_workflow_budget(
            &root,
            &manifest,
            profile,
            workflow,
            step_name,
            following_capped_step,
        );
    }
}

#[test]
fn weekly_tla_job_uses_a_runner_that_can_exceed_the_hosted_six_hour_cap() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let workflow = fs::read_to_string(root.join(".github/workflows/weekly.yml"))
        .expect("read weekly workflow");
    let job_cap = workflow_timeout_after(&workflow, "  invariants-tla:", "    timeout-minutes: ");
    assert!(job_cap > Duration::from_secs(6 * 60 * 60));
    assert!(workflow_job_block(&workflow, "  invariants-tla:")
        .lines()
        .any(|line| line == "    runs-on: [self-hosted, linux, X64]"));
}

#[test]
fn nightly_tla_job_uses_exact_compatible_checkpointing_on_self_hosted_linux() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let workflow = fs::read_to_string(root.join(".github/workflows/nightly.yml"))
        .expect("read nightly workflow");
    let job = workflow_job_block(&workflow, "  invariants-tla:");

    assert!(job
        .lines()
        .any(|line| line == "    runs-on: [self-hosted, linux, X64]"));
    for required in [
        "Restore exact-compatible nightly TLC checkpoint",
        "target/rafter-invariants/tla-checkpoint/nightly",
        "tla-nightly-checkpoint-v1-",
        "cargo run --locked -p rafter-invariants -- run --profile nightly --layer tla",
        "Save exact-compatible nightly TLC checkpoint",
        "specs/tla/raft/RaftNightly.cfg",
    ] {
        assert!(
            job.contains(required),
            "nightly source-bound TLA job omitted: {required}"
        );
    }
}

#[test]
fn main_counterexample_abandons_checkpoint_before_expired_finalization() {
    let profile = format!("complete-main-counterexample-{}", std::process::id());
    let output_dir = Path::new("target/rafter-invariants/checkpoint-test-artifacts").join(&profile);
    let checkpoint_root = Path::new("target/rafter-invariants/tla-checkpoint").join(&profile);
    let _ = fs::remove_dir_all(&output_dir);
    let _ = fs::remove_dir_all(&checkpoint_root);
    let configuration = BTreeMap::from([
        ("config".to_owned(), "Raft.cfg".to_owned()),
        ("checkpoint_minutes".to_owned(), "30".to_owned()),
    ]);
    let inputs = [
        "tla-tool",
        "tla-spec",
        "tla-trace-spec",
        "tla-detector-spec",
        "tla-runner",
        "tla-tool-asset-id",
        "tla-tool-checksums",
        "tla-config",
        "tla-trace-config",
        "tla-detector-config",
    ]
    .into_iter()
    .map(artifact)
    .collect::<Vec<_>>();
    let preparation = crate::producer::tla_checkpoint::prepare(
        &profile,
        "abc123",
        &configuration,
        &inputs,
        &output_dir,
        Instant::now() + Duration::from_secs(30),
    )
    .expect("prepare checkpoint");
    let trace = TlcRun {
        output: process_output(0, Vec::new()),
        artifact: artifact("tla-trace-log"),
    };
    let main = TlcRun {
        output: process_output(
            12,
            b"@!@!@STARTMSG 2110:1 @!@!@\nInvariant ElectionSafety is violated.\n@!@!@ENDMSG 2110 @!@!@\n\
@!@!@STARTMSG 2199:0 @!@!@\n2 states generated, 2 distinct states found, 0 states left on queue.\n@!@!@ENDMSG 2199 @!@!@\n\
@!@!@STARTMSG 2194:0 @!@!@\nThe depth of the complete state graph search is 2.\n@!@!@ENDMSG 2194 @!@!@\n\
@!@!@STARTMSG 2186:0 @!@!@\nFinished.\n@!@!@ENDMSG 2186 @!@!@\n\
@!@!@STARTMSG 2200:0 @!@!@\nProgress(3) at"
                .to_vec(),
        ),
        artifact: artifact("tla-log"),
    };

    let execution = complete_main_execution(
        MainCompletion {
            trace: &trace,
            detectors: DetectorProbes::default(),
            artifacts: inputs,
            checkpoint: Some(preparation),
            checkpoint_report: None,
            output_dir: &output_dir,
            total_deadline: Instant::now(),
        },
        main,
    )
    .expect("counterexample must not enter checkpoint finalization");
    assert_eq!(
        execution
            .main
            .as_ref()
            .and_then(|summary| summary.violated_invariant.as_deref()),
        Some("ElectionSafety")
    );
    assert!(!execution.artifacts.iter().any(|artifact| {
        matches!(
            artifact.kind.as_str(),
            "tla-checkpoint-contract" | "tla-checkpoint-inventory"
        )
    }));
    let _ = fs::remove_dir_all(output_dir);
    let _ = fs::remove_dir_all(checkpoint_root);
}

fn artifact(kind: &str) -> crate::ArtifactRef {
    crate::ArtifactRef {
        kind: kind.to_owned(),
        path: format!("artifacts/{kind}"),
        sha256: format!("{:0>64}", kind.len()),
        size_bytes: 1,
    }
}

fn process_output(exit_code: i32, stdout: Vec<u8>) -> process::ProcessOutput {
    let status = Command::new("sh")
        .args(["-c", &format!("exit {exit_code}")])
        .status()
        .expect("create fixture exit status");
    process::ProcessOutput {
        invocation: crate::InvocationReceipt {
            program: "fixture".to_owned(),
            program_sha256: "0".repeat(64),
            arguments: vec!["fixture".to_owned()],
            current_dir: "/".to_owned(),
            environment: BTreeMap::new(),
            environment_sha256: crate::provenance::invocation::digest_environment(&BTreeMap::new())
                .expect("valid fixture environment"),
            launchers: crate::receipt::fixture_launchers(false),
        },
        status,
        stdout,
        stderr: Vec::new(),
        duration: Duration::from_millis(1),
        peak_rss_kib: 1,
        timed_out: false,
        termination: None,
    }
}

fn assert_profile_workflow_budget(
    root: &Path,
    manifest: &Value,
    profile: &str,
    workflow: &str,
    step_name: &str,
    following_capped_step: Option<&str>,
) {
    const MIN_FINALIZATION_RESERVE: Duration = Duration::from_secs(2 * 60);
    const MIN_PR_SETUP_WINDOW: Duration = Duration::from_secs(4 * 60);
    const MIN_SCHEDULED_SETUP_WINDOW: Duration = Duration::from_secs(10 * 60);
    const MIN_STEP_HEADROOM: Duration = Duration::from_secs(10 * 60);
    const MIN_JOB_HEADROOM: Duration = Duration::from_secs(10 * 60);
    const MIN_SCHEDULED_JOB_HEADROOM: Duration = Duration::from_secs(30 * 60);
    const MAX_GITHUB_STEP_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);

    let configuration = profile_tla_configuration(manifest, profile);
    let qualification_phases =
        maximum_qualification_time(profile).expect("profile qualification phase duration");
    let main = required_budget_duration(&configuration, "soft_timeout");
    let inventory = qualification_phases
        .checked_add(main)
        .expect("phase inventory duration");
    let total = required_budget_duration(&configuration, TOTAL_TIMEOUT_KEY);
    let reserve = required_budget_duration(&configuration, FINALIZATION_RESERVE_KEY);
    let execution_window = total
        .checked_sub(reserve)
        .expect("total timeout exceeds finalization reserve");
    let workflow_source = fs::read_to_string(root.join(workflow)).expect("read workflow");
    let step_cap = workflow_timeout_after(
        &workflow_source,
        &format!("      - name: {step_name}"),
        "        timeout-minutes: ",
    );
    let job_cap = workflow_timeout_after(
        &workflow_source,
        "  invariants-tla:",
        "    timeout-minutes: ",
    );
    let following_cap = following_capped_step.map_or(Duration::ZERO, |step| {
        workflow_timeout_after(
            &workflow_source,
            &format!("      - name: {step}"),
            "        timeout-minutes: ",
        )
    });

    assert!(
        step_cap <= MAX_GITHUB_STEP_TIMEOUT,
        "{profile} step cap {step_cap:?} exceeds GitHub's six-hour limit"
    );
    assert!(
        inventory <= step_cap,
        "{profile} maximum phase inventory {inventory:?} exceeds step cap {step_cap:?}"
    );
    assert!(
        reserve >= MIN_FINALIZATION_RESERVE,
        "{profile} internal finalization reserve {reserve:?} is too small"
    );
    assert!(
        step_cap
            .checked_sub(total)
            .expect("step cap exceeds total layer deadline")
            >= MIN_STEP_HEADROOM,
        "{profile} total deadline needs {MIN_STEP_HEADROOM:?} before the step cap"
    );
    let required_job_headroom = if profile == "pr" {
        MIN_JOB_HEADROOM.saturating_add(following_cap)
    } else {
        MIN_SCHEDULED_JOB_HEADROOM
    };
    assert!(
        job_cap
            .checked_sub(step_cap)
            .expect("job cap exceeds the step cap")
            >= required_job_headroom,
        "{profile} job headroom is smaller than {required_job_headroom:?}"
    );
    let setup_window = execution_window
        .checked_sub(inventory)
        .expect("execution window covers the complete phase inventory");
    let required_setup_window = if profile == "pr" {
        MIN_PR_SETUP_WINDOW
    } else {
        MIN_SCHEDULED_SETUP_WINDOW
    };
    assert!(
        setup_window >= required_setup_window,
        "{profile} setup window {setup_window:?} is too small"
    );
}

fn profile_tla_configuration(manifest: &Value, profile: &str) -> BTreeMap<String, String> {
    manifest["profiles"][profile]["runners"]["tla"]["configuration"]
        .as_object()
        .expect("TLA profile configuration")
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                value
                    .as_str()
                    .expect("string configuration value")
                    .to_owned(),
            )
        })
        .collect()
}

fn required_budget_duration(configuration: &BTreeMap<String, String>, key: &str) -> Duration {
    configured_budget_duration(configuration, key)
        .unwrap_or_else(|error| panic!("valid {key}: {error}"))
        .unwrap_or_else(|| panic!("{key} configured"))
}

fn workflow_timeout_after(source: &str, marker: &str, timeout_prefix: &str) -> Duration {
    let marker_indent = marker.len() - marker.trim_start().len();
    let minutes = source
        .lines()
        .skip_while(|line| *line != marker)
        .skip(1)
        .take_while(|line| {
            line.trim().is_empty() || line.len() - line.trim_start().len() > marker_indent
        })
        .find_map(|line| line.strip_prefix(timeout_prefix))
        .unwrap_or_else(|| panic!("missing {timeout_prefix:?} after {marker:?}"))
        .parse::<u64>()
        .expect("workflow timeout uses whole minutes");
    Duration::from_secs(minutes * 60)
}

fn workflow_job_block<'a>(source: &'a str, marker: &str) -> &'a str {
    let start = source
        .find(marker)
        .unwrap_or_else(|| panic!("missing workflow job marker {marker:?}"));
    let remaining = &source[start + marker.len()..];
    let end = remaining.find("\n  invariants-").unwrap_or(remaining.len());
    &source[start..start + marker.len() + end]
}

#[cfg(not(target_os = "linux"))]
#[test]
fn unsupported_hosts_fail_closed_before_tlc_can_use_an_ambient_state_path() {
    let error = super::require_sound_tlc_state_binding()
        .expect_err("non-Linux TLC state binding must fail closed");
    assert!(error
        .to_string()
        .contains("requires Linux descriptor-relative state directories"));
}
