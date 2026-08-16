//! Producer budget, environment, identity-command, and host-policy scenarios.

use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::Path,
    time::{Duration, Instant},
};

use super::super::{
    identity_command_with_timeout, layer_budget, timed_for, ProcessKind, ProcessPolicy,
    ProcessSchedule,
};
use crate::provenance::invocation::digest_environment;

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
        obligations: Vec::new(),
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
fn standalone_schedule_preserves_the_exact_receipt_finalization_phase() {
    let policy = ProcessPolicy {
        termination_grace: Duration::from_secs(7),
        kill_confirmation_timeout: Duration::from_secs(3),
        receipt_finalization_allowance: Duration::from_secs(4),
    };
    let schedule = ProcessSchedule::standalone(Duration::from_secs(11), policy)
        .expect("standalone process schedule");
    assert_eq!(
        schedule
            .lifecycle_deadline
            .duration_since(schedule.finalization_start_deadline),
        policy.receipt_finalization_allowance
    );
    assert_eq!(
        schedule
            .cleanup_start_deadline
            .duration_since(schedule.execution_window_deadline),
        policy.termination_grace + policy.kill_confirmation_timeout
    );
    assert_eq!(
        schedule
            .finalization_start_deadline
            .duration_since(schedule.cleanup_start_deadline),
        policy.kill_confirmation_timeout
    );
}

#[test]
fn implicit_producer_process_without_a_layer_budget_fails_closed() {
    let error = timed_for(
        ProcessKind::SimulatorExecution,
        "printf",
        &[OsString::from("unreachable")],
        &super::super::base_environment(),
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
fn production_process_policy_rejects_hosts_without_descriptor_exec() {
    assert!(super::super::process_execution_policy(true, false).is_ok());
    assert!(super::super::process_execution_policy(false, true).is_ok());
    let error = super::super::process_execution_policy(false, false)
        .expect_err("production execution without descriptor exec must fail closed");
    assert!(error
        .to_string()
        .contains("requires Linux descriptor-based executable launch"));
}
