//! TLA+ source, tool, symmetry, and trace-contract scenarios.

use std::{collections::BTreeMap, ffi::OsString, path::Path, time::Duration};

use super::super::tla_output::{
    render_detector_config, DetectorProbe, DEFAULT_FIXTURE_MODE, DETECTOR_PROBES,
    REGISTERED_PREDICATES,
};
use super::{
    configured_invariants, fetch_tool_with, java_major, tool_fetch_environment,
    validate_obligation_config_sources, validate_obligation_options, validate_runner_options,
    validate_safety_only_boundary, validate_symmetry_contract, validate_trace_contract_sources,
    SPEC, TRACE_CONFIG, TRACE_SPEC,
};
use crate::contract::profile::{ObligationCompletion, ProofObligationContract};

#[test]
fn java_major_is_parsed_exactly() {
    assert_eq!(java_major("java 21.0.5 2024-10-15 LTS"), Some(21));
    assert_eq!(java_major("openjdk 21.0.7 2025-04-15"), Some(21));
    assert_eq!(java_major("java version \"1.8.0_402\""), Some(8));
    assert_eq!(java_major("java 210.0.1"), Some(210));
}

#[test]
#[cfg(unix)]
fn tool_fetch_is_managed_and_times_out_with_retained_diagnostics() {
    let error = fetch_tool_with(
        Path::new("."),
        "sh",
        &[
            OsString::from("-c"),
            OsString::from("printf fetch-started; sleep 5"),
        ],
        Duration::from_millis(50),
    )
    .expect_err("stalled tool fetch must time out")
    .to_string();
    assert!(error.contains("timed_out=true"));
    assert!(error.contains("fetch-started"));
}

#[test]
fn descriptor_bound_tool_fetch_receives_the_held_repository_root() {
    let environment = tool_fetch_environment(Path::new("/tmp/rafter-root"));

    assert_eq!(environment["RAFTER_TLA_REPO_ROOT"], "/tmp/rafter-root");
}

#[test]
fn production_tla_contract_is_safety_only() {
    let safety_spec = "Spec == Init /\\ [][Next]_vars\n";
    let safety_config = "INVARIANT TypeOK\n";
    assert!(validate_safety_only_boundary(safety_spec, safety_config).is_ok());

    let fair_spec = "Spec == Init /\\ [][Next]_vars /\\ WF_vars(Next)\n";
    assert!(validate_safety_only_boundary(fair_spec, safety_config).is_err());
    assert!(validate_safety_only_boundary(safety_spec, "PROPERTY EventualLeader\n").is_err());
}

#[test]
fn bounded_and_weekly_symmetry_contracts_are_exact() {
    assert!(validate_symmetry_contract("RaftCi.cfg", "SYMMETRY ModelPermutations\n").is_ok());
    assert!(validate_symmetry_contract("RaftNightly.cfg", "SYMMETRY NodePermutations\n").is_err());
    assert!(validate_symmetry_contract("RaftCi.cfg", "CHECK_DEADLOCK FALSE\n").is_err());
    assert!(validate_symmetry_contract("Raft.cfg", "CHECK_DEADLOCK FALSE\n").is_ok());
    assert!(validate_symmetry_contract("Raft.cfg", "SYMMETRY ModelPermutations\n").is_err());
}

#[test]
fn fixed_runner_options_cannot_drift_from_execution() {
    let mut options = BTreeMap::from([
        ("module".to_owned(), "Raft.tla".to_owned()),
        ("fp".to_owned(), "0".to_owned()),
        ("tool_mode".to_owned(), "required".to_owned()),
        ("trace_sample".to_owned(), "required".to_owned()),
        ("detector_negative".to_owned(), "required".to_owned()),
    ]);
    assert!(validate_runner_options(&options).is_ok());
    options.insert("fp".to_owned(), "1".to_owned());
    assert!(validate_runner_options(&options).is_err());
}

#[test]
fn bounded_runner_symmetry_label_cannot_drift_from_execution() {
    let mut options = BTreeMap::from([
        ("module".to_owned(), "Raft.tla".to_owned()),
        ("fp".to_owned(), "0".to_owned()),
        ("tool_mode".to_owned(), "required".to_owned()),
        ("trace_sample".to_owned(), "required".to_owned()),
        ("detector_negative".to_owned(), "required".to_owned()),
        ("config".to_owned(), "RaftCi.cfg".to_owned()),
        ("workers".to_owned(), "4".to_owned()),
        ("soft_timeout".to_owned(), "325m".to_owned()),
        ("max_heap".to_owned(), "8g".to_owned()),
        ("fp_mem".to_owned(), "0.45".to_owned()),
        (
            "symmetry".to_owned(),
            "nodes-values-read-requests-product".to_owned(),
        ),
    ]);
    assert!(validate_runner_options(&options).is_ok());
    options.insert("symmetry".to_owned(), "nodes-only".to_owned());
    assert!(validate_runner_options(&options).is_err());
}

#[test]
fn weekly_checkpoint_contract_is_exact() {
    let mut options = BTreeMap::from([
        ("module".to_owned(), "Raft.tla".to_owned()),
        ("fp".to_owned(), "0".to_owned()),
        ("tool_mode".to_owned(), "required".to_owned()),
        ("trace_sample".to_owned(), "required".to_owned()),
        ("detector_negative".to_owned(), "required".to_owned()),
        ("config".to_owned(), "Raft.cfg".to_owned()),
        ("workers".to_owned(), "auto".to_owned()),
        ("soft_timeout".to_owned(), "155m".to_owned()),
        ("checkpoint_minutes".to_owned(), "30".to_owned()),
        ("checkpoint_gzip".to_owned(), "required".to_owned()),
        ("max_heap".to_owned(), "8g".to_owned()),
        ("fp_mem".to_owned(), "0.45".to_owned()),
        (
            "checkpoint_recovery".to_owned(),
            "strict-compatible-if-present".to_owned(),
        ),
        (
            "unsymmetrized_exploration".to_owned(),
            "required".to_owned(),
        ),
    ]);
    assert!(validate_runner_options(&options).is_ok());
    // The retired 4g heap: weekly moved to nightly's 8g when the first
    // dispatch showed 4g cannot drain the unsymmetrized snapshot obligation
    // the tier gates on, so the old value must now be refused.
    options.insert("max_heap".to_owned(), "4g".to_owned());
    assert!(validate_runner_options(&options).is_err());
}

#[test]
fn nightly_checkpoint_contract_is_exact() {
    let mut options = BTreeMap::from([
        ("module".to_owned(), "Raft.tla".to_owned()),
        ("fp".to_owned(), "0".to_owned()),
        ("tool_mode".to_owned(), "required".to_owned()),
        ("trace_sample".to_owned(), "required".to_owned()),
        ("detector_negative".to_owned(), "required".to_owned()),
        ("config".to_owned(), "RaftNightly.cfg".to_owned()),
        (
            "symmetry".to_owned(),
            "nodes-values-read-requests-product".to_owned(),
        ),
        ("workers".to_owned(), "auto".to_owned()),
        ("soft_timeout".to_owned(), "240m".to_owned()),
        ("checkpoint_minutes".to_owned(), "30".to_owned()),
        ("checkpoint_gzip".to_owned(), "required".to_owned()),
        ("max_heap".to_owned(), "8g".to_owned()),
        ("fp_mem".to_owned(), "0.45".to_owned()),
        (
            "checkpoint_recovery".to_owned(),
            "strict-compatible-if-present".to_owned(),
        ),
    ]);
    assert!(validate_runner_options(&options).is_ok());
    options.insert("workers".to_owned(), "4".to_owned());
    assert!(validate_runner_options(&options).is_err());
}

#[test]
fn every_invariant_block_is_part_of_the_exact_contract() {
    let config = "INVARIANTS\n  TypeOK\n\nCHECK_DEADLOCK FALSE\n\nINVARIANT ElectionSafety\n";
    assert_eq!(
        configured_invariants(config),
        vec!["TypeOK".to_owned(), "ElectionSafety".to_owned()]
    );
}

#[test]
fn detector_configs_bind_one_unique_counterexample_identity() {
    let template = "INIT FixtureInit\nCONSTANT TargetPredicate = \"ElectionSafety\"\nCONSTANT FixtureMode = \"Default\"\nINVARIANT TypeOK\nINVARIANT ElectionSafety\n";
    let rendered = DETECTOR_PROBES
        .iter()
        .map(|probe| {
            let config = render_detector_config(template, *probe).expect("valid template");
            assert!(config.contains(&format!(
                "CONSTANT TargetPredicate = \"{}\"",
                probe.predicate
            )));
            assert!(config.contains(&format!("CONSTANT FixtureMode = \"{}\"", probe.mode)));
            assert!(config.contains(&format!("INVARIANT {}", probe.predicate)));
            config
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(rendered.len(), DETECTOR_PROBES.len());
    let invalid = DetectorProbe {
        predicate: "ExpectedViolation",
        mode: DEFAULT_FIXTURE_MODE,
    };
    assert!(render_detector_config(template, invalid).is_err());
    assert!(render_detector_config("INIT Init\n", DETECTOR_PROBES[0]).is_err());
}

#[test]
fn membership_trace_contract_rejects_any_reviewed_source_drift() {
    let symbols = REGISTERED_PREDICATES
        .iter()
        .map(|predicate| (*predicate).to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let spec = std::fs::read_to_string(root.join(TRACE_SPEC)).expect("read membership trace spec");
    let config =
        std::fs::read_to_string(root.join(TRACE_CONFIG)).expect("read membership trace config");
    validate_trace_contract_sources(&symbols, &spec, &config)
        .expect("checked-in trace contract is exact");

    for mutated in [
        spec.replace("/\\ WF_traceVars(TraceNext)", "/\\ TRUE"),
        spec.replace("/\\ InstallSnapshot", "/\\ TRUE"),
        spec.replace("\\/ TraceAction28", "\\/ TraceAction27"),
        spec.replace("/\\ Timeout(n1)", "/\\ TRUE"),
        spec.replace("/\\ ClientAppend(n1, v1)", "/\\ TRUE"),
        spec.replace(
            "TraceComplete == traceStep = 44",
            "TraceComplete == traceStep \\in 43..44",
        ),
    ] {
        assert!(validate_trace_contract_sources(&symbols, &mutated, &config).is_err());
    }
    assert!(validate_trace_contract_sources(
        &symbols,
        &spec,
        &config.replace("  LeaderCompleteness\n", "")
    )
    .is_err());
    assert!(validate_trace_contract_sources(
        &symbols,
        &spec,
        &config.replace("  MaxLogLen = 6", "  MaxLogLen = 5")
    )
    .is_err());
}

fn obligation(id: &str, config: &str) -> ProofObligationContract {
    ProofObligationContract {
        id: id.to_owned(),
        config: config.to_owned(),
        completion: ObligationCompletion::FrontierExhausted,
        minimum_generated_states: 1_000,
        minimum_distinct_states: 100,
        soft_timeout: "5m".to_owned(),
        seed: "2026081101".to_owned(),
    }
}

/// The producer keeps its own gate on the obligation list rather than trusting
/// the profile contract's. Both must independently refuse an obligation that
/// re-runs a profile's primary model or names an unusable configuration.
#[test]
fn producer_refuses_primary_and_duplicate_obligations() {
    validate_obligation_options(&[]).expect("an empty obligation list is legal");
    validate_obligation_options(&[
        obligation(
            "joint-quorum-focused-init",
            "RaftJointQuorumFocusedInit.cfg",
        ),
        obligation(
            "joint-quorum-focused-next",
            "RaftJointQuorumFocusedNext.cfg",
        ),
    ])
    .expect("distinct focused obligations are legal");

    for config in ["RaftCi.cfg", "RaftNightly.cfg", "Raft.cfg", "sub/dir.cfg"] {
        assert!(validate_obligation_options(&[obligation("focused", config)]).is_err());
    }

    let duplicate = [
        obligation("focused", "RaftJointQuorumFocusedInit.cfg"),
        obligation("focused", "RaftJointQuorumFocusedNext.cfg"),
    ];
    assert!(validate_obligation_options(&duplicate).is_err());

    let mut vacuous = obligation("focused", "RaftJointQuorumFocusedInit.cfg");
    vacuous.minimum_generated_states = 0;
    assert!(validate_obligation_options(&[vacuous]).is_err());
}

/// Discharging an obligation must mean something. A configuration that binds
/// no invariants would exit cleanly and clear any state floor, so every
/// obligation is held to the same registry as the primary configuration.
#[test]
fn obligation_specs_must_bind_the_registry_predicates() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let spec = std::fs::read_to_string(root.join(SPEC)).expect("read production spec");
    let symbols = REGISTERED_PREDICATES
        .iter()
        .map(|predicate| (*predicate).to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    let read = |name: &str| {
        std::fs::read_to_string(root.join("specs/tla/raft").join(name)).expect("read config")
    };

    for name in [
        "RaftJointQuorumFocusedInit.cfg",
        "RaftJointQuorumFocusedNext.cfg",
    ] {
        validate_obligation_config_sources("focused", name, &spec, &read(name), &symbols)
            .unwrap_or_else(|error| panic!("{name} must bind the whole registry: {error}"));
    }

    // The trace-sample config is a real file that configures the registry but
    // also declares a liveness PROPERTY, so it must be refused by the
    // safety-only boundary rather than silently accepted.
    assert!(validate_obligation_config_sources(
        "trace",
        TRACE_CONFIG,
        &spec,
        &read("RaftMembershipTraceSample.cfg"),
        &symbols,
    )
    .is_err());

    // A configuration that binds nothing exits cleanly and would otherwise
    // "discharge" while proving nothing at all.
    assert!(validate_obligation_config_sources(
        "empty",
        "RaftEmpty.cfg",
        &spec,
        "SPECIFICATION MembershipSpec\n",
        &symbols,
    )
    .is_err());
}

/// The producer keeps its own allow-list, deliberately duplicating what the
/// profile contract already pins so neither gate can be weakened alone. That
/// independence only holds if both are kept in step, and every earlier test
/// here fed the allow-list a hand-built map -- so a real profile whose budget
/// moved in the contract sailed past locally and refused at runtime in CI.
/// This runs the reviewed manifest itself through the producer's gate.
#[test]
fn every_reviewed_profile_satisfies_the_producer_allow_list() {
    let (_, manifest) = crate::tests::loaded();
    for profile in ["pr", "nightly", "weekly"] {
        let runner = &manifest.profiles[profile].runners["tla"];
        validate_runner_options(&runner.configuration).unwrap_or_else(|error| {
            panic!("{profile} TLA configuration must satisfy the producer allow-list: {error}")
        });
        validate_obligation_options(&runner.obligations).unwrap_or_else(|error| {
            panic!("{profile} TLA obligations must satisfy the producer allow-list: {error}")
        });
    }
}

/// Obligations and the primary continuation share one execution window, and
/// the producer hands each phase `min(budget, remaining)`. A reviewed manifest
/// whose budgets oversubscribe the window would silently truncate whichever
/// phase ran last, so the sum is checked against the window here as well as in
/// the contract layer.
#[test]
fn reviewed_obligation_budgets_fit_inside_every_execution_window() {
    let (_, manifest) = crate::tests::loaded();
    for profile in ["pr", "nightly", "weekly"] {
        let runner = &manifest.profiles[profile].runners["tla"];
        let minutes = |value: &str| {
            value
                .strip_suffix('m')
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("{profile} budget {value} is whole minutes"))
        };
        let obligations: u64 = runner
            .obligations
            .iter()
            .map(|obligation| minutes(&obligation.soft_timeout))
            .sum();
        let primary = minutes(&runner.configuration["soft_timeout"]);
        let window = minutes(&runner.configuration["total_timeout"])
            - minutes(&runner.configuration["finalization_reserve"]);
        assert!(
            obligations + primary <= window,
            "{profile}: {obligations}m of obligations plus {primary}m of primary exceeds the {window}m window"
        );
    }
}
