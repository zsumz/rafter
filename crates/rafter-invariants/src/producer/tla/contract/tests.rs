//! TLA+ source, tool, symmetry, and trace-contract scenarios.

use std::{collections::BTreeMap, ffi::OsString, path::Path, time::Duration};

use super::super::tla_output::{
    render_detector_config, DetectorProbe, DEFAULT_FIXTURE_MODE, DETECTOR_PROBES,
    REGISTERED_PREDICATES,
};
use super::{
    configured_invariants, fetch_tool_with, java_major, tool_fetch_environment,
    validate_runner_options, validate_safety_only_boundary, validate_symmetry_contract,
    validate_trace_contract_sources, TRACE_CONFIG, TRACE_SPEC,
};

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
        ("soft_timeout".to_owned(), "300m".to_owned()),
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
        ("soft_timeout".to_owned(), "295m".to_owned()),
        ("checkpoint_minutes".to_owned(), "30".to_owned()),
        ("checkpoint_gzip".to_owned(), "required".to_owned()),
        ("max_heap".to_owned(), "4g".to_owned()),
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
    options.insert("max_heap".to_owned(), "8g".to_owned());
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
        ("soft_timeout".to_owned(), "295m".to_owned()),
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
            "TraceComplete == traceStep = 45",
            "TraceComplete == traceStep \\in 44..45",
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
