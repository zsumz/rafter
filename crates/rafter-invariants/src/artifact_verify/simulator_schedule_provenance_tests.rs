use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use super::{
    simulator_compiler_artifact_executable, simulator_program_matches,
    verify_simulator_invocation_outcome, verify_simulator_schedule, AggregateError,
};

#[path = "simulator_schedule_provenance_fixture_tests.rs"]
mod fixtures;

use fixtures::{
    materialize_cross_root_fixture, materialize_fixture, ProvenanceSubstitution, RuntimeDefect,
    SimulatorFixture,
};

static NEXT_SOURCE_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct CompilerSourceFixture {
    active_root: PathBuf,
    producer_root: PathBuf,
}

impl CompilerSourceFixture {
    fn new(label: &str) -> Self {
        let active_root = std::env::temp_dir().join(format!(
            "rafter-simulator-provenance-{label}-{}-{}",
            std::process::id(),
            NEXT_SOURCE_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&active_root);
        let source = active_root.join("crates/rafter-sim/src/bin/rafter-model-check-fast.rs");
        fs::create_dir_all(source.parent().expect("simulator source parent"))
            .expect("create active simulator source tree");
        fs::write(&source, "fn main() {}\n").expect("write active simulator source");
        let active_root = fs::canonicalize(active_root).expect("canonical active source root");
        let producer_root = active_root.with_extension("producer-root-a");
        let _ = fs::remove_dir_all(&producer_root);
        Self {
            active_root,
            producer_root,
        }
    }
}

impl Drop for CompilerSourceFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.active_root);
        let _ = fs::remove_dir_all(&self.producer_root);
    }
}

#[test]
fn simulator_runtime_path_and_digest_must_both_match_cargo_output() {
    let emitted = Path::new("/workspace/target/rafter-model-check-fast");
    let invocation = crate::InvocationReceipt {
        program: emitted.to_string_lossy().into_owned(),
        program_sha256: "a".repeat(64),
        arguments: vec!["--profile".to_owned(), "fast".to_owned()],
        current_dir: "/workspace".to_owned(),
        environment: BTreeMap::new(),
        environment_sha256: crate::provenance::invocation::digest_environment(&BTreeMap::new())
            .expect("valid fixture environment"),
        launchers: crate::receipt::fixture_launchers(false),
    };
    assert!(simulator_program_matches(
        &invocation,
        emitted,
        &"a".repeat(64),
    ));

    let mut substituted = invocation.clone();
    substituted.program = "/workspace/target/substituted-simulator".to_owned();
    assert!(!simulator_program_matches(
        &substituted,
        emitted,
        &"a".repeat(64),
    ));

    let mut wrong_digest = invocation;
    wrong_digest.program_sha256 = "b".repeat(64);
    assert!(!simulator_program_matches(
        &wrong_digest,
        emitted,
        &"a".repeat(64),
    ));
}

#[test]
fn simulator_compiler_artifact_rejects_provenance_substitutions() {
    let roots = CompilerSourceFixture::new("substitutions");
    let target_dir = roots.producer_root.join("target/simulator-build/exact");
    let exact = simulator_compiler_message(&roots.producer_root, &target_dir);
    simulator_compiler_artifact_executable(
        exact.to_string().as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .expect("exact simulator compiler-artifact verifies");

    let mut wrong_package = exact.clone();
    wrong_package["package_id"] = serde_json::json!(format!(
        "path+file://{}#0.0.1",
        roots.producer_root.join("crates/substituted").display()
    ));
    assert!(simulator_compiler_artifact_executable(
        wrong_package.to_string().as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("package_id"));

    let mut wrong_source = exact.clone();
    wrong_source["target"]["src_path"] = serde_json::json!(roots
        .producer_root
        .join("crates/rafter-sim/src/bin/substituted-model-check.rs"));
    assert!(simulator_compiler_artifact_executable(
        wrong_source.to_string().as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("source path"));

    let mut escaped_target = exact;
    escaped_target["executable"] = serde_json::json!(
        "/workspace/target/simulator-build/substituted/release/rafter-model-check-fast"
    );
    assert!(simulator_compiler_artifact_executable(
        escaped_target.to_string().as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("exact release target"));
}

#[test]
fn simulator_compiler_artifact_rejects_missing_and_ambiguous_outputs() {
    let roots = CompilerSourceFixture::new("missing-ambiguous");
    let target_dir = roots.producer_root.join("target/simulator-build/exact");
    let exact = simulator_compiler_message(&roots.producer_root, &target_dir);

    let mut missing_executable = exact.clone();
    missing_executable
        .as_object_mut()
        .expect("compiler message object")
        .remove("executable");
    assert!(simulator_compiler_artifact_executable(
        missing_executable.to_string().as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("omitted its executable"));

    let duplicated = format!("{exact}\n{exact}\n");
    assert!(simulator_compiler_artifact_executable(
        duplicated.as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("found 2"));

    assert!(simulator_compiler_artifact_executable(
        b"not a Cargo message\n",
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("found 0"));
}

#[test]
fn simulator_compiler_artifact_preserves_prefix_malformed_and_suffix_filtering() {
    let roots = CompilerSourceFixture::new("filtering");
    let target_dir = roots.producer_root.join("target/simulator-build/exact");
    let exact = simulator_compiler_message(&roots.producer_root, &target_dir);
    let mut prefix = exact.clone();
    prefix["target"]["name"] = serde_json::json!("prefix-rafter-model-check-fast");
    let mut suffix = exact.clone();
    suffix["target"]["name"] = serde_json::json!("rafter-model-check-fast-suffix");
    let stdout = format!("{prefix}\n{{malformed Cargo JSON\n{exact}\n{suffix}\n");

    let executable = simulator_compiler_artifact_executable(
        stdout.as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .expect("only the exact target name is selected");
    assert_eq!(
        executable,
        target_dir.join("release/rafter-model-check-fast")
    );
}

fn simulator_compiler_message(source_root: &Path, target_dir: &Path) -> serde_json::Value {
    serde_json::json!({
        "reason": "compiler-artifact",
        "package_id": format!(
            "path+file://{}#0.0.1",
            source_root.join("crates/rafter-sim").to_string_lossy()
        ),
        "target": {
            "name": "rafter-model-check-fast",
            "kind": ["bin"],
            "crate_types": ["bin"],
            "src_path": source_root.join(
                "crates/rafter-sim/src/bin/rafter-model-check-fast.rs"
            ),
        },
        "executable": target_dir.join("release/rafter-model-check-fast"),
        "fresh": false,
    })
}

#[test]
fn serialized_producer_root_a_provenance_verifies_at_aggregate_root_b() {
    let fixture = materialize_cross_root_fixture(RuntimeDefect::ProvenanceOnly);
    assert_ne!(fixture.producer_root, fixture.root);
    assert!(fixture.root.exists());
    assert!(
        !fixture.producer_root.exists(),
        "producer checkout A must not be available to the verifier"
    );
    let bundle = fixture.serialized_bundle();
    assert_eq!(
        Path::new(&bundle.execution.invocation.current_dir),
        fixture.producer_root
    );
    super::super::integrity::verify(&bundle, &fixture.root)
        .expect("serialized cross-root artifacts retain integrity");

    let diagnostics = verify_simulator_schedule(&bundle, &fixture.root)
        .expect("producer-root-A simulator provenance verifies from aggregate root B");
    assert!(diagnostics
        .iter()
        .any(|diagnostic| diagnostic.contains("did not run required profile raft-soak")));
}

#[test]
fn serialized_cross_root_provenance_rejects_adversarial_substitutions() {
    for (substitution, expected) in [
        (ProvenanceSubstitution::Package, "package_id"),
        (ProvenanceSubstitution::Source, "source path"),
        (ProvenanceSubstitution::TargetName, "found 0"),
        (ProvenanceSubstitution::TargetKind, "found 0"),
        (ProvenanceSubstitution::Executable, "exact release target"),
        (ProvenanceSubstitution::CompileRoot, "source contract"),
    ] {
        let fixture = materialize_cross_root_fixture(RuntimeDefect::ProvenanceOnly);
        fixture.substitute_provenance(substitution);
        let bundle = fixture.serialized_bundle();
        super::super::integrity::verify(&bundle, &fixture.root)
            .expect("substituted serialized artifact remains digest-bound");

        let error = match verify_simulator_schedule(&bundle, &fixture.root) {
            Ok(diagnostics) => {
                panic!("{substitution:?} substitution verified: {diagnostics:?}")
            }
            Err(error) => error,
        };
        assert!(
            error.to_string().contains(expected),
            "unexpected {substitution:?} error: {error}"
        );
    }
}

#[test]
fn timed_out_zero_exit_simulator_invocation_is_rejected() {
    let result: Result<(), AggregateError> =
        verify_simulator_invocation_outcome("fast", Some(0), true);
    let error = result.expect_err("timed-out invocation must fail verification");

    assert!(error.to_string().contains("did not time out"));
}

#[test]
fn real_timed_out_zero_exit_receipt_fails_closed_through_loading_and_aggregation() {
    let fixture = materialize_fixture(RuntimeDefect::Timeout);
    let loaded = crate::aggregate::load_evidence_at(
        std::slice::from_ref(&fixture.bundle_path),
        &fixture.root,
    );
    assert_eq!(
        loaded.bundles.len(),
        1,
        "unexpected load failures: {:?}",
        loaded.harness_errors
    );
    let counterexample = loaded.bundles[0]
        .results
        .iter()
        .find(|result| {
            result.status == crate::EvidenceStatus::Fail
                && result.classification == Some(crate::FailureClassification::InvariantViolation)
        })
        .expect("serialized counterexample result")
        .clone();
    assert_eq!(loaded.harness_errors.len(), 2);
    let error = loaded
        .harness_errors
        .iter()
        .find(|error| error.contains("did not time out"))
        .expect("timeout diagnostic");
    assert!(loaded
        .harness_errors
        .iter()
        .any(|error| error.contains("did not run required profile raft-soak")));
    let source_ref = loaded.bundles[0].source_ref.clone();
    let bundles = loaded.bundles;
    let report = crate::aggregate_with_harness_errors(
        &fixture.catalog,
        &fixture.manifest,
        "pr",
        &source_ref,
        &bundles,
        &loaded.harness_errors,
    )
    .expect("verified timeout error aggregates fail-closed");
    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
    assert!(report.invariants.iter().all(|verdict| {
        verdict.status == crate::VerdictStatus::Red
            && verdict.issues.iter().any(|issue| {
                issue.evidence_id == "aggregate/harness"
                    && issue.status == crate::EvidenceStatus::Error
                    && issue.classification == crate::FailureClassification::HarnessError
                    && issue.message == *error
            })
    }));
    let verdict = report
        .invariants
        .iter()
        .find(|verdict| verdict.invariant_id == counterexample.invariant_id)
        .expect("counterexample invariant verdict");
    let issue = verdict
        .issues
        .iter()
        .find(|issue| {
            issue.classification == crate::FailureClassification::InvariantViolation
                && issue.message == "real timeout fixture found a counterexample"
        })
        .unwrap_or_else(|| {
            panic!("counterexample {counterexample:?} missing from final verdict: {verdict:?}")
        });
    assert_eq!(issue.evidence_id, counterexample.evidence_id);
    assert_eq!(issue.status, crate::EvidenceStatus::Fail);
    assert_eq!(issue.artifacts, counterexample.artifacts);
}

#[test]
fn malformed_event_after_a_counterexample_is_retained_as_a_separate_harness_error() {
    let fixture = materialize_fixture(RuntimeDefect::MalformedEvent);
    let loaded = crate::aggregate::load_evidence_at(
        std::slice::from_ref(&fixture.bundle_path),
        &fixture.root,
    );
    assert_eq!(
        loaded.bundles.len(),
        1,
        "counterexample bundle was discarded: {:?}",
        loaded.harness_errors
    );
    let counterexample = loaded.bundles[0]
        .results
        .iter()
        .find(|result| {
            result.status == crate::EvidenceStatus::Fail
                && result.classification == Some(crate::FailureClassification::InvariantViolation)
        })
        .expect("serialized counterexample result")
        .clone();
    assert!(loaded.bundles[0].results.iter().any(|result| {
        result.status == crate::EvidenceStatus::Fail
            && result.classification == Some(crate::FailureClassification::InvariantViolation)
            && result.message.as_deref() == Some("real timeout fixture found a counterexample")
    }));
    assert!(loaded
        .harness_errors
        .iter()
        .any(|error| error.contains("parse simulator log")));
    let report = aggregate_fixture(&fixture, &loaded);
    assert_counterexample_survives(&report, &counterexample);
    assert!(report.invariants.iter().all(|verdict| {
        verdict.issues.iter().any(|issue| {
            issue.evidence_id == "aggregate/harness"
                && issue.message.contains("parse simulator log")
        })
    }));
}

#[test]
fn later_launch_failure_survives_loading_and_final_aggregation() {
    let fixture = materialize_fixture(RuntimeDefect::LaunchFailure);
    let loaded = crate::aggregate::load_evidence_at(
        std::slice::from_ref(&fixture.bundle_path),
        &fixture.root,
    );
    assert_eq!(
        loaded.bundles.len(),
        1,
        "launch-failure bundle was discarded: {:?}",
        loaded.harness_errors
    );
    let counterexample = loaded.bundles[0]
        .results
        .iter()
        .find(|result| result.status == crate::EvidenceStatus::Fail)
        .expect("first-run counterexample result")
        .clone();
    let launch_error = loaded.bundles[0]
        .results
        .iter()
        .find(|result| {
            result.status == crate::EvidenceStatus::Error
                && result
                    .message
                    .as_deref()
                    .is_some_and(|message| message.contains("injected raft-soak launch failure"))
        })
        .expect("later launch failure result")
        .clone();
    assert!(loaded
        .harness_errors
        .iter()
        .any(|error| error.contains("did not run required profile raft-soak")));
    let report = aggregate_fixture(&fixture, &loaded);
    assert_counterexample_survives(&report, &counterexample);
    let launch_verdict = report
        .invariants
        .iter()
        .find(|verdict| verdict.invariant_id == launch_error.invariant_id)
        .expect("launch-failure invariant verdict");
    let issue = launch_verdict
        .issues
        .iter()
        .find(|issue| issue.evidence_id == launch_error.evidence_id)
        .expect("launch failure survives final aggregation");
    assert_eq!(
        issue.classification,
        crate::FailureClassification::HarnessError
    );
    assert_eq!(issue.message, launch_error.message.expect("launch message"));
    assert_eq!(issue.artifacts, launch_error.artifacts);
}

#[test]
fn real_valid_looking_pass_then_exit_one_is_rejected_through_final_aggregation() {
    let fixture = materialize_fixture(RuntimeDefect::PassExitOne);
    let loaded = crate::aggregate::load_evidence_at(
        std::slice::from_ref(&fixture.bundle_path),
        &fixture.root,
    );
    assert_eq!(
        loaded.bundles.len(),
        1,
        "exit-one bundle was discarded: {:?}",
        loaded.harness_errors
    );
    let raw_log = fs::read_to_string(fixture.root.join("artifacts/invariants/fast.log"))
        .expect("read serialized exit-one simulator log");
    assert!(raw_log.lines().any(|line| line == "exit_code: Some(1)"));
    assert!(raw_log.contains("\"status\":\"pass\""));
    assert!(loaded
        .harness_errors
        .iter()
        .any(|error| { error.contains("simulator log fast requires a zero-exit invocation") }));
    assert!(loaded.bundles[0].results.iter().all(|result| {
        result.status == crate::EvidenceStatus::Error
            && result.classification == Some(crate::FailureClassification::HarnessError)
    }));

    let report = aggregate_fixture(&fixture, &loaded);
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, report.summary.total);
    assert!(report.invariants.iter().all(|verdict| {
        verdict.issues.iter().any(|issue| {
            issue.evidence_id == "aggregate/harness"
                && issue
                    .message
                    .contains("simulator log fast requires a zero-exit invocation")
        })
    }));
}

#[test]
fn real_counterexample_then_exit_one_preserves_semantics_through_final_aggregation() {
    let fixture = materialize_fixture(RuntimeDefect::CounterexampleExitOne);
    let loaded = crate::aggregate::load_evidence_at(
        std::slice::from_ref(&fixture.bundle_path),
        &fixture.root,
    );
    assert_eq!(
        loaded.bundles.len(),
        1,
        "counterexample exit-one bundle was discarded: {:?}",
        loaded.harness_errors
    );
    let raw_log = fs::read_to_string(fixture.root.join("artifacts/invariants/fast.log"))
        .expect("read serialized counterexample exit-one simulator log");
    assert!(raw_log.lines().any(|line| line == "exit_code: Some(1)"));
    assert!(raw_log.contains("real exit-one fixture found a counterexample"));
    assert!(loaded
        .harness_errors
        .iter()
        .any(|error| { error.contains("simulator log fast requires a zero-exit invocation") }));
    let counterexample = loaded.bundles[0]
        .results
        .iter()
        .find(|result| {
            result.status == crate::EvidenceStatus::Fail
                && result.classification == Some(crate::FailureClassification::InvariantViolation)
                && result.message.as_deref() == Some("real exit-one fixture found a counterexample")
        })
        .expect("serialized semantic counterexample")
        .clone();

    let report = aggregate_fixture(&fixture, &loaded);
    assert_counterexample_survives(&report, &counterexample);
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, report.summary.total);
}

fn aggregate_fixture(
    fixture: &SimulatorFixture,
    loaded: &crate::aggregate::LoadedEvidence,
) -> crate::VerdictReport {
    crate::aggregate_with_harness_errors(
        &fixture.catalog,
        &fixture.manifest,
        "pr",
        &loaded.bundles[0].source_ref,
        &loaded.bundles,
        &loaded.harness_errors,
    )
    .expect("aggregate verified simulator fixture")
}

fn assert_counterexample_survives(
    report: &crate::VerdictReport,
    counterexample: &crate::EvidenceResult,
) {
    let verdict = report
        .invariants
        .iter()
        .find(|verdict| verdict.invariant_id == counterexample.invariant_id)
        .expect("counterexample invariant verdict");
    let issue = verdict
        .issues
        .iter()
        .find(|issue| issue.evidence_id == counterexample.evidence_id)
        .expect("semantic counterexample survives final aggregation");
    assert_eq!(
        issue.classification,
        crate::FailureClassification::InvariantViolation
    );
    assert_eq!(issue.message, counterexample.message.as_deref().unwrap());
    assert_eq!(issue.artifacts, counterexample.artifacts);
}
