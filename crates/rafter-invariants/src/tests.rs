//! End-to-end registry, profile, producer, and aggregate invariant scenarios.

use std::path::Path;

use super::*;

mod support;

use support::{
    aggregate, artifact, assert_only_parent_red, invocation_receipt, producer_binding,
    source_receipt, verify_layer_bundle,
};
pub(crate) use support::{
    aggregate_unverified, loaded, passing_bundles, passing_bundles_for_profile, plan_receipt,
    synthetic_obligation_summary,
};

#[test]
fn empty_pr_evidence_is_exactly_44_rows_and_red() {
    let (catalog, manifest) = loaded();
    let report = aggregate(&catalog, &manifest, "pr", "abc", &[]).expect("report aggregates");
    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
    assert!(report
        .invariants
        .iter()
        .all(|verdict| verdict.status == VerdictStatus::Red));
}

#[test]
fn complete_matching_evidence_is_44_of_44_green() {
    let (catalog, manifest) = loaded();
    let bundles = passing_bundles(&catalog, &manifest);
    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_eq!(
        report.summary.green,
        44,
        "unexpected red verdicts: {:#?}",
        report
            .invariants
            .iter()
            .filter(|verdict| verdict.status == VerdictStatus::Red)
            .collect::<Vec<_>>()
    );
    assert_eq!(report.summary.red, 0);
    assert_eq!(report.artifacts.len(), 6);
}

#[test]
fn tests_runner_failure_contract_is_pinned_for_every_profile() {
    let (catalog, mut manifest) = loaded();
    manifest
        .validate(&catalog)
        .expect("control manifest validates");
    for profile in ["pr", "nightly", "weekly"] {
        assert_eq!(
            manifest.profiles[profile].runners["tests"]
                .configuration
                .get("failure_contract")
                .map(String::as_str),
            Some("typed-oracle-libtest-v4")
        );
    }
    manifest
        .profiles
        .get_mut("pr")
        .expect("PR profile")
        .runners
        .get_mut("tests")
        .expect("tests runner")
        .configuration
        .insert("failure_contract".to_owned(), "best-effort".to_owned());
    assert!(manifest.validate(&catalog).is_err());
}

#[test]
fn same_commit_evidence_from_a_different_plan_is_red() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    bundles[0].execution.plan.contract.description = "different plan".to_owned();

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert!(report.summary.red > 0);
    assert!(report.invariants.iter().any(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("hashed execution plan"))));
}

#[test]
fn missing_actual_invocation_is_red() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    bundles[0].execution.invocation.arguments.clear();

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert!(report.summary.red > 0);
    assert!(report.invariants.iter().any(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("actual producer invocation"))));
}

#[test]
fn producer_invocation_cannot_claim_a_subprocess_launcher_chain() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    bundles[0].execution.invocation.launchers = crate::receipt::fixture_launchers(false);

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert!(report.summary.red > 0);
    assert!(report.invariants.iter().any(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("actual producer invocation"))));
}
#[test]
fn missing_direct_clause_keeps_only_parent_red() {
    let (mut catalog, manifest) = loaded();
    let evidence = catalog
        .evidence
        .iter_mut()
        .find(|evidence| evidence.clause_id == "RD-06.b" && evidence.strength == "direct")
        .expect("RD-06.b direct evidence exists");
    evidence.invariant_id = "RD-05".to_owned();
    evidence.clause_id = "RD-05.a".to_owned();
    let bundles = passing_bundles(&catalog, &manifest);

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_only_parent_red(&report, "RD-06");
    assert!(report.invariants.iter().any(|verdict| {
        verdict.invariant_id == "RD-06"
            && verdict
                .issues
                .iter()
                .any(|issue| issue.evidence_id == "RD-06.b/coverage")
    }));
}

#[test]
fn e2e_does_not_satisfy_direct_clause() {
    let (mut catalog, manifest) = loaded();
    let evidence = catalog
        .evidence
        .iter_mut()
        .find(|evidence| evidence.clause_id == "RD-06.b" && evidence.strength == "direct")
        .expect("RD-06.b direct evidence exists");
    evidence.strength = "e2e".to_owned();
    let bundles = passing_bundles(&catalog, &manifest);

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_only_parent_red(&report, "RD-06");
    assert!(report.invariants.iter().any(|verdict| {
        verdict.invariant_id == "RD-06"
            && verdict
                .issues
                .iter()
                .any(|issue| issue.message.contains("has no direct evidence"))
    }));
}

#[test]
fn sibling_clause_evidence_cannot_substitute() {
    let (mut catalog, manifest) = loaded();
    let evidence = catalog
        .evidence
        .iter_mut()
        .find(|evidence| evidence.clause_id == "RD-06.b" && evidence.strength == "direct")
        .expect("RD-06.b direct evidence exists");
    evidence.clause_id = "RD-06.a".to_owned();
    let bundles = passing_bundles(&catalog, &manifest);

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_only_parent_red(&report, "RD-06");
}

#[test]
fn incomplete_clause_result_fails_parent() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    let evidence_id = catalog
        .evidence
        .iter()
        .find(|evidence| evidence.clause_id == "RD-06.b" && evidence.strength == "direct")
        .expect("RD-06.b direct evidence exists")
        .evidence_id();
    let tests = bundles
        .iter_mut()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle exists");
    let execution_id = tests
        .results
        .iter()
        .find(|result| result.evidence_id == evidence_id)
        .expect("RD-06.b result exists")
        .execution_id
        .clone();
    let check = tests
        .execution
        .checks
        .iter_mut()
        .find(|check| check.execution_id == execution_id)
        .expect("RD-06.b check exists");
    check.completion = CheckCompletion::CoverageNotReached;
    check.observations = std::collections::BTreeMap::from([
        ("discovered".to_owned(), 1),
        ("executed".to_owned(), 0),
        ("passed".to_owned(), 0),
    ]);
    let artifacts = check.artifacts.clone();
    for result in tests
        .results
        .iter_mut()
        .filter(|result| result.execution_id == execution_id)
    {
        result.status = EvidenceStatus::Incomplete;
        result.classification = Some(FailureClassification::CoverageNotReached);
        result.message = Some("unknown-write branch was not reached".to_owned());
        result.artifacts.clone_from(&artifacts);
    }

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_only_parent_red(&report, "RD-06");
}

#[test]
fn detector_fixtures_have_distinct_evidence_ids() {
    let (catalog, _) = loaded();
    let simulator = catalog
        .evidence
        .iter()
        .filter(|evidence| evidence.layer == "simulator" && evidence.strength == "direct")
        .collect::<Vec<_>>();
    let ids = simulator
        .iter()
        .map(|evidence| evidence.evidence_id())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), simulator.len());
    let mut physical_checks = std::collections::BTreeMap::new();
    for evidence in &simulator {
        physical_checks
            .entry((
                evidence.path.as_str(),
                evidence.symbol.as_str(),
                evidence.negative_fixture.as_deref(),
                evidence
                    .simulator
                    .as_ref()
                    .map(|identity| identity.required_observation.as_str()),
            ))
            .or_insert_with(Vec::new)
            .push(evidence);
    }
    for descriptors in physical_checks.values().filter(|group| group.len() > 1) {
        let atomic_groups = descriptors
            .iter()
            .map(|evidence| evidence.atomic_group.as_deref())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(atomic_groups.len(), 1);
        assert!(atomic_groups.iter().all(Option::is_some));
    }
}

#[test]
fn budget_exhaustion_cannot_be_reported_as_pass() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    bundles[0].execution.checks[0].completion = CheckCompletion::BudgetExhausted;

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_eq!(report.summary.green, 0);
    assert!(report.invariants.iter().all(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("status disagrees"))));
}

#[test]
fn dirty_source_receipt_cannot_be_green() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    bundles[0].execution.source.clean = false;

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_eq!(report.summary.green, 0);
    assert!(report.invariants.iter().all(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("provenance is incomplete"))));
}

#[test]
fn result_must_reference_its_actual_check() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    bundles[0].results[0].execution_id = "forged".to_owned();

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_eq!(report.summary.green, 0);
    assert!(report.invariants.iter().all(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("does not reference"))));
}

#[test]
fn tests_pass_requires_registry_check_identity_and_exact_observations() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    let tests = bundles
        .iter_mut()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle exists");
    tests.execution.checks[0].observations.clear();

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_eq!(report.summary.green, 0);
    assert!(report
        .invariants
        .iter()
        .all(|verdict| verdict.issues.iter().any(|issue| issue
            .message
            .contains("observations, and artifacts disagree"))));
}

#[test]
fn tests_check_fanout_must_match_registry() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    let tests = bundles
        .iter_mut()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle exists");
    tests.execution.checks[0].check_id = "tests/forged".to_owned();

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_eq!(report.summary.green, 0);
    assert!(report.invariants.iter().all(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("exactly match the registry"))));
}

#[test]
fn simulator_pass_requires_its_registry_semantic_witness() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    let simulator = bundles
        .iter_mut()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle exists");
    let check = &mut simulator.execution.checks[0];
    let evidence_id = check.evidence_ids[0].clone();
    let descriptor = catalog
        .evidence
        .iter()
        .find(|descriptor| descriptor.evidence_id() == evidence_id)
        .expect("simulator evidence exists");
    let required_observation = descriptor
        .simulator
        .as_ref()
        .expect("simulator identity exists")
        .required_observation
        .clone();
    check.observations.remove(&required_observation);

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert!(report.summary.green < 44);
    let verdict = report
        .invariants
        .iter()
        .find(|verdict| verdict.invariant_id == descriptor.invariant_id)
        .expect("affected invariant verdict exists");
    assert_eq!(verdict.status, VerdictStatus::Red);
    assert!(verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("lacks semantic coverage")));
}

#[test]
fn simulator_pass_requires_profile_owned_per_check_floors_and_observations() {
    let (catalog, manifest) = loaded();
    for (key, expected) in [
        (
            crate::contract::profile::per_check_protocol_states_key("raft-commit-production"),
            "profile-owned per-check state floor",
        ),
        (
            crate::contract::profile::per_check_observation_key(
                "raft-commit-production",
                "production_config_commit_observed",
            ),
            "profile-owned per-check semantic observation",
        ),
    ] {
        let mut bundles = passing_bundles(&catalog, &manifest);
        let simulator = bundles
            .iter_mut()
            .find(|bundle| bundle.runner == "simulator")
            .expect("simulator bundle exists");
        let check = simulator
            .execution
            .checks
            .iter_mut()
            .find(|check| check.observations.contains_key(&key))
            .expect("production check receipt exists");
        check.observations.remove(&key);

        let report =
            aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
        assert_eq!(report.summary.green, 0);
        assert!(report.invariants.iter().all(|verdict| verdict
            .issues
            .iter()
            .any(|issue| issue.message.contains(expected))));
    }
}

#[test]
fn profile_owned_check_floors_do_not_replace_descriptor_floors() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    let simulator = bundles
        .iter_mut()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle exists");
    let per_check_key =
        crate::contract::profile::per_check_protocol_states_key("raft-commit-production");
    let check = simulator
        .execution
        .checks
        .iter_mut()
        .find(|check| check.observations.contains_key(&per_check_key))
        .expect("production check receipt exists");
    check
        .observations
        .insert("unique_protocol_states".to_owned(), 0);

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_eq!(report.summary.green, 0);
    assert!(report
        .invariants
        .iter()
        .all(|verdict| verdict.issues.iter().any(|issue| issue
            .message
            .contains("descriptor state or completion floor"))));
}

#[test]
fn simulator_liveness_pass_rejects_shallow_counter_only_receipt() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    let simulator = bundles
        .iter_mut()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle exists");
    let check = simulator
        .execution
        .checks
        .iter_mut()
        .find(|check| check.simulator_liveness.is_some())
        .expect("liveness check exists");
    check.simulator_liveness = None;

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert!(report.summary.green < 44);
    assert!(report.invariants.iter().any(|verdict| verdict
        .issues
        .iter()
        .any(|issue| { issue.message.contains("exact typed report binding") })));
}

#[test]
fn tla_pass_requires_every_framed_predicate_observation() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    let tla = bundles
        .iter_mut()
        .find(|bundle| bundle.runner == "tla")
        .expect("TLA bundle exists");
    tla.execution.checks[0]
        .observations
        .remove("detector_qualified:ElectionSafety");

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
    assert!(report.invariants.iter().any(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("terminal frames"))));
}

#[test]
fn tla_pass_requires_every_model_transition_observation() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    let tla = bundles
        .iter_mut()
        .find(|bundle| bundle.runner == "tla")
        .expect("TLA bundle exists");
    tla.execution.checks[0]
        .observations
        .remove("transition_covered:InstallSnapshot");

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
    assert!(report.invariants.iter().any(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("terminal frames"))));
}

#[test]
fn tla_generic_completed_status_cannot_claim_pass() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    let tla = bundles
        .iter_mut()
        .find(|bundle| bundle.runner == "tla")
        .expect("TLA bundle exists");
    tla.execution.checks[0].completion = CheckCompletion::Completed;

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
    assert!(report.invariants.iter().any(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("generic completed"))));
}

#[test]
fn canonical_invariant_with_one_layer_stays_red() {
    let (mut catalog, manifest) = loaded();
    catalog
        .evidence
        .retain(|evidence| evidence.invariant_id != "LG-01" || evidence.layer != "tests");
    let bundles = passing_bundles(&catalog, &manifest);

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    let verdict = report
        .invariants
        .iter()
        .find(|verdict| verdict.invariant_id == "LG-01")
        .expect("LG-01 verdict exists");
    assert_eq!(verdict.status, VerdictStatus::Red);
    assert!(verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("requires 2 independent layers")));
}

#[test]
fn result_bundle_rejects_unknown_fields() {
    let source = r#"{
        "schema_version": 9,
        "runner": "tests",
        "profile": "pr",
        "source_ref": "abc",
        "execution": {
          "plan": {
            "schema_version": 1,
            "profile": "pr",
            "registry": {"path": "verification/raft-invariants.yaml", "sha256": "0000000000000000000000000000000000000000000000000000000000000000", "size_bytes": 1},
            "manifest": {"path": "verification/raft-invariant-profiles.json", "sha256": "0000000000000000000000000000000000000000000000000000000000000000", "size_bytes": 1},
            "contract": {}
          },
          "invocation": {
            "program": "target/debug/rafter-invariants", "program_sha256": "0000000000000000000000000000000000000000000000000000000000000000", "arguments": ["run"],
            "current_dir": "/workspace/rafter", "environment": {}, "environment_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
          },
          "source": {
            "commit": "abc", "tree": "tree", "cargo_lock_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "cargo": "cargo test", "cargo_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "cargo_config_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "rustc": "rustc test", "rustc_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "target": "test-target", "build_profile": "test", "features": [], "tools": {},
            "environment_sha256": "0000000000000000000000000000000000000000000000000000000000000000", "clean": true
          },
          "checks": [],
          "duration_ms": 1,
          "peak_rss_kib": 1,
          "artifacts": [{"kind": "log", "path": "test.log", "sha256": "0000000000000000000000000000000000000000000000000000000000000000", "size_bytes": 1}]
        },
        "results": [],
        "unreviewed_override": true
    }"#;
    assert!(serde_json::from_str::<ResultBundle>(source).is_err());
}

#[test]
fn old_receipts_without_report_binding_field_are_rejected() {
    let (catalog, manifest) = loaded();
    let bundles = passing_bundles(&catalog, &manifest);
    let simulator_index = bundles
        .iter()
        .position(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle");
    let mut value =
        serde_json::to_value(&bundles[simulator_index]).expect("serialize simulator bundle");
    let liveness_check = value["execution"]["checks"]
        .as_array_mut()
        .expect("check array")
        .iter_mut()
        .find(|check| !check["simulator_liveness"].is_null())
        .expect("liveness check");
    liveness_check
        .as_object_mut()
        .expect("check object")
        .remove("simulator_liveness");
    let error = serde_json::from_value::<ResultBundle>(value)
        .expect_err("versioned receipts require an explicit report binding field");
    assert!(error.to_string().contains("simulator_liveness"));
}

#[test]
fn stale_bundle_is_red_never_green() {
    let (catalog, manifest) = loaded();
    let producer = producer_binding("artifacts/tests-producer");
    let bundle = ResultBundle {
        schema_version: crate::evidence::RESULT_SCHEMA_VERSION,
        runner: "tests".to_owned(),
        profile: "pr".to_owned(),
        source_ref: "old".to_owned(),
        execution: ExecutionReceipt {
            plan: plan_receipt(&manifest, "pr"),
            invocation: invocation_receipt("tests"),
            producer: producer.clone(),
            source: source_receipt("old"),
            checks: Vec::new(),
            duration_ms: 1,
            peak_rss_kib: 1,
            artifacts: vec![artifact("artifacts/tests.log"), producer.executable],
        },
        results: Vec::new(),
    };
    let report = aggregate(&catalog, &manifest, "pr", "new", &[bundle]).expect("report aggregates");
    assert_eq!(report.summary.green, 0);
    assert!(report.invariants.iter().all(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("evidence is stale"))));
}

#[test]
fn evidence_load_error_still_emits_exactly_44_red_verdicts() {
    let (catalog, manifest) = loaded();
    let bundles = passing_bundles(&catalog, &manifest);
    let plan = plan_receipt(&manifest, "pr");
    let request = crate::verification::VerificationRequest::new(
        &catalog,
        &manifest,
        &plan,
        "abc",
        Path::new("."),
        crate::verification::VerificationContext::ProducingJob,
    );
    let intake = crate::verification::verify_receipts_for_test(
        request,
        &bundles,
        vec![crate::verification::IntakeDefect::malformed(
            "parse artifacts/invariants/pr-tests.json: malformed JSON",
        )],
    )
    .expect("evidence intake verifies");
    let report = crate::verdict::reduce(&catalog, &manifest, &intake).expect("report aggregates");

    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
    assert!(report
        .invariants
        .iter()
        .all(|verdict| verdict.issues.iter().any(|issue| {
            issue.evidence_id == "aggregate/harness"
                && issue.message.contains("malformed JSON")
                && issue.classification == FailureClassification::HarnessError
                && issue.status == EvidenceStatus::Error
        })));
}

#[test]
fn one_layer_can_be_independently_verified_against_its_profile() {
    let (catalog, manifest) = loaded();
    let bundles = passing_bundles(&catalog, &manifest);
    let tests = bundles
        .iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle exists");

    verify_layer_bundle(&catalog, &manifest, "pr", "tests", tests)
        .expect("complete tests layer verifies");

    let mut incomplete = tests.clone();
    incomplete.results[0].status = EvidenceStatus::Incomplete;
    incomplete.execution.checks[0].completion = CheckCompletion::CoverageNotReached;
    assert!(verify_layer_bundle(&catalog, &manifest, "pr", "tests", &incomplete).is_err());
}
