use std::path::PathBuf;

use super::*;

fn workspace_file(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

pub(crate) fn loaded() -> (Catalog, ProfileManifest) {
    let catalog = Catalog::load(&workspace_file("verification/raft-invariants.yaml"))
        .expect("registry loads");
    let manifest =
        ProfileManifest::load(&workspace_file("verification/raft-invariant-profiles.json"))
            .expect("profile manifest loads");
    (catalog, manifest)
}

fn artifact(path: &str) -> ArtifactRef {
    artifact_kind(path, "log")
}

fn artifact_kind(path: &str, kind: &str) -> ArtifactRef {
    ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    }
}

fn source_receipt(commit: &str) -> SourceReceipt {
    SourceReceipt {
        commit: commit.to_owned(),
        tree: "tree".to_owned(),
        cargo_lock_sha256: "0".repeat(64),
        cargo: "cargo test".to_owned(),
        cargo_sha256: "0".repeat(64),
        cargo_config_sha256: "0".repeat(64),
        rustc: "rustc test".to_owned(),
        rustc_sha256: "0".repeat(64),
        target: "test-target".to_owned(),
        build_profile: "test".to_owned(),
        features: Vec::new(),
        tools: std::collections::BTreeMap::new(),
        environment_sha256: "0".repeat(64),
        clean: true,
    }
}

pub(crate) fn plan_receipt(manifest: &ProfileManifest, profile: &str) -> ExecutionPlanReceipt {
    ExecutionPlanReceipt {
        schema_version: PLAN_SCHEMA_VERSION,
        profile: profile.to_owned(),
        registry: plan_input("verification/raft-invariants.yaml"),
        manifest: plan_input("verification/raft-invariant-profiles.json"),
        contract: manifest.profiles[profile].clone(),
    }
}

fn plan_input(path: &str) -> PlanInput {
    PlanInput {
        path: path.to_owned(),
        sha256: "0".repeat(64),
        size_bytes: 1,
    }
}

fn invocation_receipt(runner: &str) -> InvocationReceipt {
    InvocationReceipt {
        program: "target/debug/rafter-invariants".to_owned(),
        program_sha256: "0".repeat(64),
        arguments: vec![
            "run".to_owned(),
            "--profile".to_owned(),
            "pr".to_owned(),
            "--layer".to_owned(),
            runner.to_owned(),
        ],
        current_dir: "/workspace/rafter".to_owned(),
        environment: std::collections::BTreeMap::new(),
        environment_sha256: crate::producer::process::digest_environment(
            &std::collections::BTreeMap::new(),
        ),
    }
}

fn synthetic_check_id(descriptor: &EvidenceDescriptor) -> String {
    if descriptor.layer == "simulator" {
        return format!("simulator/{}", descriptor.evidence_id());
    }
    if descriptor.layer == "tla" {
        return "tla/RaftCi.cfg#Spec".to_owned();
    }
    descriptor
        .test
        .as_ref()
        .map_or_else(|| descriptor.evidence_id(), TestIdentity::check_id)
}

fn synthetic_observations(
    descriptors: &[EvidenceDescriptor],
) -> std::collections::BTreeMap<String, u64> {
    let descriptor = &descriptors[0];
    if descriptor.layer == "tests" {
        return std::collections::BTreeMap::from([
            ("discovered".to_owned(), 1),
            ("executed".to_owned(), 1),
            ("passed".to_owned(), 1),
        ]);
    }
    let Some(identity) = &descriptor.simulator else {
        if descriptor.layer == "tla" {
            let mut observations = std::collections::BTreeMap::from([
                ("configured_invariants".to_owned(), 9),
                ("tool_pin_verified".to_owned(), 1),
                ("trace_sample_passed".to_owned(), 1),
                ("generated_states".to_owned(), 130_000_000),
                ("distinct_states".to_owned(), 120_000_000),
                ("states_left_on_queue".to_owned(), 0),
                ("search_depth".to_owned(), 1),
            ]);
            observations.extend(
                crate::producer::tla_output::REGISTERED_PREDICATES
                    .into_iter()
                    .map(|predicate| (format!("detector_qualified:{predicate}"), 1)),
            );
            observations.extend(
                descriptors
                    .iter()
                    .map(|descriptor| (format!("checked:{}", descriptor.symbol), 1)),
            );
            return observations;
        }
        return std::collections::BTreeMap::new();
    };
    let liveness_reports = identity.liveness_report.as_ref().map(|_| {
        identity.checks.len() as u64 * identity.minimum_runs_per_check.unwrap_or_default() as u64
    });
    let mut observations = std::collections::BTreeMap::from([
        ("detector_qualified".to_owned(), 1),
        (
            identity.required_observation.clone(),
            liveness_reports.unwrap_or(identity.minimum_observation as u64),
        ),
    ]);
    if let (Some(protocol), Some(verifier)) = (
        identity.minimum_protocol_states,
        identity.minimum_verifier_states,
    ) {
        observations.insert("unique_protocol_states".to_owned(), protocol as u64);
        observations.insert("unique_verifier_states".to_owned(), verifier as u64);
    }
    for check in &identity.checks {
        let runs = identity.minimum_runs_per_check.unwrap_or(1) as u64;
        observations.insert(format!("passes:{check}"), runs);
        observations.insert(format!("runs:{check}"), runs);
        if let Some(steps) = identity.minimum_steps {
            observations.insert(format!("steps:{check}"), steps as u64);
        }
    }
    observations
}

fn synthetic_liveness_binding(
    descriptor: &EvidenceDescriptor,
) -> Option<crate::types::SimulatorLivenessBinding> {
    let identity = descriptor.simulator.as_ref()?;
    let contract = identity.liveness_report.clone()?;
    let runs = identity.minimum_runs_per_check?;
    let mut reports = identity
        .checks
        .iter()
        .flat_map(|check_id| {
            let execution_contract = crate::catalog::expected_execution_contract("pr", check_id)
                .expect("synthetic PR execution contract");
            (0..runs).map(move |index| crate::types::SimulatorLivenessReportBinding {
                check_id: check_id.clone(),
                seed: index as u64 + 1,
                execution_contract_sha256: crate::catalog::execution_contract_digest(
                    &execution_contract,
                ),
                execution_contract: execution_contract.clone(),
                report_sha256: format!("{:064x}", index + 1),
                round_limit: 1,
                rounds_used: 1,
            })
        })
        .collect::<Vec<_>>();
    reports.sort();
    Some(crate::types::SimulatorLivenessBinding {
        schema_version: 1,
        contract_sha256: crate::catalog::liveness_contract_digest(&contract),
        reports_sha256: crate::catalog::liveness_reports_digest(&reports),
        contract,
        reports,
    })
}

fn synthetic_artifacts(descriptor: &EvidenceDescriptor) -> Vec<ArtifactRef> {
    match descriptor.layer.as_str() {
        "tests" => vec![
            artifact_kind("artifacts/tests.log", "test-log"),
            artifact_kind("artifacts/tests.bin", "test-binary"),
        ],
        "simulator" => {
            let mut artifacts = vec![
                artifact_kind("artifacts/simulator.log", "simulator-log"),
                artifact_kind("artifacts/simulator.bin", "simulator-binary"),
            ];
            if descriptor
                .simulator
                .as_ref()
                .is_some_and(|identity| identity.negative_test.is_some())
            {
                artifacts.extend([
                    artifact_kind("artifacts/detector.log", "test-log"),
                    artifact_kind("artifacts/detector.bin", "test-binary"),
                ]);
            }
            artifacts
        }
        "tla" => {
            let mut kinds = [
                "tla-log",
                "tla-trace-log",
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
            .map(str::to_owned)
            .collect::<Vec<_>>();
            for probe in crate::producer::tla_output::DETECTOR_PROBES {
                kinds.push(
                    crate::producer::tla_output::detector_log_kind(probe)
                        .expect("registered detector probe"),
                );
                kinds.push(
                    crate::producer::tla_output::detector_config_kind(probe)
                        .expect("registered detector probe"),
                );
            }
            kinds
                .into_iter()
                .map(|kind| artifact_kind(&format!("artifacts/{kind}"), &kind))
                .collect()
        }
        runner => vec![artifact(&format!("artifacts/{runner}.log"))],
    }
}

pub(crate) fn passing_bundles(catalog: &Catalog, manifest: &ProfileManifest) -> Vec<ResultBundle> {
    let required = catalog.required_evidence(&manifest.profiles["pr"]);
    let mut by_runner = std::collections::BTreeMap::<String, Vec<EvidenceDescriptor>>::new();
    for evidence in required.values().flatten() {
        by_runner
            .entry(evidence.layer.clone())
            .or_default()
            .push(evidence.clone());
    }
    by_runner
        .into_iter()
        .map(|(runner, evidence)| {
            let mut groups = std::collections::BTreeMap::<String, Vec<EvidenceDescriptor>>::new();
            for descriptor in evidence {
                let check_id = synthetic_check_id(&descriptor);
                groups.entry(check_id).or_default().push(descriptor);
            }
            let mut results = Vec::new();
            let checks = groups
                .into_iter()
                .enumerate()
                .map(|(index, (check_id, descriptors))| {
                    let execution_id = format!("{runner}-execution-{index}");
                    let evidence_ids = descriptors
                        .iter()
                        .map(EvidenceDescriptor::evidence_id)
                        .collect::<Vec<_>>();
                    results.extend(descriptors.iter().cloned().map(|descriptor| {
                        let evidence_id = descriptor.evidence_id();
                        EvidenceResult {
                            invariant_id: descriptor.invariant_id,
                            evidence_id,
                            execution_id: execution_id.clone(),
                            status: EvidenceStatus::Pass,
                            classification: None,
                            message: None,
                            artifacts: Vec::new(),
                        }
                    }));
                    let completion = if runner == "tla" {
                        CheckCompletion::FrontierExhausted
                    } else {
                        CheckCompletion::Completed
                    };
                    CheckReceipt {
                        execution_id,
                        check_id,
                        evidence_ids,
                        completion,
                        observations: synthetic_observations(&descriptors),
                        simulator_liveness: synthetic_liveness_binding(&descriptors[0]),
                        duration_ms: 1,
                        peak_rss_kib: 1,
                        artifacts: synthetic_artifacts(&descriptors[0]),
                    }
                })
                .collect();
            let mut source = source_receipt("abc");
            if runner == "tla" {
                source.tools.insert(
                    "java".to_owned(),
                    ToolReceipt {
                        version: "java 21 test".to_owned(),
                        sha256: "0".repeat(64),
                    },
                );
            }
            ResultBundle {
                schema_version: crate::types::RESULT_SCHEMA_VERSION,
                runner: runner.clone(),
                profile: "pr".to_owned(),
                source_ref: "abc".to_owned(),
                execution: ExecutionReceipt {
                    plan: plan_receipt(manifest, "pr"),
                    invocation: invocation_receipt(&runner),
                    source,
                    checks,
                    duration_ms: 1,
                    peak_rss_kib: 1,
                    artifacts: vec![
                        artifact(&format!("artifacts/{runner}.log")),
                        artifact_kind(&format!("artifacts/{runner}-producer"), "producer-binary"),
                    ],
                },
                results,
            }
        })
        .collect()
}

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

fn relaxed_check_minimums(manifest: &mut ProfileManifest) {
    for contract in manifest.profiles.values_mut() {
        for runner in contract.runners.values_mut() {
            runner.minimum_observed_checks = 1;
        }
    }
}

fn assert_only_parent_red(report: &VerdictReport, invariant_id: &str) {
    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 43);
    assert_eq!(report.summary.red, 1);
    let red = report
        .invariants
        .iter()
        .filter(|verdict| verdict.status == VerdictStatus::Red)
        .map(|verdict| verdict.invariant_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(red, [invariant_id]);
}

#[test]
fn missing_direct_clause_keeps_only_parent_red() {
    let (mut catalog, mut manifest) = loaded();
    relaxed_check_minimums(&mut manifest);
    catalog
        .evidence
        .retain(|evidence| evidence.clause_id != "RD-06.b");
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
    let (mut catalog, mut manifest) = loaded();
    relaxed_check_minimums(&mut manifest);
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
    let result = tests
        .results
        .iter_mut()
        .find(|result| result.evidence_id == evidence_id)
        .expect("RD-06.b result exists");
    result.status = EvidenceStatus::Incomplete;
    result.classification = Some(FailureClassification::CoverageNotReached);
    result.message = Some("unknown-write branch was not reached".to_owned());
    result.artifacts = vec![artifact("artifacts/rd-06-b.log")];
    let check = tests
        .execution
        .checks
        .iter_mut()
        .find(|check| check.execution_id == result.execution_id)
        .expect("RD-06.b check exists");
    check.completion = CheckCompletion::CoverageNotReached;

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
    let physical_checks = simulator
        .iter()
        .map(|evidence| {
            (
                evidence.path.as_str(),
                evidence.symbol.as_str(),
                evidence.negative_fixture.as_deref(),
                evidence
                    .simulator
                    .as_ref()
                    .map(|identity| identity.required_observation.as_str()),
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(physical_checks.len(), 54);
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
    assert!(report.invariants.iter().all(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("exact observations"))));
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
        "schema_version": 7,
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
    let bundle = ResultBundle {
        schema_version: crate::types::RESULT_SCHEMA_VERSION,
        runner: "tests".to_owned(),
        profile: "pr".to_owned(),
        source_ref: "old".to_owned(),
        execution: ExecutionReceipt {
            plan: plan_receipt(&manifest, "pr"),
            invocation: invocation_receipt("tests"),
            source: source_receipt("old"),
            checks: Vec::new(),
            duration_ms: 1,
            peak_rss_kib: 1,
            artifacts: vec![
                artifact("artifacts/tests.log"),
                artifact_kind("artifacts/tests-producer", "producer-binary"),
            ],
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
    let report = aggregate_with_harness_errors(
        &catalog,
        &manifest,
        "pr",
        "abc",
        &bundles,
        &["parse artifacts/invariants/pr-tests.json: malformed JSON".to_owned()],
    )
    .expect("report aggregates");

    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
    assert!(report.invariants.iter().all(|verdict| verdict
        .issues
        .iter()
        .any(|issue| issue.message.contains("malformed JSON"))));
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

#[test]
fn renderers_emit_every_invariant() {
    let (catalog, manifest) = loaded();
    let report = aggregate(&catalog, &manifest, "pr", "abc", &[]).expect("report aggregates");
    let markdown = render_markdown(&report);
    let junit = render_junit(&report);
    for invariant_id in &catalog.ids {
        assert!(markdown.contains(invariant_id));
        assert!(junit.contains(invariant_id));
    }
}
