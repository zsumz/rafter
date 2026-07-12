use std::path::PathBuf;

use super::*;

fn workspace_file(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

fn loaded() -> (Catalog, ProfileManifest) {
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
                ("detector_negative_passed".to_owned(), 1),
                ("generated_states".to_owned(), 130_000_000),
                ("distinct_states".to_owned(), 120_000_000),
                ("states_left_on_queue".to_owned(), 0),
                ("search_depth".to_owned(), 1),
            ]);
            observations.extend(
                descriptors
                    .iter()
                    .map(|descriptor| (format!("checked:{}", descriptor.symbol), 1)),
            );
            return observations;
        }
        return std::collections::BTreeMap::new();
    };
    let mut observations = std::collections::BTreeMap::from([
        ("detector_qualified".to_owned(), 1),
        (
            identity.required_observation.clone(),
            identity.minimum_observation as u64,
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
        "tla" => [
            "tla-log",
            "tla-trace-log",
            "tla-detector-log",
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
        .map(|kind| artifact_kind(&format!("artifacts/{kind}"), kind))
        .collect(),
        runner => vec![artifact(&format!("artifacts/{runner}.log"))],
    }
}

fn passing_bundles(catalog: &Catalog, manifest: &ProfileManifest) -> Vec<ResultBundle> {
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
            let runner_contract = &manifest.profiles["pr"].runners[&runner];
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
                schema_version: 5,
                runner: runner.clone(),
                profile: "pr".to_owned(),
                source_ref: "abc".to_owned(),
                execution: ExecutionReceipt {
                    producer: runner_contract.producer.clone(),
                    command: runner_contract.command.clone(),
                    configuration: runner_contract.configuration.clone(),
                    source,
                    checks,
                    duration_ms: 1,
                    peak_rss_kib: 1,
                    artifacts: vec![artifact(&format!("artifacts/{runner}.log"))],
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
    assert_eq!(report.summary.green, 44);
    assert_eq!(report.summary.red, 0);
    assert_eq!(report.artifacts.len(), 3);
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
    assert_eq!(simulator.len(), 39);
    assert_eq!(ids.len(), simulator.len());
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
fn tla_pass_requires_every_framed_predicate_observation() {
    let (catalog, manifest) = loaded();
    let mut bundles = passing_bundles(&catalog, &manifest);
    let tla = bundles
        .iter_mut()
        .find(|bundle| bundle.runner == "tla")
        .expect("TLA bundle exists");
    tla.execution.checks[0]
        .observations
        .remove("checked:ElectionSafety");

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
    assert!(report.invariants.iter().all(|verdict| verdict
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
    assert!(report.invariants.iter().all(|verdict| verdict
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
        "schema_version": 5,
        "runner": "tests",
        "profile": "pr",
        "source_ref": "abc",
        "execution": {
          "producer": "tests-v1",
          "command": ["test"],
          "configuration": {"suite": "test"},
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
fn stale_bundle_is_red_never_green() {
    let (catalog, manifest) = loaded();
    let bundle = ResultBundle {
        schema_version: 5,
        runner: "tests".to_owned(),
        profile: "pr".to_owned(),
        source_ref: "old".to_owned(),
        execution: ExecutionReceipt {
            producer: "rafter-invariants-tests-v1".to_owned(),
            command: manifest.profiles["pr"].runners["tests"].command.clone(),
            configuration: manifest.profiles["pr"].runners["tests"]
                .configuration
                .clone(),
            source: source_receipt("old"),
            checks: Vec::new(),
            duration_ms: 1,
            peak_rss_kib: 1,
            artifacts: vec![artifact("artifacts/tests.log")],
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
