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
    ArtifactRef {
        kind: "log".to_owned(),
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
        rustc: "rustc test".to_owned(),
        target: "test-target".to_owned(),
        build_profile: "test".to_owned(),
        features: Vec::new(),
        clean: true,
    }
}

fn passing_bundles(catalog: &Catalog, manifest: &ProfileManifest) -> Vec<ResultBundle> {
    let required = catalog.required_evidence(&manifest.profiles["pr"]);
    let mut by_runner = std::collections::BTreeMap::<String, Vec<EvidenceResult>>::new();
    for (index, evidence) in required.values().flatten().enumerate() {
        by_runner
            .entry(evidence.layer.clone())
            .or_default()
            .push(EvidenceResult {
                invariant_id: evidence.invariant_id.clone(),
                evidence_id: evidence.evidence_id(),
                execution_id: format!("execution-{index}"),
                status: EvidenceStatus::Pass,
                classification: None,
                message: None,
                artifacts: Vec::new(),
            });
    }
    by_runner
        .into_iter()
        .map(|(runner, results)| {
            let runner_contract = &manifest.profiles["pr"].runners[&runner];
            let checks = results
                .iter()
                .map(|result| CheckReceipt {
                    execution_id: result.execution_id.clone(),
                    check_id: result.evidence_id.clone(),
                    evidence_ids: vec![result.evidence_id.clone()],
                    completion: CheckCompletion::Completed,
                    observations: std::collections::BTreeMap::new(),
                    duration_ms: 1,
                    peak_rss_kib: 1,
                    artifacts: vec![artifact(&format!("artifacts/{runner}.log"))],
                })
                .collect();
            ResultBundle {
                schema_version: 2,
                runner: runner.clone(),
                profile: "pr".to_owned(),
                source_ref: "abc".to_owned(),
                execution: ExecutionReceipt {
                    producer: runner_contract.producer.clone(),
                    command: runner_contract.command.clone(),
                    configuration: runner_contract.configuration.clone(),
                    source: source_receipt("abc"),
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
        "schema_version": 2,
        "runner": "tests",
        "profile": "pr",
        "source_ref": "abc",
        "execution": {
          "producer": "tests-v1",
          "command": ["test"],
          "configuration": {"suite": "test"},
          "source": {
            "commit": "abc", "tree": "tree", "cargo_lock_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "rustc": "rustc test", "target": "test-target", "build_profile": "test", "features": [], "clean": true
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
        schema_version: 2,
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
