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

fn passing_bundles(catalog: &Catalog, manifest: &ProfileManifest) -> Vec<ResultBundle> {
    let required = catalog.required_evidence(&manifest.profiles["pr"]);
    let mut by_runner = std::collections::BTreeMap::<String, Vec<EvidenceResult>>::new();
    for evidence in required.values().flatten() {
        by_runner
            .entry(evidence.layer.clone())
            .or_default()
            .push(EvidenceResult {
                invariant_id: evidence.invariant_id.clone(),
                evidence_id: evidence.evidence_id(),
                status: EvidenceStatus::Pass,
                classification: None,
                message: None,
                artifacts: Vec::new(),
            });
    }
    by_runner
        .into_iter()
        .map(|(runner, results)| ResultBundle {
            schema_version: 1,
            runner,
            profile: "pr".to_owned(),
            source_ref: "abc".to_owned(),
            results,
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
        "schema_version": 1,
        "runner": "tests",
        "profile": "pr",
        "source_ref": "abc",
        "results": [],
        "unreviewed_override": true
    }"#;
    assert!(serde_json::from_str::<ResultBundle>(source).is_err());
}

#[test]
fn stale_bundle_is_red_never_green() {
    let (catalog, manifest) = loaded();
    let bundle = ResultBundle {
        schema_version: 1,
        runner: "tests".to_owned(),
        profile: "pr".to_owned(),
        source_ref: "old".to_owned(),
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
