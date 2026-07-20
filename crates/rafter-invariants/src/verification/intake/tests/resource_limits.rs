//! Scenarios: bounded receipt intake rejects hostile resources before observation.

use std::{
    fs,
    path::{Path, PathBuf},
};

use super::super::{
    preflight::profile_artifacts, verify_layer_paths, verify_paths, IntakeDefectKind,
};
use super::{request, unique_path};

#[test]
fn oversized_result_receipt_is_rejected_before_json_allocation() {
    let path = unique_path("oversized");
    let file = fs::File::create(&path).expect("create sparse oversized receipt");
    file.set_len(crate::verification::bundle::MAX_RECEIPT_BYTES + 1)
        .expect("size sparse receipt");
    drop(file);
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = request(
        &catalog,
        &manifest,
        &plan,
        "abc",
        path.parent().expect("parent"),
    );
    let intake = verify_paths(request, std::slice::from_ref(&path), Vec::new())
        .expect("oversized evidence has a typed intake");
    let _ = fs::remove_file(&path);

    assert!(intake.accepted().is_empty());
    assert!(intake.defects().iter().any(|defect| {
        defect.kind() == IntakeDefectKind::Unverifiable && defect.message().contains("exceeding")
    }));
}

#[cfg(unix)]
#[test]
fn symlink_and_fifo_result_receipts_are_rejected_as_non_regular() {
    use std::os::unix::fs::symlink;

    let target = unique_path("receipt-target");
    fs::write(&target, b"{}\n").expect("write receipt target");
    let symlink_path = unique_path("receipt-symlink");
    symlink(&target, &symlink_path).expect("create receipt symlink");
    assert_non_regular_receipt(&symlink_path);
    fs::remove_file(&symlink_path).expect("remove receipt symlink");
    fs::remove_file(&target).expect("remove receipt target");

    let fifo = unique_path("receipt-fifo");
    let status = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("invoke mkfifo");
    assert!(status.success(), "create receipt FIFO");
    assert_non_regular_receipt(&fifo);
    fs::remove_file(&fifo).expect("remove receipt FIFO");
}

#[cfg(unix)]
fn assert_non_regular_receipt(path: &Path) {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = request(
        &catalog,
        &manifest,
        &plan,
        "abc",
        path.parent().expect("parent"),
    );
    let intake = verify_paths(request, &[path.to_path_buf()], Vec::new())
        .expect("non-regular evidence has a typed intake");
    assert!(intake.accepted().is_empty());
    assert!(intake.defects().iter().any(|defect| {
        defect.kind() == IntakeDefectKind::Unverifiable
            && defect.message().contains(&path.display().to_string())
    }));
}

#[test]
fn invalid_json_and_utf8_are_typed_malformed() {
    for (label, contents) in [("json", b"{not-json".as_slice()), ("utf8", &[0xff, 0xfe])] {
        let path = unique_path(label);
        fs::write(&path, contents).expect("write malformed fixture");
        let (catalog, manifest) = crate::tests::loaded();
        let plan = crate::tests::plan_receipt(&manifest, "pr");
        let request = request(
            &catalog,
            &manifest,
            &plan,
            "abc",
            path.parent().expect("parent"),
        );
        let intake = verify_paths(request, std::slice::from_ref(&path), Vec::new())
            .expect("malformed path has a typed intake");
        let _ = fs::remove_file(&path);

        assert!(intake.accepted().is_empty());
        assert!(intake
            .defects()
            .iter()
            .any(|defect| defect.kind() == IntakeDefectKind::Malformed));
    }
}

#[test]
fn valid_json_outside_the_result_schema_is_typed_malformed() {
    let path = unique_path("schema");
    fs::write(&path, b"{}\n").expect("write schema-invalid fixture");
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = request(
        &catalog,
        &manifest,
        &plan,
        "abc",
        path.parent().expect("parent"),
    );
    let intake = verify_paths(request, std::slice::from_ref(&path), Vec::new())
        .expect("schema-invalid path has a typed intake");
    let _ = fs::remove_file(&path);

    assert!(intake.accepted().is_empty());
    assert!(intake
        .defects()
        .iter()
        .any(|defect| defect.kind() == IntakeDefectKind::Malformed));
}

#[test]
fn excess_receipt_paths_are_rejected_without_opening_any_path() {
    let paths = (0..5)
        .map(|index| unique_path(&format!("excess-{index}")))
        .collect::<Vec<_>>();
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = request(&catalog, &manifest, &plan, "abc", Path::new("."));
    let intake = verify_paths(request, &paths, Vec::new())
        .expect("excess evidence paths have a typed intake");

    assert!(intake.accepted().is_empty());
    assert_eq!(intake.defects().len(), 1);
    assert!(intake.defects()[0].message().contains("accepts at most 3"));
    assert!(!intake
        .defects()
        .iter()
        .any(|defect| defect.kind() == IntakeDefectKind::Missing));
}

#[test]
fn duplicate_runner_set_is_rejected_before_artifact_opening() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let mut bundles = crate::tests::passing_bundles(&catalog, &manifest);
    bundles
        .iter_mut()
        .find(|bundle| bundle.runner == "tla")
        .expect("TLA bundle")
        .runner = "tests".to_owned();
    let paths = bundles
        .iter()
        .enumerate()
        .map(|(index, bundle)| {
            let path = unique_path(&format!("duplicate-runner-{index}"));
            fs::write(
                &path,
                serde_json::to_vec_pretty(bundle).expect("serialize bundle"),
            )
            .expect("write bundle");
            path
        })
        .collect::<Vec<_>>();
    let request = request(&catalog, &manifest, &plan, "abc", Path::new("."));
    let intake = verify_paths(request, &paths, Vec::new())
        .expect("duplicate runner evidence has a typed intake");
    for path in &paths {
        let _ = fs::remove_file(path);
    }

    assert!(intake.accepted().is_empty());
    assert!(intake.defects().iter().any(|defect| {
        defect
            .message()
            .contains("exactly one bundle for each trusted runner")
    }));
    assert!(!intake
        .defects()
        .iter()
        .any(|defect| defect.message().contains("open artifact")));
}

#[test]
fn profile_artifact_budget_is_checked_before_any_artifact_open() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "weekly");
    let mut bundles = crate::tests::passing_bundles_for_profile(&catalog, &manifest, "weekly");
    for bundle in &mut bundles {
        let (kind, count) = match bundle.runner.as_str() {
            "tests" => ("test-binary", 3),
            "simulator" => ("simulator-binary", 3),
            "tla" => ("tla-tool", 3),
            "maelstrom" => ("maelstrom-tool-jar", 15),
            runner => panic!("unexpected runner {runner}"),
        };
        for index in 0..count {
            bundle.execution.artifacts.push(crate::ArtifactRef {
                kind: kind.to_owned(),
                path: format!("artifacts/{}/profile-budget-{index}", bundle.runner),
                sha256: format!("{index:064x}"),
                size_bytes: crate::evidence::limits::MAX_ARTIFACT_BYTES,
            });
        }
    }
    let trusted = bundles
        .into_iter()
        .map(|bundle| {
            (
                bundle.runner.clone(),
                PathBuf::from(format!("{}.json", bundle.runner)),
                bundle,
            )
        })
        .collect::<Vec<_>>();
    let request = request(&catalog, &manifest, &plan, "abc", Path::new("."));
    let budget = crate::verification::bundle::ProfileBudget::for_trusted("weekly", trusted.len())
        .expect("weekly profile budget");
    let error = profile_artifacts(&request, &trusted, budget)
        .expect_err("profile artifact budget must reject before opening paths");
    assert!(
        error.to_string().contains("weekly profile declares"),
        "{error}"
    );
}

#[test]
fn path_authentication_rejects_wrong_profile_source_and_layer_before_source_observation() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");

    let mut wrong_profile = bundle.clone();
    wrong_profile.profile = "nightly".to_owned();
    assert_serialized_defect(
        &catalog,
        &manifest,
        &plan,
        &bundle.source_ref,
        "wrong-profile",
        &wrong_profile,
        None,
        IntakeDefectKind::Stale,
    );

    let mut wrong_source = bundle.clone();
    wrong_source.source_ref = "different-source".to_owned();
    assert_serialized_defect(
        &catalog,
        &manifest,
        &plan,
        &bundle.source_ref,
        "wrong-source",
        &wrong_source,
        None,
        IntakeDefectKind::Stale,
    );

    assert_serialized_defect(
        &catalog,
        &manifest,
        &plan,
        &bundle.source_ref,
        "wrong-layer",
        &bundle,
        Some("simulator"),
        IntakeDefectKind::Unverifiable,
    );
}

#[test]
fn wrong_active_plan_is_stale_before_artifact_budget_preflight() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .next()
        .expect("bundle");
    for index in 0..5 {
        bundle.execution.artifacts.push(crate::ArtifactRef {
            kind: "test-log".to_owned(),
            path: format!("artifacts/stale-oversize-{index}"),
            sha256: format!("{index:064x}"),
            size_bytes: crate::evidence::limits::MAX_ARTIFACT_BYTES,
        });
    }
    let path = unique_path("stale-plan");
    fs::write(
        &path,
        serde_json::to_vec_pretty(&bundle).expect("serialize bundle"),
    )
    .expect("write bundle");
    let mut active_plan = bundle.execution.plan.clone();
    active_plan.registry.sha256 = "f".repeat(64);
    let request = request(
        &catalog,
        &manifest,
        &active_plan,
        &bundle.source_ref,
        path.parent().expect("parent"),
    );
    let intake = verify_paths(request, std::slice::from_ref(&path), Vec::new())
        .expect("stale plan has a typed intake");
    let _ = fs::remove_file(&path);

    assert!(intake.accepted().is_empty());
    assert!(intake
        .defects()
        .iter()
        .any(|defect| defect.kind() == IntakeDefectKind::Stale));
    assert!(!intake
        .defects()
        .iter()
        .any(|defect| defect.message().contains("artifact bundle declares")));
}

#[allow(clippy::too_many_arguments)]
fn assert_serialized_defect(
    catalog: &crate::Catalog,
    manifest: &crate::ProfileManifest,
    plan: &crate::ExecutionPlanReceipt,
    source_ref: &str,
    label: &str,
    bundle: &crate::evidence::ResultBundle,
    required_layer: Option<&str>,
    expected: IntakeDefectKind,
) {
    let path = unique_path(label);
    fs::write(
        &path,
        serde_json::to_vec_pretty(bundle).expect("serialize fixture bundle"),
    )
    .expect("write fixture bundle");
    let request = request(
        catalog,
        manifest,
        plan,
        source_ref,
        path.parent().expect("parent"),
    );
    let intake = match required_layer {
        Some(layer) => verify_layer_paths(request, layer, path.clone()),
        None => verify_paths(request, std::slice::from_ref(&path), Vec::new()),
    }
    .expect("invalid serialized evidence has a typed intake");
    let _ = fs::remove_file(path);

    assert!(intake.accepted().is_empty());
    assert!(intake
        .defects()
        .iter()
        .any(|defect| defect.kind() == expected));
    let report = crate::verdict::reduce(catalog, manifest, &intake)
        .expect("aggregate invalid serialized evidence");
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
}
