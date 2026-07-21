//! Scenarios: paths become an opaque intake only after complete authentication.

use std::{path::Path, path::PathBuf};

use super::{
    require_passing_layer, verify_paths, verify_receipts_for_test, IntakeDefect, IntakeDefectKind,
    VerificationRequest,
};

mod json;
mod profile_budget;
mod replay;
mod resource_limits;

#[test]
fn defect_kind_identities_are_stable_and_exhaustive() {
    assert_eq!(IntakeDefectKind::Missing.as_str(), "missing");
    assert_eq!(IntakeDefectKind::Malformed.as_str(), "malformed");
    assert_eq!(IntakeDefectKind::Stale.as_str(), "stale");
    assert_eq!(IntakeDefectKind::Unverifiable.as_str(), "unverifiable");
}

#[test]
fn empty_intake_preserves_the_existing_row_local_44_red_wire_contract() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = request(&catalog, &manifest, &plan, "abc", Path::new("."));
    let intake = verify_receipts_for_test(request, &[], Vec::new())
        .expect("empty evidence has a typed intake");

    assert!(intake.defects().is_empty());
    let report =
        crate::verdict::reduce(&catalog, &manifest, &intake).expect("aggregate empty intake");
    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
    assert!(report.invariants.iter().all(|verdict| {
        !verdict
            .issues
            .iter()
            .any(|issue| issue.evidence_id == "aggregate/harness")
    }));
}

#[test]
fn complete_receipt_intake_is_bound_to_profile_and_source_and_stays_44_green() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = request(&catalog, &manifest, &plan, "abc", Path::new("."));
    let bundles = crate::tests::passing_bundles(&catalog, &manifest);
    let intake = verify_receipts_for_test(request, &bundles, Vec::new())
        .expect("passing synthetic receipts verify");

    assert_eq!(intake.profile(), "pr");
    assert_eq!(intake.source_ref(), "abc");
    assert!(intake.defects().is_empty());
    let expected = catalog
        .required_evidence(&manifest.profiles["pr"])
        .values()
        .map(Vec::len)
        .sum::<usize>();
    assert_eq!(intake.accepted().len(), expected);

    let report =
        crate::verdict::reduce(&catalog, &manifest, &intake).expect("aggregate passing intake");
    assert_eq!(report.summary.total, 44);
    assert_eq!(report.summary.green, 44);
    assert_eq!(report.summary.red, 0);
}

#[test]
fn typed_defects_preserve_the_versioned_aggregate_harness_projection() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = request(&catalog, &manifest, &plan, "abc", Path::new("."));
    let seeded = [
        IntakeDefect::missing("missing fixture"),
        IntakeDefect::malformed("malformed fixture"),
        IntakeDefect::stale("stale fixture"),
        IntakeDefect::unverifiable("unverifiable fixture"),
    ];
    let intake = verify_receipts_for_test(request, &[], seeded.to_vec())
        .expect("defective evidence has a typed intake");

    for expected in seeded {
        assert!(intake.defects().contains(&expected));
    }
    let report =
        crate::verdict::reduce(&catalog, &manifest, &intake).expect("aggregate defective intake");
    assert!(report.invariants.iter().all(|verdict| {
        verdict
            .issues
            .iter()
            .filter(|issue| issue.evidence_id == "aggregate/harness")
            .count()
            == 4
    }));
}

#[test]
fn stale_receipt_is_not_accepted() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = request(
        &catalog,
        &manifest,
        &plan,
        "different-source",
        Path::new("."),
    );
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    let intake = verify_receipts_for_test(request, std::slice::from_ref(&bundle), Vec::new())
        .expect("stale evidence has a typed intake");

    assert!(intake.accepted().is_empty());
    assert!(intake
        .defects()
        .iter()
        .any(|defect| defect.kind() == IntakeDefectKind::Stale));
}

#[test]
fn duplicate_results_are_removed_as_unverifiable_ambiguity() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = request(&catalog, &manifest, &plan, "abc", Path::new("."));
    let mut bundles = crate::tests::passing_bundles(&catalog, &manifest);
    let duplicate = bundles
        .iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle")
        .clone();
    let ambiguous_id = duplicate.results[0].evidence_id.clone();
    bundles.push(duplicate);

    let intake = verify_receipts_for_test(request, &bundles, Vec::new())
        .expect("duplicate evidence has a typed intake");

    assert!(!intake.accepted().contains_key(&ambiguous_id));
    assert!(intake.defects().iter().any(|defect| {
        defect.kind() == IntakeDefectKind::Unverifiable && defect.message().contains(&ambiguous_id)
    }));
}

#[test]
fn one_complete_layer_uses_the_same_receipt_acceptance_boundary() {
    let (catalog, manifest) = crate::tests::loaded();
    let plan = crate::tests::plan_receipt(&manifest, "pr");
    let request = request(&catalog, &manifest, &plan, "abc", Path::new("."));
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("tests bundle");
    let intake = verify_receipts_for_test(request, std::slice::from_ref(&bundle), Vec::new())
        .expect("tests layer verifies");

    require_passing_layer(request, "tests", &intake).expect("complete passing layer is accepted");
}

#[test]
fn missing_result_path_is_typed_missing_and_44_red() {
    let path = unique_path("missing");
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
        .expect("missing path has a typed intake");

    assert!(intake.accepted().is_empty());
    assert!(intake
        .defects()
        .iter()
        .any(|defect| defect.kind() == IntakeDefectKind::Missing));
    assert!(intake.defects().iter().any(|defect| {
        defect.kind() == IntakeDefectKind::Unverifiable
            && defect.message().contains("exactly 3 result receipt paths")
    }));
    let report =
        crate::verdict::reduce(&catalog, &manifest, &intake).expect("aggregate missing path");
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
}

fn request<'a>(
    catalog: &'a crate::Catalog,
    manifest: &'a crate::ProfileManifest,
    plan: &'a crate::ExecutionPlanReceipt,
    source_ref: &'a str,
    root: &'a Path,
) -> VerificationRequest<'a> {
    VerificationRequest::new(catalog, manifest, plan, source_ref, root)
}

pub(super) fn unique_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rafter-invariant-intake-{label}-{}-{}.json",
        std::process::id(),
        std::thread::current().name().unwrap_or("unnamed")
    ))
}
