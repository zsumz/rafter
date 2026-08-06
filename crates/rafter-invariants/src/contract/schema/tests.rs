//! Scenarios: checked-in schemas and semantic validation fail closed under tampering.

use crate::{
    evidence::{validate_result_bundle, validate_result_value},
    verdict::{validate_verdict_report, validate_verdict_value},
};

#[test]
fn rust_receipts_and_reports_conform_to_distinct_checked_in_schemas() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundles = crate::tests::passing_bundles(&catalog, &manifest);
    for bundle in &bundles {
        validate_result_bundle(bundle).expect("synthetic bundle conforms");
    }
    let report = crate::tests::aggregate_unverified(&catalog, &manifest, "pr", "abc", &bundles)
        .expect("synthetic report aggregates");
    validate_verdict_report(&report, &catalog, &manifest).expect("aggregate report conforms");
    assert_ne!(
        crate::evidence::RESULT_SCHEMA_VERSION,
        crate::verdict::VERDICT_SCHEMA_VERSION
    );
}

#[test]
fn schema_validation_rejects_version_and_shape_tampering() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .next()
        .expect("bundle");
    let mut value = serde_json::to_value(bundle).expect("bundle serializes");
    value["schema_version"] = serde_json::json!(u64::MAX);
    assert!(validate_result_value(&value).is_err());
    value["schema_version"] = serde_json::json!(11);
    assert!(validate_result_value(&value).is_err());
    value["schema_version"] = serde_json::json!(crate::evidence::RESULT_SCHEMA_VERSION);
    value["execution"]
        .as_object_mut()
        .expect("execution object")
        .remove("producer");
    assert!(validate_result_value(&value).is_err());
    value = passing_bundle_value(&catalog, &manifest);
    value["execution"]["unreviewed"] = serde_json::json!(true);
    assert!(validate_result_value(&value).is_err());

    value = passing_bundle_value(&catalog, &manifest);
    value["execution"]["source"]
        .as_object_mut()
        .expect("source object")
        .remove("materialization");
    assert!(validate_result_value(&value).is_err());

    value = passing_bundle_value(&catalog, &manifest);
    value["execution"]["source"]["materialization"]["contract"] =
        serde_json::json!("git-status-only-v0");
    assert!(validate_result_value(&value).is_err());
}

#[test]
fn result_schema_rejects_receipt_resource_limit_violations() {
    let (catalog, manifest) = crate::tests::loaded();
    let value = passing_bundle_value(&catalog, &manifest);

    let mut oversized_artifact = value.clone();
    oversized_artifact["execution"]["artifacts"][0]["size_bytes"] =
        serde_json::json!(268_435_457_u64);
    assert!(validate_result_value(&oversized_artifact).is_err());

    let mut oversized_plan_input = value.clone();
    oversized_plan_input["execution"]["plan"]["registry"]["size_bytes"] =
        serde_json::json!(8_388_609_u64);
    assert!(validate_result_value(&oversized_plan_input).is_err());

    let mut oversized_kind = value;
    oversized_kind["execution"]["artifacts"][0]["kind"] = serde_json::json!("x".repeat(129));
    assert!(validate_result_value(&oversized_kind).is_err());
}

#[test]
fn schema_validation_diagnostic_is_bounded_under_many_independent_errors() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut value = passing_bundle_value(&catalog, &manifest);
    value["results"] =
        serde_json::Value::Array((0..4096).map(|_| serde_json::json!({})).collect::<Vec<_>>());
    let error = validate_result_value(&value).expect_err("invalid results are rejected");
    assert!(error.len() <= 4_300, "diagnostic was {} bytes", error.len());
}

#[test]
fn schema_validation_diagnostic_identifies_the_invalid_field() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut value = passing_bundle_value(&catalog, &manifest);
    value["execution"]["source"]["materialization"]["tracked_entries"] = serde_json::json!(0);

    let error = validate_result_value(&value).expect_err("zero tracked entries are rejected");

    assert!(
        error.contains("/execution/source/materialization/tracked_entries"),
        "schema diagnostic omitted the invalid field: {error}"
    );
}

#[test]
fn result_schema_rejects_invalid_profile_owned_simulator_check_contracts() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "simulator")
        .expect("simulator bundle");
    let value = serde_json::to_value(bundle).expect("bundle serializes");
    let path = [
        "execution",
        "plan",
        "contract",
        "runners",
        "simulator",
        "simulator_checks",
        "raft-election",
    ];

    let mut unknown = value.clone();
    nested(&mut unknown, &path)["unreviewed"] = serde_json::json!(true);
    assert!(validate_result_value(&unknown).is_err());

    let mut zero_floor = value.clone();
    nested(&mut zero_floor, &path)["minimum_protocol_states"] = serde_json::json!(0);
    assert!(validate_result_value(&zero_floor).is_err());

    let mut duplicate = value.clone();
    nested(&mut duplicate, &path)["required_observations"] = serde_json::json!(["same", "same"]);
    assert!(validate_result_value(&duplicate).is_err());

    let mut blank = value;
    nested(&mut blank, &path)["required_observations"] = serde_json::json!(["  "]);
    assert!(validate_result_value(&blank).is_err());
}

#[test]
fn verdict_validation_rejects_duplicate_ids_and_forged_counts() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundles = crate::tests::passing_bundles(&catalog, &manifest);
    let report = crate::tests::aggregate_unverified(&catalog, &manifest, "pr", "abc", &bundles)
        .expect("synthetic report aggregates");

    let mut duplicate = report.clone();
    duplicate.invariants[1] = duplicate.invariants[0].clone();
    assert!(validate_verdict_report(&duplicate, &catalog, &manifest).is_err());

    let mut disguised_duplicate = report.clone();
    disguised_duplicate.invariants[1].invariant_id =
        disguised_duplicate.invariants[0].invariant_id.clone();
    disguised_duplicate.invariants[1].status = crate::VerdictStatus::Red;
    assert!(validate_verdict_report(&disguised_duplicate, &catalog, &manifest).is_err());

    let mut forged_summary = report.clone();
    forged_summary.summary.green = 43;
    forged_summary.summary.red = 1;
    assert!(validate_verdict_report(&forged_summary, &catalog, &manifest).is_err());

    let mut forged_row = report.clone();
    forged_row.invariants[0].passed_clauses = 0;
    assert!(validate_verdict_report(&forged_row, &catalog, &manifest).is_err());

    let mut fabricated_contract = report;
    fabricated_contract.invariants[0].required_evidence = 0;
    fabricated_contract.invariants[0].passed_evidence = 0;
    for clause in &mut fabricated_contract.invariants[0].clauses {
        clause.required_evidence = 0;
        clause.passed_evidence = 0;
    }
    assert!(validate_verdict_report(&fabricated_contract, &catalog, &manifest).is_err());

    let mut mismatched_issue =
        crate::tests::aggregate_unverified(&catalog, &manifest, "pr", "abc", &[])
            .expect("missing evidence aggregates red");
    mismatched_issue.invariants[0].issues[0].classification =
        crate::FailureClassification::InvariantViolation;
    assert!(validate_verdict_report(&mismatched_issue, &catalog, &manifest).is_err());
}

#[test]
fn verdict_schema_can_represent_an_internally_red_zero_evidence_row() {
    let (catalog, manifest) = crate::tests::loaded();
    let mut report = crate::tests::aggregate_unverified(&catalog, &manifest, "pr", "abc", &[])
        .expect("missing evidence aggregates red");
    let row = &mut report.invariants[0];
    row.required_evidence = 0;
    row.passed_evidence = 0;
    for clause in &mut row.clauses {
        clause.required_evidence = 0;
        clause.passed_evidence = 0;
    }
    let value = serde_json::to_value(report).expect("red report serializes");
    validate_verdict_value(&value).expect("schema permits an internally red zero-evidence row");
}

fn passing_bundle_value(
    catalog: &crate::Catalog,
    manifest: &crate::ProfileManifest,
) -> serde_json::Value {
    serde_json::to_value(
        crate::tests::passing_bundles(catalog, manifest)
            .into_iter()
            .next()
            .expect("bundle"),
    )
    .expect("bundle serializes")
}

fn nested<'a>(value: &'a mut serde_json::Value, path: &[&str]) -> &'a mut serde_json::Value {
    let mut current = value;
    for segment in path {
        current = &mut current[*segment];
    }
    current
}
