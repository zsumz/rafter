use serde_json::Value;

const RESULT_SCHEMA: &str = include_str!("../../../verification/invariant-result-schema.json");
const VERDICT_SCHEMA: &str = include_str!("../../../verification/invariant-verdict-schema.json");

pub(crate) fn validate_result_bundle(bundle: &crate::ResultBundle) -> Result<(), String> {
    let value = serde_json::to_value(bundle)
        .map_err(|error| format!("serialize invariant result bundle: {error}"))?;
    validate(&value, RESULT_SCHEMA, "invariant result bundle")
}

pub(crate) fn validate_result_value(value: &Value) -> Result<(), String> {
    validate(value, RESULT_SCHEMA, "invariant result bundle")
}

pub(crate) fn validate_verdict_report(
    report: &crate::VerdictReport,
    catalog: &crate::Catalog,
    manifest: &crate::ProfileManifest,
) -> Result<(), String> {
    let value = serde_json::to_value(report)
        .map_err(|error| format!("serialize invariant verdict report: {error}"))?;
    validate_verdict_value(&value)?;
    validate_verdict_semantics(report, catalog, manifest)
}

pub(crate) fn validate_verdict_value(value: &Value) -> Result<(), String> {
    validate(value, VERDICT_SCHEMA, "invariant verdict report")
}

fn validate_verdict_semantics(
    report: &crate::VerdictReport,
    catalog: &crate::Catalog,
    manifest: &crate::ProfileManifest,
) -> Result<(), String> {
    use crate::VerdictStatus;

    manifest
        .validate(catalog)
        .map_err(|error| error.to_string())?;
    let contract = manifest
        .profiles
        .get(&report.profile)
        .ok_or_else(|| format!("unknown verdict profile {}", report.profile))?;
    let required = catalog.required_evidence(contract);
    let reported_ids = report
        .invariants
        .iter()
        .map(|verdict| verdict.invariant_id.as_str())
        .collect::<Vec<_>>();
    let expected_ids = catalog.ids.iter().map(String::as_str).collect::<Vec<_>>();
    if reported_ids != expected_ids {
        return Err(
            "invariant verdict rows must exactly match the ordered reviewed IDs".to_owned(),
        );
    }

    validate_verdict_summary(report)?;

    for verdict in &report.invariants {
        let expected_clauses = catalog.clauses_for(&verdict.invariant_id);
        let expected_required_clauses = expected_clauses
            .iter()
            .filter(|clause| clause.required)
            .count();
        let expected_evidence = required
            .get(&verdict.invariant_id)
            .ok_or_else(|| format!("missing evidence contract for {}", verdict.invariant_id))?;
        if verdict.required_clauses != expected_required_clauses
            || verdict.required_evidence != expected_evidence.len()
            || verdict.clauses.len() != expected_clauses.len()
        {
            return Err(format!(
                "invariant verdict {} does not match its registry/profile contract",
                verdict.invariant_id
            ));
        }
        let passed_clauses = verdict
            .clauses
            .iter()
            .filter(|clause| clause.status == VerdictStatus::Green)
            .count();
        if verdict.required_clauses != verdict.clauses.len()
            || verdict.passed_clauses != passed_clauses
            || verdict.passed_evidence > verdict.required_evidence
            || verdict.issues.iter().any(issue_classification_mismatch)
        {
            return Err(format!(
                "invariant verdict {} has inconsistent counts",
                verdict.invariant_id
            ));
        }
        for (clause, expected_clause) in verdict.clauses.iter().zip(&expected_clauses) {
            let expected_clause_evidence = expected_evidence
                .iter()
                .filter(|evidence| evidence.clause_id == expected_clause.clause_id)
                .count();
            if clause.clause_id != expected_clause.clause_id
                || clause.statement != expected_clause.statement
                || clause.required_evidence != expected_clause_evidence
            {
                return Err(format!(
                    "clause verdict {} does not match its registry/profile contract",
                    clause.clause_id
                ));
            }
            let green_clause =
                clause.issues.is_empty() && clause.passed_evidence == clause.required_evidence;
            if clause.passed_evidence > clause.required_evidence
                || (clause.status == VerdictStatus::Green) != green_clause
                || clause.issues.iter().any(issue_classification_mismatch)
            {
                return Err(format!(
                    "clause verdict {} has inconsistent status or counts",
                    clause.clause_id
                ));
            }
        }
        let green_verdict = verdict.issues.is_empty()
            && verdict.passed_clauses == verdict.required_clauses
            && verdict.passed_evidence == verdict.required_evidence;
        if (verdict.status == VerdictStatus::Green) != green_verdict {
            return Err(format!(
                "invariant verdict {} has inconsistent status",
                verdict.invariant_id
            ));
        }
    }
    Ok(())
}

fn validate_verdict_summary(report: &crate::VerdictReport) -> Result<(), String> {
    let green = report
        .invariants
        .iter()
        .filter(|verdict| verdict.status == crate::VerdictStatus::Green)
        .count();
    let red = report.invariants.len().saturating_sub(green);
    if report.summary.total == report.invariants.len()
        && report.summary.green == green
        && report.summary.red == red
        && report.summary.green + report.summary.red == report.summary.total
    {
        Ok(())
    } else {
        Err("invariant verdict report summary does not match its rows".to_owned())
    }
}

fn issue_classification_mismatch(issue: &crate::types::VerdictIssue) -> bool {
    use crate::{EvidenceStatus, FailureClassification};

    !matches!(
        (issue.status, issue.classification),
        (
            EvidenceStatus::Fail,
            FailureClassification::InvariantViolation
        ) | (
            EvidenceStatus::Incomplete,
            FailureClassification::CoverageNotReached
        ) | (EvidenceStatus::Error, FailureClassification::HarnessError)
    )
}

fn validate(instance: &Value, source: &str, label: &str) -> Result<(), String> {
    let schema: Value = serde_json::from_str(source)
        .map_err(|error| format!("parse checked-in {label} schema: {error}"))?;
    let validator = jsonschema::validator_for(&schema)
        .map_err(|error| format!("compile checked-in {label} schema: {error}"))?;
    let errors = validator
        .iter_errors(instance)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{label} violates its checked-in schema: {}",
            errors.join("; ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        validate_result_bundle, validate_result_value, validate_verdict_report,
        validate_verdict_value,
    };

    #[test]
    fn rust_receipts_and_reports_conform_to_distinct_checked_in_schemas() {
        let (catalog, manifest) = crate::tests::loaded();
        let bundles = crate::tests::passing_bundles(&catalog, &manifest);
        for bundle in &bundles {
            validate_result_bundle(bundle).expect("synthetic bundle conforms");
        }
        let report = crate::aggregate(&catalog, &manifest, "pr", "abc", &bundles)
            .expect("synthetic report aggregates");
        validate_verdict_report(&report, &catalog, &manifest).expect("aggregate report conforms");
        assert_ne!(
            crate::types::RESULT_SCHEMA_VERSION,
            crate::types::VERDICT_SCHEMA_VERSION
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
        value["schema_version"] = serde_json::json!(crate::types::RESULT_SCHEMA_VERSION);
        value["execution"]["unreviewed"] = serde_json::json!(true);
        assert!(validate_result_value(&value).is_err());
    }

    #[test]
    fn verdict_validation_rejects_duplicate_ids_and_forged_counts() {
        let (catalog, manifest) = crate::tests::loaded();
        let bundles = crate::tests::passing_bundles(&catalog, &manifest);
        let report = crate::aggregate(&catalog, &manifest, "pr", "abc", &bundles)
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

        let mut mismatched_issue = crate::aggregate(&catalog, &manifest, "pr", "abc", &[])
            .expect("missing evidence aggregates red");
        mismatched_issue.invariants[0].issues[0].classification =
            crate::FailureClassification::InvariantViolation;
        assert!(validate_verdict_report(&mismatched_issue, &catalog, &manifest).is_err());
    }

    #[test]
    fn verdict_schema_can_represent_an_internally_red_zero_evidence_row() {
        let (catalog, manifest) = crate::tests::loaded();
        let mut report = crate::aggregate(&catalog, &manifest, "pr", "abc", &[])
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
}
