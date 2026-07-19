//! Shape and registry/profile semantic validation for verdict reports.

use serde_json::Value;

use super::{InvariantVerdict, VerdictIssue, VerdictReport, VerdictStatus};
use crate::{
    contract::{
        catalog::Catalog,
        profile::ProfileManifest,
        schema::{validate, VERDICT_SCHEMA},
    },
    evidence::{EvidenceStatus, FailureClassification},
};

pub(crate) fn validate_verdict_report(
    report: &VerdictReport,
    catalog: &Catalog,
    manifest: &ProfileManifest,
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
    report: &VerdictReport,
    catalog: &Catalog,
    manifest: &ProfileManifest,
) -> Result<(), String> {
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
        validate_invariant_counts(verdict, expected_evidence.len())?;
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

fn validate_invariant_counts(
    verdict: &InvariantVerdict,
    expected_evidence: usize,
) -> Result<(), String> {
    let passed_clauses = verdict
        .clauses
        .iter()
        .filter(|clause| clause.status == VerdictStatus::Green)
        .count();
    if verdict.required_clauses == verdict.clauses.len()
        && verdict.passed_clauses == passed_clauses
        && verdict.required_evidence == expected_evidence
        && verdict.passed_evidence <= verdict.required_evidence
        && !verdict.issues.iter().any(issue_classification_mismatch)
    {
        Ok(())
    } else {
        Err(format!(
            "invariant verdict {} has inconsistent counts",
            verdict.invariant_id
        ))
    }
}

fn validate_verdict_summary(report: &VerdictReport) -> Result<(), String> {
    let green = report
        .invariants
        .iter()
        .filter(|verdict| verdict.status == VerdictStatus::Green)
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

fn issue_classification_mismatch(issue: &VerdictIssue) -> bool {
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
