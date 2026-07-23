//! Terminal projection of gate-owned verdict outcomes.

use crate::verdict::{VerdictReport, VerdictStatus};

pub(super) fn report_lines(report: &VerdictReport) -> Vec<String> {
    let mut lines = report
        .invariants
        .iter()
        .map(|result| {
            let label = match result.status {
                VerdictStatus::Green => "GREEN",
                VerdictStatus::Red => "RED",
            };
            format!(
                "{label} {} {}/{} clauses, {}/{} evidence checks",
                result.invariant_id,
                result.passed_clauses,
                result.required_clauses,
                result.passed_evidence,
                result.required_evidence
            )
        })
        .collect::<Vec<_>>();
    lines.push(format!(
        "invariant verdict: {}/{} green ({})",
        report.summary.green, report.summary.total, report.profile
    ));
    lines
}
