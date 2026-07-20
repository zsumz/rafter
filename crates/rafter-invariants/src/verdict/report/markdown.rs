//! Exact 44-row Markdown rendering for CI summaries.

use std::fmt::Write;

use super::super::{InvariantVerdict, VerdictReport, VerdictStatus};

#[must_use]
pub(crate) fn render_markdown(report: &VerdictReport) -> String {
    let mut output = format!(
        "# Rafter invariant report: {}\n\nSource: `{}`\n\nVerdict: **{}/{} green**\n\n| Invariant | Verdict | Clauses | Evidence | Detail |\n| --- | --- | ---: | ---: | --- |\n",
        report.profile, report.source_ref, report.summary.green, report.summary.total
    );
    for verdict in &report.invariants {
        let detail = verdict.issues.first().map_or_else(
            || "all required clauses and evidence passed".to_owned(),
            |issue| issue.message.clone(),
        );
        let _ = writeln!(
            output,
            "| `{}` | {} | {}/{} | {}/{} | {} |",
            verdict.invariant_id,
            verdict_label(verdict),
            verdict.passed_clauses,
            verdict.required_clauses,
            verdict.passed_evidence,
            verdict.required_evidence,
            markdown_cell(&detail)
        );
    }
    output
}

fn verdict_label(verdict: &InvariantVerdict) -> &'static str {
    match verdict.status {
        VerdictStatus::Green => "GREEN",
        VerdictStatus::Red => "RED",
    }
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}
