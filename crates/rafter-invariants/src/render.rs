use std::fmt::Write;

use crate::{InvariantVerdict, VerdictReport, VerdictStatus};

/// Renders the exact 44-row human-readable report used in CI summaries.
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

/// Renders one `JUnit` testcase per reviewed invariant.
#[must_use]
pub(crate) fn render_junit(report: &VerdictReport) -> String {
    let mut output = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuite name=\"rafter-invariants-{}\" tests=\"{}\" failures=\"{}\">\n",
        xml(&report.profile),
        report.summary.total,
        report.summary.red
    );
    for verdict in &report.invariants {
        let _ = writeln!(
            output,
            "  <testcase classname=\"rafter.invariants\" name=\"{}\">",
            xml(&verdict.invariant_id)
        );
        if verdict.status == VerdictStatus::Red {
            let detail = verdict
                .issues
                .iter()
                .map(|issue| format!("{}: {}", issue.evidence_id, issue.message))
                .collect::<Vec<_>>()
                .join("\n");
            let _ = writeln!(
                output,
                "    <failure message=\"invariant evidence is red\">{}</failure>",
                xml(&detail)
            );
        }
        output.push_str("  </testcase>\n");
    }
    output.push_str("</testsuite>\n");
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

fn xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
