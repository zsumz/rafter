//! Exact one-testcase-per-invariant `JUnit` rendering.

use std::fmt::Write;

use super::super::{VerdictReport, VerdictStatus};

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

fn xml(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            character if xml_10_character(character) => escaped.push(character),
            _ => escaped.push('\u{fffd}'),
        }
    }
    escaped
}

const fn xml_10_character(character: char) -> bool {
    matches!(
        character,
        '\u{9}' | '\u{a}' | '\u{d}' | '\u{20}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}'
    )
}
