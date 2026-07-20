//! Scenarios for stable report coverage and escaping.

use super::{render_junit, render_markdown};

const JSON_WIRE_V2: &str = include_str!("fixtures/red-report-v2.json");
const JUNIT_WIRE_V2: &str = include_str!("fixtures/red-report-v2.xml");
const MARKDOWN_WIRE_V2: &str = include_str!("fixtures/red-report-v2.md");

#[test]
fn report_wire_formats_are_byte_stable_and_escape_independently() {
    let report: crate::VerdictReport =
        serde_json::from_str(JSON_WIRE_V2).expect("decode golden verdict report");
    assert_eq!(
        format!(
            "{}\n",
            serde_json::to_string_pretty(&report).expect("encode golden verdict report")
        ),
        JSON_WIRE_V2
    );
    assert_eq!(render_junit(&report), JUNIT_WIRE_V2);
    assert_eq!(render_markdown(&report), MARKDOWN_WIRE_V2);
}

#[test]
fn renderers_emit_every_reviewed_invariant() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundles = crate::tests::passing_bundles(&catalog, &manifest);
    let report = crate::tests::aggregate_unverified(&catalog, &manifest, "pr", "abc", &bundles)
        .expect("aggregate passing report");
    let markdown = render_markdown(&report);
    let junit = render_junit(&report);
    for invariant in &catalog.invariants {
        assert!(markdown.contains(&invariant.id));
        assert!(junit.contains(&invariant.id));
    }
}

#[test]
fn junit_replaces_xml_forbidden_characters_and_remains_parseable() {
    let mut report: crate::VerdictReport =
        serde_json::from_str(JSON_WIRE_V2).expect("decode golden verdict report");
    report.invariants[0].issues[0]
        .message
        .push_str("\0\u{8}\u{b}\u{c}\u{1f}\u{fffe}\u{ffff}");

    let junit = render_junit(&report);
    assert!(!junit.chars().any(|character| matches!(
        character,
        '\0'..='\u{8}' | '\u{b}' | '\u{c}' | '\u{e}'..='\u{1f}' | '\u{fffe}' | '\u{ffff}'
    )));
    assert_eq!(junit.matches('\u{fffd}').count(), 7);

    let mut reader = quick_xml::Reader::from_str(&junit);
    loop {
        if reader.read_event().expect("parse generated JUnit XML") == quick_xml::events::Event::Eof
        {
            break;
        }
    }
}
