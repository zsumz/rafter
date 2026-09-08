//! Reading links preserve failure identity and resolve to real production transitions.

use rafter_sim::model_check::FailureKind;

use super::{failure::failure_timeline_lines, rules::rule_guide};

#[test]
fn rule_references_resolve_without_inventing_a_checker_or_a_minimized_trace() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let document = std::fs::read_to_string(root.join("docs/protocol-rules.md")).unwrap();
    for label in [
        "EL-01 term monotonicity",
        "EL-03 safe vote eligibility",
        "EL-07 term and authority fencing",
        "CM-02 commit requires effective quorum",
        "CM-03 leaders only commit current-term entries",
    ] {
        let guide = rule_guide(label).expect("reviewed failure has a reading path");
        let source = std::fs::read_to_string(root.join(guide.source)).unwrap();
        assert!(source.contains(&format!("fn {}(", guide.transition)));
        assert!(document.contains(&format!("../{}", guide.source)));
        assert!(document
            .lines()
            .filter_map(|line| line.strip_prefix("## "))
            .any(|heading| heading.to_lowercase().replace(' ', "-") == guide.guide_anchor));
        let lines = failure_timeline_lines(
            "fixture",
            FailureKind::InvariantViolation,
            label,
            "original checker detail",
            [(7, "exact action".to_string())],
        );
        assert!(lines[0].contains("original checker detail"));
        assert!(lines.iter().any(|line| line.contains("reduction=none")));
        assert_eq!(
            lines.last().unwrap(),
            "DEBUG test trace step step=7 action=\"exact action\""
        );
    }
    // A catalog clause without a simulator-owned label must not become a
    // guessed classification just to attach a helpful-looking source link.
    assert!(rule_guide("RD-02 fresh quorum confirmation").is_none());
    assert!(rule_guide("CM-03 renamed condition").is_none());
}
