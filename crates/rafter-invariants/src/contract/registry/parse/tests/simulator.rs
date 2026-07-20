//! Direct simulator evidence and detector-fixture scenarios.

use super::super::fixtures::{VALID_ATOMIC_SIMULATOR_EVIDENCE, VALID_CLAUSE, VALID_INVARIANT};
use super::super::parse_registry_document;
use super::support::valid_registry;

#[test]
fn multi_clause_simulator_evidence_requires_a_qualified_atomic_group() {
    let parsed = parse_registry_document(&valid_registry(
        VALID_ATOMIC_SIMULATOR_EVIDENCE,
        VALID_CLAUSE,
        VALID_INVARIANT,
    ))
    .expect("qualified atomic group parses");
    assert_eq!(parsed.evidence[0].clauses, ["CM-03.a", "CM-03.b"]);
    assert_eq!(
        parsed.evidence[0].atomic_group.as_deref(),
        Some("CM-03/current-term-commit-point")
    );

    let cases = [
        VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
            "    atomic_group: \"CM-03/current-term-commit-point\"\n",
            "",
        ),
        VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
            "    clauses: \"CM-03.a,CM-03.b\"",
            "    clauses: \"CM-03.a\"",
        ),
        VALID_ATOMIC_SIMULATOR_EVIDENCE
            .replace("    negative_fixture_detector: \"check_atomic_rule\"\n", ""),
        VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
            "    atomic_group: \"CM-03/current-term-commit-point\"",
            "    atomic_group: \"other/atomic-rule\"",
        ),
        VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
            "    atomic_group: \"CM-03/current-term-commit-point\"",
            "    atomic_group: \"CM-03/unreviewed-rule\"",
        ),
    ];
    for source in cases {
        let registry = valid_registry(&source, VALID_CLAUSE, VALID_INVARIANT);
        assert!(
            parse_registry_document(&registry).is_err(),
            "invalid atomic group was accepted"
        );
    }
}

#[test]
fn direct_simulator_evidence_requires_an_executable_detector_fixture() {
    let fixture_block = r#"    negative_fixture: "atomic_rule_rejects_mutation"
    negative_fixture_path: "src/model/tests.rs"
    negative_fixture_detector: "check_atomic_rule"
    negative_fixture_package: "test-package"
    negative_fixture_target_kind: "lib"
    negative_fixture_target: "test_package"
    negative_fixture_test_name: "tests::atomic_rule_rejects_mutation"
"#;
    let exempt = VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
        fixture_block,
        "    negative_fixture_exemption: \"reviewed exception\"\n",
    );
    let error = parse_registry_document(&valid_registry(&exempt, VALID_CLAUSE, VALID_INVARIANT))
        .expect_err("direct simulator exemption must fail");
    assert_eq!(
        error.to_string(),
        "direct simulator evidence record 1 may not use negative_fixture_exemption"
    );

    let missing_fixture = VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(fixture_block, "");
    let error = parse_registry_document(&valid_registry(
        &missing_fixture,
        VALID_CLAUSE,
        VALID_INVARIANT,
    ))
    .expect_err("direct simulator evidence without a fixture must fail");
    assert_eq!(
        error.to_string(),
        "direct simulator evidence record 1 lacks detector qualification"
    );

    let missing_test_name = VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
        "    negative_fixture_test_name: \"tests::atomic_rule_rejects_mutation\"\n",
        "",
    );
    let error = parse_registry_document(&valid_registry(
        &missing_test_name,
        VALID_CLAUSE,
        VALID_INVARIANT,
    ))
    .expect_err("direct simulator fixture without executable identity must fail");
    assert_eq!(
        error.to_string(),
        "evidence record 1 is missing required field negative_fixture_test_name"
    );
}

#[test]
fn simulator_negative_fixture_identity_is_validated_when_the_registry_loads() {
    for (source, expected) in [
        (
            VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
                "    negative_fixture_target_kind: \"lib\"",
                "    negative_fixture_target_kind: \"proc-macro\"",
            ),
            "unsupported Cargo target kind",
        ),
        (
            VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
                "tests::atomic_rule_rejects_mutation",
                "tests::another_fixture",
            ),
            "exact test-name leaf",
        ),
        (
            VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
                "tests::atomic_rule_rejects_mutation",
                "tests::::atomic_rule_rejects_mutation",
            ),
            "malformed test identity",
        ),
    ] {
        let error =
            parse_registry_document(&valid_registry(&source, VALID_CLAUSE, VALID_INVARIANT))
                .expect_err("malformed simulator test identity must fail at registry load")
                .to_string();
        assert!(error.contains(expected), "unexpected error: {error}");
    }
}

#[test]
fn detector_path_is_a_canonical_repository_relative_path() {
    let valid = with_detector_path("src/model/checks.rs");
    let registry = parse_registry_document(&valid_registry(&valid, VALID_CLAUSE, VALID_INVARIANT))
        .expect("canonical detector path parses");
    assert_eq!(
        registry.evidence[0]
            .negative_fixture_detector_path
            .as_deref(),
        Some("src/model/checks.rs")
    );

    for path in [
        "",
        "   ",
        "/src/checks.rs",
        "C:/src/checks.rs",
        ".",
        "./src/checks.rs",
        "src/./checks.rs",
        "../src/checks.rs",
        "src/../checks.rs",
        "src//checks.rs",
        "src/checks.rs/",
        "src\\\\checks.rs",
    ] {
        let source = with_detector_path(path);
        let error =
            parse_registry_document(&valid_registry(&source, VALID_CLAUSE, VALID_INVARIANT))
                .expect_err("non-canonical detector path must fail closed");
        assert!(
            error
                .to_string()
                .contains("non-canonical repository-relative"),
            "unexpected error for {path:?}: {error}"
        );
    }
}

#[test]
fn detector_path_is_rejected_outside_its_direct_simulator_fixture_binding() {
    let detector_path = "    negative_fixture_detector_path: \"src/model/checks.rs\"\n";
    let without_detector = VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
        "    negative_fixture_detector: \"check_atomic_rule\"\n",
        detector_path,
    );
    let test_evidence = super::super::fixtures::VALID_EVIDENCE.replace(
        "    symbol: \"test_symbol\"",
        &format!("    symbol: \"test_symbol\"\n{detector_path}"),
    );

    for evidence in [without_detector, test_evidence] {
        let error =
            parse_registry_document(&valid_registry(&evidence, VALID_CLAUSE, VALID_INVARIANT))
                .expect_err("misplaced detector path must fail closed");
        assert!(error
            .to_string()
            .contains("misplaced negative_fixture_detector_path"));
    }
}

fn with_detector_path(path: &str) -> String {
    VALID_ATOMIC_SIMULATOR_EVIDENCE.replace(
        "    negative_fixture_detector: \"check_atomic_rule\"",
        &format!(
            "    negative_fixture_detector: \"check_atomic_rule\"\n    negative_fixture_detector_path: \"{path}\""
        ),
    )
}
