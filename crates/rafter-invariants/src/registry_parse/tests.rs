use std::{collections::BTreeSet, fs, path::PathBuf};

use super::parse_registry_document;
use super::registry_parse_test_fixtures::{
    VALID_ATOMIC_SIMULATOR_EVIDENCE, VALID_CLAUSE, VALID_EVIDENCE, VALID_INVARIANT,
};

#[test]
fn current_registry_parses_as_exactly_44_unique_invariants() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("verification/raft-invariants.yaml");
    let source = fs::read_to_string(path).expect("read current registry");

    let registry = parse_registry_document(&source).expect("parse current registry");
    assert_eq!(registry.schema_version, 3);
    assert_eq!(registry.invariants.len(), 44);
    assert_eq!(
        registry
            .invariants
            .iter()
            .map(|invariant| invariant.id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        44
    );
    assert!(!registry.clauses.is_empty());
    assert!(!registry.evidence.is_empty());
}

#[test]
fn unknown_and_malformed_registry_fields_fail_closed() {
    let valid = valid_registry(VALID_EVIDENCE, VALID_CLAUSE, VALID_INVARIANT);
    let cases = [
        (
            "unknown top-level field",
            valid.replace(
                "repository: \"zsumz/rafter\"",
                "unknown_metadata: \"ignored before\"\nrepository: \"zsumz/rafter\"",
            ),
        ),
        (
            "unknown count",
            valid.replace(
                "  total_entries: 1",
                "  unknown_count: 1\n  total_entries: 1",
            ),
        ),
        (
            "unknown invariant field",
            valid.replace(
                "    next_action: \"Keep testing.\"",
                "    unknown_future_field: \"ignored before\"\n    next_action: \"Keep testing.\"",
            ),
        ),
        (
            "unknown clause field",
            valid.replace(
                "    required: \"true\"",
                "    unknown_clause_field: \"ignored before\"\n    required: \"true\"",
            ),
        ),
        (
            "unknown evidence field",
            valid.replace(
                "    symbol: \"test_symbol\"",
                "    unknown_binding: \"ignored before\"\n    symbol: \"test_symbol\"",
            ),
        ),
        (
            "known field in the wrong layer",
            valid.replace(
                "    symbol: \"test_symbol\"",
                "    symbol: \"test_symbol\"\n    simulator_check: \"ignored before\"",
            ),
        ),
        (
            "malformed invariant field",
            valid.replace(
                "    statement: \"The statement holds.\"",
                "    statement \"The statement disappears.\"",
            ),
        ),
        (
            "unsupported indentation",
            valid.replace(
                "    statement: \"The statement holds.\"",
                "   statement: \"The statement disappears.\"",
            ),
        ),
        (
            "malformed quoted value",
            valid.replace(
                "    statement: \"The statement holds.\"",
                "    statement: \"The statement disappears.",
            ),
        ),
        (
            "unquoted value",
            valid.replace(
                "    statement: \"The statement holds.\"",
                "    statement: The statement disappears.",
            ),
        ),
        (
            "quoted count",
            valid.replace("  total_entries: 1", "  total_entries: \"1\""),
        ),
    ];

    for (case, source) in cases {
        assert!(
            parse_registry_document(&source).is_err(),
            "{case} was silently accepted"
        );
    }
}

#[test]
fn duplicate_fields_and_nested_fields_are_rejected() {
    let valid = valid_registry(VALID_EVIDENCE, VALID_CLAUSE, VALID_INVARIANT);
    let duplicate_statement = valid.replace(
        "    scope: \"Test scope.\"",
        "    statement: \"A replacement.\"\n    scope: \"Test scope.\"",
    );
    let duplicate_coverage = valid.replace(
        "      simulator: \"direct\"",
        "      tla: \"replacement\"\n      simulator: \"direct\"",
    );
    let duplicate_metadata = valid.replace(
        "repository: \"zsumz/rafter\"",
        "repository: \"replacement\"\nrepository: \"zsumz/rafter\"",
    );

    for source in [duplicate_statement, duplicate_coverage, duplicate_metadata] {
        let error = parse_registry_document(&source).expect_err("duplicate field must fail");
        assert!(error.to_string().contains("duplicate field"));
    }
}

#[test]
fn malformed_evidence_rows_cannot_hide_behind_the_invariant_count() {
    parse_registry_document(&valid_registry(
        VALID_EVIDENCE,
        VALID_CLAUSE,
        VALID_INVARIANT,
    ))
    .expect("control evidence parses");
    let cases = [
        VALID_EVIDENCE.replace(
            "    symbol: \"test_symbol\"",
            "    unsupported_binding: \"ignored before\"\n    symbol: \"test_symbol\"",
        ),
        format!("{VALID_EVIDENCE}  - layer: \"tests\"\n"),
        VALID_EVIDENCE.replace(
            "    symbol: \"test_symbol\"",
            "    path: \"replacement.rs\"\n    symbol: \"test_symbol\"",
        ),
    ];

    for source in cases {
        let registry = valid_registry(&source, VALID_CLAUSE, VALID_INVARIANT);
        assert!(
            parse_registry_document(&registry).is_err(),
            "malformed evidence was silently accepted"
        );
    }
}

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
fn non_direct_non_simulator_evidence_may_retain_a_fixture_exemption() {
    let evidence = VALID_EVIDENCE
        .replace("    strength: \"direct\"", "    strength: \"e2e\"")
        .replace(
            "    symbol: \"test_symbol\"",
            "    symbol: \"test_symbol\"\n    negative_fixture_exemption: \"reviewed external boundary\"",
        );
    parse_registry_document(&valid_registry(&evidence, VALID_CLAUSE, VALID_INVARIANT))
        .expect("non-direct tests evidence exemption should remain valid");
}

#[test]
fn direct_test_evidence_binds_exactly_one_clause_without_an_atomic_escape_hatch() {
    parse_registry_document(&valid_registry(
        VALID_EVIDENCE,
        VALID_CLAUSE,
        VALID_INVARIANT,
    ))
    .expect("single-clause direct test evidence parses");

    let multi_clause = VALID_EVIDENCE.replace(
        "    clauses: \"AA-01.a\"",
        "    clauses: \"AA-01.a,AA-01.b\"",
    );
    let ceremonial_atomic_group = multi_clause.replace(
        "    layer: \"tests\"",
        "    atomic_group: \"AA-01/ceremonial\"\n    layer: \"tests\"",
    );
    for evidence in [multi_clause, ceremonial_atomic_group] {
        let error =
            parse_registry_document(&valid_registry(&evidence, VALID_CLAUSE, VALID_INVARIANT))
                .expect_err("multi-clause direct test evidence must fail");
        assert_eq!(
            error.to_string(),
            "direct tests evidence record 1 must bind exactly one clause, found 2"
        );
    }
}

#[test]
fn duplicate_invariant_and_clause_ids_are_rejected() {
    let duplicate_invariant = format!(
        "{VALID_INVARIANT}{}",
        VALID_INVARIANT.trim_start_matches("invariants:\n")
    );
    let duplicate_clause = format!(
        "{VALID_CLAUSE}{}",
        VALID_CLAUSE.trim_start_matches("clauses:\n")
    );

    assert!(parse_registry_document(&valid_registry(
        VALID_EVIDENCE,
        VALID_CLAUSE,
        &duplicate_invariant,
    ))
    .expect_err("duplicate invariant ID must fail")
    .to_string()
    .contains("duplicate invariant ID"));
    assert!(parse_registry_document(&valid_registry(
        VALID_EVIDENCE,
        &duplicate_clause,
        VALID_INVARIANT,
    ))
    .expect_err("duplicate clause ID must fail")
    .to_string()
    .contains("duplicate clause ID"));
}

#[test]
fn duplicate_or_malformed_schema_version_is_rejected() {
    let valid = valid_registry(VALID_EVIDENCE, VALID_CLAUSE, VALID_INVARIANT);
    for source in [
        valid.replace("schema_version: 3", "schema_version: 2"),
        valid.replace("schema_version: 3", "schema_version:3"),
        valid.replace("schema_version: 3", "schema_version: \"3\""),
        valid.replace("schema_version: 3", "schema_version: 3\nschema_version: 3"),
    ] {
        assert!(parse_registry_document(&source).is_err());
    }
}

fn valid_registry(evidence: &str, clauses: &str, invariants: &str) -> String {
    format!(
        r#"schema_version: 3
repository: "zsumz/rafter"
catalog_origin_ref: "test-origin"
catalog_working_ref: "test-work"
audit_date: "2026-07-13"
document: "docs/raft-invariants.md"
scope: "Test scope."
counting:
  canonical_raft_safety_properties: 1
  tla_predicates_now: 1
  well_formedness_meta_invariants: 0
  semantic_safety_invariants: 1
  liveness_obligations: 0
  total_entries: 1
{evidence}{clauses}{invariants}"#
    )
}
