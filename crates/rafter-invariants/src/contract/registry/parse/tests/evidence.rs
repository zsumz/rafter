//! Evidence-row shape and executable test-binding scenarios.

use super::super::fixtures::{VALID_CLAUSE, VALID_EVIDENCE, VALID_INVARIANT};
use super::super::parse_registry_document;
use super::support::valid_registry;

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
fn direct_test_identity_is_exact_and_uses_a_supported_cargo_target() {
    parse_registry_document(&valid_registry(
        VALID_EVIDENCE,
        VALID_CLAUSE,
        VALID_INVARIANT,
    ))
    .expect("control direct-test identity parses");

    for evidence in [
        VALID_EVIDENCE.replace("    target_kind: \"lib\"", "    target_kind: \"bench\""),
        VALID_EVIDENCE.replace(
            "    test_name: \"tests::test_symbol\"",
            "    test_name: \"tests::different_symbol\"",
        ),
        VALID_EVIDENCE.replace(
            "    test_name: \"tests::test_symbol\"",
            "    test_name: \"tests::::test_symbol\"",
        ),
    ] {
        assert!(
            parse_registry_document(&valid_registry(&evidence, VALID_CLAUSE, VALID_INVARIANT,))
                .is_err(),
            "malformed direct-test identity was accepted"
        );
    }
}

#[test]
fn persistence_evidence_uses_a_closed_typed_vocabulary() {
    for kind in ["crash_reopen", "failure_injection"] {
        let evidence = VALID_EVIDENCE.replace(
            "    symbol: \"test_symbol\"",
            &format!("    symbol: \"test_symbol\"\n    persistence_evidence: \"{kind}\""),
        );
        parse_registry_document(&valid_registry(&evidence, VALID_CLAUSE, VALID_INVARIANT))
            .expect("supported persistence evidence kind parses");
    }

    let evidence = VALID_EVIDENCE.replace(
        "    symbol: \"test_symbol\"",
        "    symbol: \"test_symbol\"\n    persistence_evidence: \"restartish\"",
    );
    let error = parse_registry_document(&valid_registry(&evidence, VALID_CLAUSE, VALID_INVARIANT))
        .expect_err("unknown persistence evidence kind must fail closed");
    assert!(error
        .to_string()
        .contains("unsupported persistence_evidence restartish"));
}
