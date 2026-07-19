//! Registry document shape, identity, and schema-version scenarios.

use std::{collections::BTreeSet, fs, path::PathBuf};

use super::super::fixtures::{VALID_CLAUSE, VALID_EVIDENCE, VALID_INVARIANT};
use super::super::parse_registry_document;
use super::support::valid_registry;

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
