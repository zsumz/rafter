//! Shared registry source construction for parser scenarios.

pub(super) fn valid_registry(evidence: &str, clauses: &str, invariants: &str) -> String {
    format!(
        r#"schema_version: 4
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
