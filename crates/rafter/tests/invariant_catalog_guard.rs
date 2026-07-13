use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[path = "invariant_catalog_guard/assertions.rs"]
mod assertions;
#[path = "invariant_catalog_guard/doc_checks.rs"]
mod doc_checks;
#[path = "invariant_catalog_guard/evidence.rs"]
mod evidence_checks;
#[path = "invariant_catalog_guard/parse.rs"]
mod parse;

use assertions::{
    assert_clauses_are_well_formed, assert_declared_counts_match, assert_entries_are_well_formed,
};
use doc_checks::{
    assert_generated_doc_mentions_every_entry, assert_model_check_catalog_labels_are_registered,
    assert_rendered_doc_is_current,
};
use evidence_checks::assert_evidence_is_machine_checkable;
use parse::{parse_clauses, parse_entries, parse_evidence};

const EXPECTED_TOTAL: usize = 44;
const EXPECTED_CANONICAL: usize = 5;
const EXPECTED_TLA_PREDICATES: usize = 9;
const EXPECTED_WELL_FORMEDNESS: usize = 1;
const EXPECTED_SAFETY: usize = 40;
const EXPECTED_LIVENESS: usize = 3;
const COVERAGE_LAYERS: &[&str] = &["tla", "simulator", "tests", "maelstrom"];
const VALID_FAMILIES: &[&str] = &[
    "state",
    "election",
    "log",
    "commit",
    "membership",
    "read",
    "persistence",
    "liveness",
];
const VALID_KINDS: &[&str] = &["well_formedness", "safety", "liveness"];
const VALID_TIERS: &[&str] = &[
    "meta",
    "canonical",
    "feature",
    "durable",
    "client",
    "progress",
];
const VALID_EVIDENCE_STRENGTHS: &[&str] = &["direct", "e2e"];
const ID_PREFIX_TO_KIND: &[(&str, &str)] = &[
    ("ST", "well_formedness"),
    ("EL", "safety"),
    ("LG", "safety"),
    ("CM", "safety"),
    ("AP", "safety"),
    ("MB", "safety"),
    ("RD", "safety"),
    ("PS", "safety"),
    ("SS", "safety"),
    ("LV", "liveness"),
];

#[derive(Debug, Default)]
struct Entry {
    id: String,
    kind: String,
    family: String,
    tier: String,
    title: String,
    statement: String,
    scope: String,
    assumptions: String,
    action_class: String,
    next_action: String,
    priority: String,
    current_coverage: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
struct Clause {
    id: String,
    invariant_id: String,
    statement: String,
    scope: String,
    assumptions: String,
    required: bool,
}

#[derive(Debug, Default)]
struct Evidence {
    id: String,
    clauses: Vec<String>,
    layer: String,
    strength: String,
    path: String,
    symbol: String,
    atomic_group: Option<String>,
    negative_fixture: Option<String>,
    negative_fixture_path: Option<String>,
    negative_fixture_detector: Option<String>,
    negative_fixture_exemption: Option<String>,
}

#[test]
fn invariant_catalog_is_complete_and_documented() {
    let workspace = workspace_root();
    let registry_path = workspace.join("verification/raft-invariants.yaml");
    let doc_path = workspace.join("docs/raft-invariants.md");
    let registry = std::fs::read_to_string(&registry_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", registry_path.display()));
    let doc = std::fs::read_to_string(&doc_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", doc_path.display()));

    let entries = parse_entries(&registry);
    let clauses = parse_clauses(&registry);
    let evidence = parse_evidence(&registry);
    assert_eq!(entries.len(), EXPECTED_TOTAL, "unexpected catalog size");
    assert_declared_counts_match(&registry, &entries, &workspace);
    assert_entries_are_well_formed(&entries);
    assert_clauses_are_well_formed(&entries, &clauses);
    assert_evidence_is_machine_checkable(&workspace, &entries, &clauses, &evidence);
    assert_rendered_doc_is_current(&workspace);
    assert_generated_doc_mentions_every_entry(&doc, &entries);
    assert_model_check_catalog_labels_are_registered(&workspace, &entries);
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
}
