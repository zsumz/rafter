use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

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
    required_action: String,
    priority: String,
    current_coverage: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
struct Evidence {
    id: String,
    layer: String,
    strength: String,
    path: String,
    symbol: String,
    negative_fixture: Option<String>,
}

#[test]
fn invariant_catalog_is_complete_and_documented() {
    let workspace = workspace_root();
    let registry_path = workspace.join("verification/raft-invariants.yaml");
    let doc_path = workspace.join("docs/raft-invariants.md");
    let registry = fs::read_to_string(&registry_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", registry_path.display()));
    let doc = fs::read_to_string(&doc_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", doc_path.display()));

    let entries = parse_entries(&registry);
    let evidence = parse_evidence(&registry);
    assert_eq!(entries.len(), EXPECTED_TOTAL, "unexpected catalog size");
    assert_declared_counts_match(&registry, &entries, &workspace);
    assert_entries_are_well_formed(&entries);
    assert_evidence_is_machine_checkable(&workspace, &entries, &evidence);
    assert_rendered_doc_is_current(&workspace);
    assert_generated_doc_mentions_every_entry(&doc, &entries);
    assert_model_check_catalog_labels_are_registered(&workspace, &entries);
}

fn assert_declared_counts_match(registry: &str, entries: &[Entry], workspace: &Path) {
    assert_eq!(
        declared_count(registry, "total_entries"),
        EXPECTED_TOTAL,
        "registry total_entries must match the reviewed catalog size",
    );
    assert_eq!(
        declared_count(registry, "canonical_raft_safety_properties"),
        EXPECTED_CANONICAL,
        "registry canonical_raft_safety_properties count drifted",
    );
    assert_eq!(
        declared_count(registry, "tla_predicates_now"),
        EXPECTED_TLA_PREDICATES,
        "registry tla_predicates_now count drifted",
    );
    assert_eq!(
        declared_count(registry, "well_formedness_meta_invariants"),
        EXPECTED_WELL_FORMEDNESS,
        "registry well-formedness count drifted",
    );
    assert_eq!(
        declared_count(registry, "semantic_safety_invariants"),
        EXPECTED_SAFETY,
        "registry safety count drifted",
    );
    assert_eq!(
        declared_count(registry, "liveness_obligations"),
        EXPECTED_LIVENESS,
        "registry liveness count drifted",
    );

    let mut actual = BTreeMap::<&str, usize>::new();
    for entry in entries {
        *actual.entry(entry.kind.as_str()).or_default() += 1;
    }
    assert_eq!(
        actual.get("well_formedness").copied().unwrap_or_default(),
        EXPECTED_WELL_FORMEDNESS,
        "well-formedness entries do not match declared count",
    );
    assert_eq!(
        actual.get("safety").copied().unwrap_or_default(),
        EXPECTED_SAFETY,
        "safety entries do not match declared count",
    );
    assert_eq!(
        actual.get("liveness").copied().unwrap_or_default(),
        EXPECTED_LIVENESS,
        "liveness entries do not match declared count",
    );

    let canonical_entries = entries
        .iter()
        .filter(|entry| entry.tier == "canonical")
        .count();
    assert_eq!(
        canonical_entries, EXPECTED_CANONICAL,
        "canonical-tier entries do not match declared count",
    );
    assert_tla_invariant_counts_match(workspace, EXPECTED_TLA_PREDICATES);
}

fn assert_entries_are_well_formed(entries: &[Entry]) {
    let mut ids = BTreeSet::new();
    for entry in entries {
        assert!(ids.insert(entry.id.as_str()), "{} is duplicated", entry.id);
        assert_valid_id(entry);
        assert!(
            VALID_KINDS.contains(&entry.kind.as_str()),
            "{} has unknown kind {}",
            entry.id,
            entry.kind,
        );
        assert!(
            VALID_FAMILIES.contains(&entry.family.as_str()),
            "{} has unknown family {}",
            entry.id,
            entry.family,
        );
        assert!(
            VALID_TIERS.contains(&entry.tier.as_str()),
            "{} has unknown tier {}",
            entry.id,
            entry.tier,
        );
        assert!(
            !entry.title.trim().is_empty()
                && !entry.statement.trim().is_empty()
                && !entry.required_action.trim().is_empty(),
            "{} must have title, statement, and required_action",
            entry.id,
        );
        assert!(
            matches!(entry.priority.as_str(), "p0" | "p1" | "p2"),
            "{} has invalid priority {}",
            entry.id,
            entry.priority,
        );
        for layer in COVERAGE_LAYERS {
            assert!(
                entry
                    .current_coverage
                    .get(*layer)
                    .is_some_and(|value| !value.trim().is_empty()),
                "{} must declare current_coverage.{}",
                entry.id,
                layer,
            );
        }
        if entry.kind == "safety" {
            assert!(
                entry
                    .current_coverage
                    .values()
                    .any(|value| !value.starts_with("none")),
                "{} safety invariant must name current evidence or an explicit gap",
                entry.id,
            );
            if !entry
                .current_coverage
                .values()
                .any(|value| value.starts_with('D') || value.starts_with("E2E"))
            {
                assert_eq!(
                    entry.priority, "p0",
                    "{} safety invariant lacks direct evidence and must be first-priority work",
                    entry.id,
                );
            }
        }
        if entry.kind == "liveness" {
            assert!(
                entry.id.starts_with("LV-") && entry.family == "liveness",
                "{} liveness obligations must use LV-* IDs and liveness family",
                entry.id,
            );
        }
    }
}

fn assert_evidence_is_machine_checkable(
    workspace: &Path,
    entries: &[Entry],
    evidence: &[Evidence],
) {
    assert!(
        !evidence.is_empty(),
        "registry must declare machine-checkable evidence records",
    );

    let ids = entries
        .iter()
        .map(|entry| entry.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen_records = BTreeSet::new();

    for record in evidence {
        assert!(
            ids.contains(record.id.as_str()),
            "evidence record references unknown invariant ID {}",
            record.id,
        );
        assert!(
            COVERAGE_LAYERS.contains(&record.layer.as_str()),
            "{} evidence has unknown layer {}",
            record.id,
            record.layer,
        );
        assert!(
            VALID_EVIDENCE_STRENGTHS.contains(&record.strength.as_str()),
            "{} evidence has unknown strength {}",
            record.id,
            record.strength,
        );
        assert!(
            seen_records.insert((
                record.id.as_str(),
                record.layer.as_str(),
                record.strength.as_str(),
                record.path.as_str(),
                record.symbol.as_str(),
            )),
            "{} {} {} evidence record for {}#{} is duplicated",
            record.id,
            record.layer,
            record.strength,
            record.path,
            record.symbol,
        );
        assert!(
            !record.path.trim().is_empty() && !record.symbol.trim().is_empty(),
            "{} {} {} evidence must name path and symbol",
            record.id,
            record.layer,
            record.strength,
        );
        let path = workspace.join(&record.path);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read evidence path {}: {error}", path.display()));
        assert!(
            source.contains(&record.symbol),
            "{} {} {} evidence symbol `{}` was not found in {}",
            record.id,
            record.layer,
            record.strength,
            record.symbol,
            record.path,
        );
        if let Some(negative_fixture) = &record.negative_fixture {
            assert!(
                source.contains(negative_fixture),
                "{} {} {} negative fixture `{}` was not found in {}",
                record.id,
                record.layer,
                record.strength,
                negative_fixture,
                record.path,
            );
        }
    }

    for entry in entries {
        for layer in COVERAGE_LAYERS {
            let coverage = entry
                .current_coverage
                .get(*layer)
                .unwrap_or_else(|| panic!("{} missing current_coverage.{layer}", entry.id));
            let Some(strength) = evidence_strength_for_coverage(coverage) else {
                continue;
            };
            assert!(
                evidence.iter().any(|record| {
                    record.id == entry.id && record.layer == *layer && record.strength == strength
                }),
                "{} current_coverage.{} declares {} evidence but has no machine-checkable evidence record",
                entry.id,
                layer,
                coverage,
            );
        }
    }
}

fn evidence_strength_for_coverage(coverage: &str) -> Option<&'static str> {
    let coverage = coverage.trim();
    if coverage == "D" || coverage.starts_with("D:") {
        Some("direct")
    } else if coverage == "E2E" || coverage.starts_with("E2E:") {
        Some("e2e")
    } else {
        None
    }
}

fn assert_valid_id(entry: &Entry) {
    let Some((prefix, number)) = entry.id.split_once('-') else {
        panic!("{} must use PREFIX-NN form", entry.id);
    };
    assert_eq!(
        number.len(),
        2,
        "{} must use a two-digit numeric suffix",
        entry.id,
    );
    assert!(
        number.chars().all(|character| character.is_ascii_digit()),
        "{} suffix must be numeric",
        entry.id,
    );
    let expected_kind = ID_PREFIX_TO_KIND
        .iter()
        .find_map(|(candidate, kind)| (*candidate == prefix).then_some(*kind))
        .unwrap_or_else(|| panic!("{} uses unknown ID prefix {}", entry.id, prefix));
    assert_eq!(
        entry.kind, expected_kind,
        "{} prefix {} does not match kind {}",
        entry.id, prefix, entry.kind,
    );
}

fn assert_generated_doc_mentions_every_entry(doc: &str, entries: &[Entry]) {
    assert!(
        doc.contains("Generated from `verification/raft-invariants.yaml`"),
        "docs/raft-invariants.md must identify its registry source",
    );
    let list = section(doc, "## List", "## First Closures");
    for entry in entries {
        assert!(
            doc.contains(&format!("`{}`", entry.id)),
            "docs/raft-invariants.md does not mention {}",
            entry.id,
        );
        assert!(
            list.contains(&format!("| `{}` | {}", entry.id, entry.statement)),
            "docs/raft-invariants.md list does not include {}",
            entry.id,
        );
    }
}

fn assert_rendered_doc_is_current(workspace: &Path) {
    let output = Command::new("python3")
        .arg("scripts/render-raft-invariants-doc")
        .arg("--check")
        .current_dir(workspace)
        .output()
        .expect("run invariant doc renderer check");
    assert!(
        output.status.success(),
        "docs/raft-invariants.md is not the exact rendered output\nstdout:\n{}\nstderr:\n{}",
        command_output_text(&output.stdout),
        command_output_text(&output.stderr),
    );
}

fn command_output_text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_owned()
}

fn assert_model_check_catalog_labels_are_registered(workspace: &Path, entries: &[Entry]) {
    let ids = entries
        .iter()
        .map(|entry| entry.id.as_str())
        .collect::<BTreeSet<_>>();
    let labels = model_check_catalog_labels(workspace);
    assert!(
        !labels.is_empty(),
        "model-check catalog labels should not be empty"
    );
    for label in labels {
        let Some((id, description)) = label.split_once(' ') else {
            panic!("model-check catalog label `{label}` must start with an invariant ID");
        };
        assert!(
            ids.contains(id),
            "model-check catalog label `{label}` references an unknown invariant ID",
        );
        assert!(
            !description.trim().is_empty(),
            "model-check catalog label `{label}` must include a short description",
        );
    }
}

fn model_check_catalog_labels(workspace: &Path) -> Vec<String> {
    let path = workspace.join("crates/rafter-sim/src/model_check/catalog.rs");
    let source = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let mut labels = Vec::new();
    let mut reading_const = false;
    for line in source.lines() {
        let line = line.trim();
        if line.starts_with("pub(super) const ") {
            reading_const = true;
        }
        if reading_const {
            if let Some(label) = quoted_literal(line) {
                labels.push(label);
                reading_const = false;
            }
        }
    }
    labels
}

fn assert_tla_invariant_counts_match(workspace: &Path, expected: usize) {
    for relative_path in [
        "specs/tla/raft/RaftCi.cfg",
        "specs/tla/raft/RaftNightly.cfg",
        "specs/tla/raft/Raft.cfg",
        "specs/tla/raft/RaftTraceSample.cfg",
    ] {
        let path = workspace.join(relative_path);
        assert_eq!(
            tla_invariant_count(&path),
            expected,
            "{relative_path} INVARIANTS block must match tla_predicates_now",
        );
    }
}

fn tla_invariant_count(path: &Path) -> usize {
    let source =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let mut in_block = false;
    let mut count = 0;

    for raw_line in source.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with("\\*") {
            if in_block && count > 0 {
                break;
            }
            continue;
        }
        if !in_block {
            in_block = line == "INVARIANTS";
            continue;
        }
        if !raw_line.chars().next().is_some_and(char::is_whitespace) {
            break;
        }
        count += 1;
    }

    assert!(in_block, "{} must declare INVARIANTS", path.display());
    assert!(
        count > 0,
        "{} must list at least one TLA invariant",
        path.display(),
    );
    count
}

fn quoted_literal(line: &str) -> Option<String> {
    let start = line.find('"')?;
    let rest = &line[start + 1..];
    let end = rest.find('"')?;
    Some(rest[..end].to_owned())
}

fn section<'a>(document: &'a str, start: &str, end: &str) -> &'a str {
    let start_index = document
        .find(start)
        .unwrap_or_else(|| panic!("document missing section {start}"));
    let after_start = &document[start_index..];
    let end_index = after_start
        .find(end)
        .unwrap_or_else(|| panic!("document missing section terminator {end} after {start}"));
    &after_start[..end_index]
}

fn declared_count(registry: &str, key: &str) -> usize {
    registry
        .lines()
        .find_map(|line| {
            let line = line.trim();
            line.strip_prefix(key)
                .and_then(|rest| rest.strip_prefix(": "))
                .and_then(|value| value.parse().ok())
        })
        .unwrap_or_else(|| panic!("registry missing count {key}"))
}

fn parse_entries(registry: &str) -> Vec<Entry> {
    let mut entries = Vec::new();
    let mut current = None::<Entry>;
    let mut active_map = None::<String>;
    let mut in_invariants = false;

    for raw_line in registry.lines() {
        if raw_line.trim().is_empty() || raw_line.trim_start().starts_with('#') {
            continue;
        }

        let indent = raw_line
            .chars()
            .take_while(|character| *character == ' ')
            .count();
        let line = raw_line.trim();

        if indent == 0 {
            in_invariants = line == "invariants:";
            active_map = None;
            continue;
        }
        if !in_invariants {
            continue;
        }

        if indent == 2 && line.starts_with("- id: ") {
            if let Some(entry) = current.take() {
                entries.push(entry);
            }
            current = Some(Entry {
                id: yaml_value(line.trim_start_matches("- id: ")),
                ..Entry::default()
            });
            active_map = None;
            continue;
        }

        let Some(entry) = current.as_mut() else {
            continue;
        };

        if indent == 4 && line.ends_with(':') {
            active_map = Some(line.trim_end_matches(':').to_owned());
            continue;
        }

        if indent == 4 {
            let Some((key, value)) = line.split_once(": ") else {
                continue;
            };
            set_entry_field(entry, key, yaml_value(value));
            active_map = None;
            continue;
        }

        if indent == 6 && active_map.as_deref() == Some("current_coverage") {
            let Some((key, value)) = line.split_once(": ") else {
                continue;
            };
            entry
                .current_coverage
                .insert(key.to_owned(), yaml_value(value));
        }
    }

    if let Some(entry) = current {
        entries.push(entry);
    }
    entries
}

fn parse_evidence(registry: &str) -> Vec<Evidence> {
    let mut evidence = Vec::new();
    let mut current = None::<Evidence>;
    let mut in_evidence = false;

    for raw_line in registry.lines() {
        if raw_line.trim().is_empty() || raw_line.trim_start().starts_with('#') {
            continue;
        }

        let indent = raw_line
            .chars()
            .take_while(|character| *character == ' ')
            .count();
        let line = raw_line.trim();

        if indent == 0 {
            if let Some(record) = current.take() {
                evidence.push(record);
            }
            in_evidence = line == "evidence:";
            continue;
        }
        if !in_evidence {
            continue;
        }

        if indent == 2 && line.starts_with("- id: ") {
            if let Some(record) = current.take() {
                evidence.push(record);
            }
            current = Some(Evidence {
                id: yaml_value(line.trim_start_matches("- id: ")),
                ..Evidence::default()
            });
            continue;
        }

        if indent == 4 {
            let Some((key, value)) = line.split_once(": ") else {
                continue;
            };
            let Some(record) = current.as_mut() else {
                continue;
            };
            set_evidence_field(record, key, yaml_value(value));
        }
    }

    if let Some(record) = current {
        evidence.push(record);
    }
    evidence
}

fn set_entry_field(entry: &mut Entry, key: &str, value: String) {
    match key {
        "kind" => entry.kind = value,
        "family" => entry.family = value,
        "tier" => entry.tier = value,
        "title" => entry.title = value,
        "statement" => entry.statement = value,
        "required_action" => entry.required_action = value,
        "priority" => entry.priority = value,
        _ => {}
    }
}

fn set_evidence_field(record: &mut Evidence, key: &str, value: String) {
    match key {
        "layer" => record.layer = value,
        "strength" => record.strength = value,
        "path" => record.path = value,
        "symbol" => record.symbol = value,
        "negative_fixture" => record.negative_fixture = Some(value),
        _ => {}
    }
}

fn yaml_value(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        value[1..value.len() - 1]
            .replace("\\\"", "\"")
            .replace("\\'", "'")
    } else {
        value.to_owned()
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
}
