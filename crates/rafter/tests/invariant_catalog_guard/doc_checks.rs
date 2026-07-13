use std::{collections::BTreeSet, fs, path::Path};

use rafter_invariants::{render_registry_markdown, RegistryDocument};

use super::Entry;

pub(super) fn assert_generated_doc_mentions_every_entry(doc: &str, entries: &[Entry]) {
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

pub(super) fn assert_rendered_doc_is_current(registry: &RegistryDocument, current: &str) {
    assert_eq!(
        current,
        render_registry_markdown(registry),
        "docs/raft-invariants.md is not the exact canonical Rust rendering",
    );
}

pub(super) fn assert_model_check_catalog_labels_are_registered(
    workspace: &Path,
    entries: &[Entry],
) {
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

pub(super) fn assert_tla_invariant_counts_match(workspace: &Path, expected: usize) {
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
