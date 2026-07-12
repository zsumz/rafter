use super::{Clause, Entry, Evidence};

pub(super) fn parse_entries(registry: &str) -> Vec<Entry> {
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

pub(super) fn parse_clauses(registry: &str) -> Vec<Clause> {
    let mut clauses = Vec::new();
    let mut current = None::<Clause>;
    let mut in_clauses = false;
    for raw_line in registry.lines() {
        let indent = raw_line
            .chars()
            .take_while(|character| *character == ' ')
            .count();
        let line = raw_line.trim();
        if indent == 0 {
            if let Some(clause) = current.take() {
                clauses.push(clause);
            }
            in_clauses = line == "clauses:";
            continue;
        }
        if !in_clauses || line.is_empty() || line.starts_with('#') {
            continue;
        }
        if indent == 2 && line.starts_with("- id: ") {
            if let Some(clause) = current.take() {
                clauses.push(clause);
            }
            current = Some(Clause {
                id: yaml_value(line.trim_start_matches("- id: ")),
                ..Clause::default()
            });
            continue;
        }
        if indent != 4 {
            continue;
        }
        let Some((key, value)) = line.split_once(": ") else {
            continue;
        };
        let Some(clause) = current.as_mut() else {
            continue;
        };
        let value = yaml_value(value);
        match key {
            "invariant_id" => clause.invariant_id = value,
            "statement" => clause.statement = value,
            "scope" => clause.scope = value,
            "assumptions" => clause.assumptions = value,
            "required" => clause.required = value == "true",
            _ => {}
        }
    }
    if let Some(clause) = current {
        clauses.push(clause);
    }
    clauses
}

pub(super) fn parse_evidence(registry: &str) -> Vec<Evidence> {
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

pub(super) fn declared_count(registry: &str, key: &str) -> usize {
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

fn set_entry_field(entry: &mut Entry, key: &str, value: String) {
    match key {
        "kind" => entry.kind = value,
        "family" => entry.family = value,
        "tier" => entry.tier = value,
        "title" => entry.title = value,
        "statement" => entry.statement = value,
        "scope" => entry.scope = value,
        "assumptions" => entry.assumptions = value,
        "action_class" => entry.action_class = value,
        "next_action" => entry.next_action = value,
        "priority" => entry.priority = value,
        _ => {}
    }
}

fn set_evidence_field(record: &mut Evidence, key: &str, value: String) {
    match key {
        "clauses" => {
            record.clauses = value
                .split(',')
                .map(str::trim)
                .filter(|clause| !clause.is_empty())
                .map(str::to_owned)
                .collect();
        }
        "layer" => record.layer = value,
        "strength" => record.strength = value,
        "path" => record.path = value,
        "symbol" => record.symbol = value,
        "negative_fixture" => record.negative_fixture = Some(value),
        "negative_fixture_path" => record.negative_fixture_path = Some(value),
        "negative_fixture_detector" => record.negative_fixture_detector = Some(value),
        "negative_fixture_exemption" => record.negative_fixture_exemption = Some(value),
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
