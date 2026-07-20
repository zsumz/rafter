//! Ordering and table primitives for canonical registry rendering.

use std::{collections::BTreeMap, fmt::Write as _};

use super::super::{RegistryCounts, RegistryEvidence, RegistryInvariant};

pub(super) const FAMILY_ORDER: &[(&str, &str, &str)] = &[
    (
        "state",
        "State Shape",
        "well-formed protocol state and derived indexes",
    ),
    (
        "election",
        "Terms, Votes, and Leadership",
        "terms, votes, leadership, pre-vote, and authority fencing",
    ),
    (
        "log",
        "Log Replication",
        "leader append-only behavior, append acceptance, and log matching",
    ),
    (
        "commit",
        "Commitment and Application",
        "commitment, application order, and state-machine safety",
    ),
    (
        "membership",
        "Dynamic Membership",
        "stable and joint configurations, learners, and transfers",
    ),
    (
        "read",
        "Read Barriers and Leases",
        "ReadIndex, lease reads, apply-before-read, and history checks",
    ),
    (
        "persistence",
        "Persistence, Restart, and Snapshots",
        "durability, restart, applied floors, and snapshots",
    ),
    (
        "liveness",
        "Liveness Obligations",
        "bounded progress obligations with explicit assumptions",
    ),
];

const PREFIX_ORDER: &[&str] = &["ST", "EL", "LG", "CM", "AP", "MB", "RD", "PS", "SS", "LV"];

pub(super) fn render_count_table(output: &mut String, counts: &RegistryCounts) {
    output.push_str("| Scope | Count |\n| --- | ---: |\n");
    for (label, count) in [
        (
            "Canonical Raft paper safety properties",
            counts.canonical_raft_safety_properties,
        ),
        (
            "Predicates in Rafter's current TLA+ config",
            counts.tla_predicates_now,
        ),
        (
            "Well-formedness meta-invariants",
            counts.well_formedness_meta_invariants,
        ),
        (
            "Semantic safety invariants",
            counts.semantic_safety_invariants,
        ),
        ("Liveness obligations", counts.liveness_obligations),
        ("Catalog entries", counts.total_entries),
    ] {
        let _ = writeln!(output, "| {label} | {count} |");
    }
}

pub(super) fn render_family_map(output: &mut String, invariants: &[RegistryInvariant]) {
    let counts = count_by(invariants.iter().map(|entry| entry.family.as_str()));
    output.push_str("| Family | Entries | Purpose |\n| --- | ---: | --- |\n");
    for (family, label, purpose) in FAMILY_ORDER {
        let _ = writeln!(
            output,
            "| {label} | {} | {purpose} |",
            counts.get(family).copied().unwrap_or_default()
        );
    }
}

pub(super) fn render_evidence_references(output: &mut String, evidence: &[RegistryEvidence]) {
    output.push_str(
        "| ID | Clauses | Layer | Strength | Reference |\n| --- | --- | --- | --- | --- |\n",
    );
    let mut sorted = evidence.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|record| {
        let (prefix, number) = split_id(&record.id);
        (
            prefix_position(prefix),
            number,
            record.layer.as_str(),
            record.strength.as_str(),
            record.path.as_str(),
        )
    });
    for record in sorted {
        let mut reference = format!("`{}#{}`", record.path, record.symbol);
        if let Some(fixture) = &record.negative_fixture {
            let _ = write!(reference, "; negative fixture `{fixture}`");
        }
        if let Some(group) = &record.atomic_group {
            let _ = write!(reference, "; reviewed atomic group `{group}`");
        }
        if let Some(kind) = record.persistence_evidence {
            let _ = write!(reference, "; persistence evidence `{}`", kind.wire_name());
        }
        if let Some(exemption) = &record.negative_fixture_exemption {
            let _ = write!(reference, "; negative fixture exemption `{exemption}`");
        }
        let _ = writeln!(
            output,
            "| `{}` | `{}` | {} | {} | {} |",
            record.id,
            record.clauses.join(","),
            record.layer,
            record.strength,
            reference
        );
    }
}

pub(super) fn sorted_invariants(invariants: &[RegistryInvariant]) -> Vec<&RegistryInvariant> {
    let mut sorted = invariants.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|entry| {
        let (prefix, number) = split_id(&entry.id);
        (prefix_position(prefix), number)
    });
    sorted
}

fn prefix_position(prefix: &str) -> usize {
    PREFIX_ORDER
        .iter()
        .position(|candidate| *candidate == prefix)
        .unwrap_or(usize::MAX)
}

fn split_id(id: &str) -> (&str, usize) {
    let (prefix, number) = id.split_once('-').unwrap_or((id, "0"));
    (prefix, number.parse().unwrap_or(usize::MAX))
}

pub(super) fn kind_label(kind: &str) -> &str {
    match kind {
        "well_formedness" => "well-formedness",
        "safety" => "safety",
        "liveness" => "liveness",
        other => other,
    }
}

pub(super) fn count_by<'a>(values: impl Iterator<Item = &'a str>) -> BTreeMap<&'a str, usize> {
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value).or_default() += 1;
    }
    counts
}
