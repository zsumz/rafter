use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use rafter_invariants::RegistryCounts;

use super::{
    doc_checks::assert_tla_invariant_counts_match, Clause, Entry, COVERAGE_LAYERS,
    EXPECTED_CANONICAL, EXPECTED_LIVENESS, EXPECTED_SAFETY, EXPECTED_TLA_PREDICATES,
    EXPECTED_TOTAL, EXPECTED_WELL_FORMEDNESS, ID_PREFIX_TO_KIND, VALID_FAMILIES, VALID_KINDS,
    VALID_TIERS,
};

pub(super) fn assert_declared_counts_match(
    counts: &RegistryCounts,
    entries: &[Entry],
    workspace: &Path,
) {
    assert_eq!(
        counts.total_entries, EXPECTED_TOTAL,
        "registry total_entries must match the reviewed catalog size",
    );
    assert_eq!(
        counts.canonical_raft_safety_properties, EXPECTED_CANONICAL,
        "registry canonical_raft_safety_properties count drifted",
    );
    assert_eq!(
        counts.tla_predicates_now, EXPECTED_TLA_PREDICATES,
        "registry tla_predicates_now count drifted",
    );
    assert_eq!(
        counts.well_formedness_meta_invariants, EXPECTED_WELL_FORMEDNESS,
        "registry well-formedness count drifted",
    );
    assert_eq!(
        counts.semantic_safety_invariants, EXPECTED_SAFETY,
        "registry safety count drifted",
    );
    assert_eq!(
        counts.liveness_obligations, EXPECTED_LIVENESS,
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

pub(super) fn assert_entries_are_well_formed(entries: &[Entry]) {
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
                && !entry.scope.trim().is_empty()
                && !entry.assumptions.trim().is_empty()
                && !entry.action_class.trim().is_empty()
                && !entry.next_action.trim().is_empty(),
            "{} must have title, statement, scope, assumptions, action_class, and next_action",
            entry.id,
        );
        assert!(
            matches!(
                entry.action_class.as_str(),
                "completion_blocker" | "future_strengthening"
            ),
            "{} has invalid action_class {}",
            entry.id,
            entry.action_class,
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

pub(super) fn assert_clauses_are_well_formed(entries: &[Entry], clauses: &[Clause]) {
    let parents = entries
        .iter()
        .map(|entry| entry.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut ids = BTreeSet::new();
    for clause in clauses {
        assert!(
            ids.insert(clause.id.as_str()),
            "{} is duplicated",
            clause.id
        );
        assert!(
            parents.contains(clause.invariant_id.as_str()),
            "{} has unknown parent {}",
            clause.id,
            clause.invariant_id,
        );
        assert!(
            clause.id.starts_with(&format!("{}.", clause.invariant_id)),
            "{} must be namespaced by parent {}",
            clause.id,
            clause.invariant_id,
        );
        assert!(
            !clause.statement.trim().is_empty()
                && !clause.scope.trim().is_empty()
                && !clause.assumptions.trim().is_empty(),
            "{} must document statement, scope, and assumptions",
            clause.id,
        );
        assert!(
            clause.required,
            "{} normative clause must be required",
            clause.id
        );
    }
    for entry in entries {
        assert!(
            clauses.iter().any(|clause| clause.invariant_id == entry.id),
            "{} must own at least one normative clause",
            entry.id,
        );
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
