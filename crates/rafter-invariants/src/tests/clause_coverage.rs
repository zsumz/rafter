//! A clause needs its own direct evidence, even when several bindings cover it.

use super::{aggregate, assert_only_parent_red, loaded, passing_bundles};

#[test]
fn missing_direct_clause_keeps_only_parent_red() {
    let (mut catalog, manifest) = loaded();
    let direct: Vec<_> = catalog
        .evidence
        .iter_mut()
        .filter(|evidence| evidence.clause_id == "RD-06.b" && evidence.strength == "direct")
        .collect();
    assert!(!direct.is_empty(), "RD-06.b direct evidence exists");
    for evidence in direct {
        evidence.invariant_id = "RD-05".to_owned();
        evidence.clause_id = "RD-05.a".to_owned();
    }
    let bundles = passing_bundles(&catalog, &manifest);

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_only_parent_red(&report, "RD-06");
    assert!(report.invariants.iter().any(|verdict| {
        verdict.invariant_id == "RD-06"
            && verdict
                .issues
                .iter()
                .any(|issue| issue.evidence_id == "RD-06.b/coverage")
    }));
}

#[test]
fn e2e_does_not_satisfy_direct_clause() {
    let (mut catalog, manifest) = loaded();
    let direct: Vec<_> = catalog
        .evidence
        .iter_mut()
        .filter(|evidence| evidence.clause_id == "RD-06.b" && evidence.strength == "direct")
        .collect();
    assert!(!direct.is_empty(), "RD-06.b direct evidence exists");
    for evidence in direct {
        evidence.strength = "e2e".to_owned();
    }
    let bundles = passing_bundles(&catalog, &manifest);

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_only_parent_red(&report, "RD-06");
    assert!(report.invariants.iter().any(|verdict| {
        verdict.invariant_id == "RD-06"
            && verdict
                .issues
                .iter()
                .any(|issue| issue.message.contains("has no direct evidence"))
    }));
}

#[test]
fn sibling_clause_evidence_cannot_substitute() {
    let (mut catalog, manifest) = loaded();
    let direct: Vec<_> = catalog
        .evidence
        .iter_mut()
        .filter(|evidence| evidence.clause_id == "RD-06.b" && evidence.strength == "direct")
        .collect();
    assert!(!direct.is_empty(), "RD-06.b direct evidence exists");
    for evidence in direct {
        evidence.clause_id = "RD-06.a".to_owned();
    }
    let bundles = passing_bundles(&catalog, &manifest);

    let report = aggregate(&catalog, &manifest, "pr", "abc", &bundles).expect("report aggregates");
    assert_only_parent_red(&report, "RD-06");
}
