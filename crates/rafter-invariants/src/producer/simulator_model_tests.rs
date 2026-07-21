//! Scenarios for deterministic simulator plans and canonical scheduled identities.

use super::{canonical_check_id, execution_plan};

#[test]
fn scheduled_plans_use_stable_source_derived_seed_counts() {
    let first = execution_plan("nightly", "abc123").expect("nightly plan");
    let second = execution_plan("nightly", "abc123").expect("nightly plan");
    assert_eq!(first, second);
    assert_eq!(first.len(), 1);
    let seeds = first[0].arguments[3].to_string_lossy();
    assert_eq!(seeds.split(',').count(), 6);

    let weekly = execution_plan("weekly", "abc123").expect("weekly plan");
    assert_eq!(
        weekly[0].arguments[3].to_string_lossy().split(',').count(),
        10
    );
    assert_ne!(first[0].arguments[3], weekly[0].arguments[3]);
}

#[test]
fn scheduled_check_ids_bind_to_canonical_registry_checks() {
    assert_eq!(
        canonical_check_id("nightly", "raft-commit-nightly").as_deref(),
        Some("raft-commit")
    );
    assert_eq!(
        canonical_check_id("weekly", "raft-election-prevote-weekly").as_deref(),
        Some("raft-election-prevote")
    );
    assert_eq!(
        canonical_check_id("nightly", "raft-nightly-soak-membership").as_deref(),
        Some("raft-soak-membership")
    );
    assert_eq!(canonical_check_id("pr", "raft-commit"), None);
}

#[test]
fn unsupported_simulator_profile_has_no_execution_plan() {
    assert!(execution_plan("adhoc", "abc123").is_err());
}
