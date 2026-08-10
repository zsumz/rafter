//! Scenarios for deterministic simulator plans and canonical scheduled identities.

use std::collections::BTreeMap;

use super::{canonical_check_id, collect_events, execution_plan};
use crate::producer::simulator::events::passing_simulator_event_contract;

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
fn scheduled_soak_runs_bind_liveness_under_their_canonical_check_id() {
    for profile in ["nightly", "weekly"] {
        let (identity, contracts, events) =
            crate::verification::simulator::liveness_report_tests::scheduled_fixture(profile);
        crate::producer::simulator::liveness::derive_liveness_binding(
            profile, &identity, &contracts, &events,
        )
        .unwrap_or_else(|error| {
            panic!("{profile} canonical soak run must bind: {}", error.message)
        });
    }
}

#[test]
fn scheduled_events_are_canonicalized_before_contract_validation() {
    let source = serde_json::json!({
        "event": "exhaustive-check",
        "check_id": "raft-commit-nightly",
        "status": "pass",
        "classification": null,
        "observations": {"commit_floor_advances": 1},
        "unique_protocol_states": 10,
        "unique_verifier_states": 11
    });
    let stdout = format!("RAFTER_EVENT {source}\n");
    let mut events = BTreeMap::new();

    collect_events("nightly", stdout.as_bytes(), &mut events).expect("collect scheduled event");

    assert_eq!(
        events["raft-commit-nightly"][0]["check_id"],
        "raft-commit-nightly"
    );
    let canonical = &events["raft-commit"][0];
    assert_eq!(canonical["check_id"], "raft-commit");
    passing_simulator_event_contract("raft-commit", canonical)
        .expect("canonical event satisfies the passing contract");
}

#[test]
fn unsupported_simulator_profile_has_no_execution_plan() {
    assert!(execution_plan("adhoc", "abc123").is_err());
}
