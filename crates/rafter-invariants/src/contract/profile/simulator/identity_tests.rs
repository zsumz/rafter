//! Scenarios for scheduled simulator check and seed identities.

use super::{canonical_simulator_check_id, scheduled_simulator_seeds};

#[test]
fn canonical_ids_preserve_scheduled_soak_suffixes() {
    assert_eq!(
        canonical_simulator_check_id("nightly", "raft-nightly-soak-membership").as_deref(),
        Some("raft-soak-membership")
    );
    assert_eq!(
        canonical_simulator_check_id("weekly", "raft-commit-weekly").as_deref(),
        Some("raft-commit")
    );
    assert_eq!(
        canonical_simulator_check_id("nightly", "raft-commit-prevote-nightly").as_deref(),
        Some("raft-commit")
    );
    assert_eq!(
        canonical_simulator_check_id("nightly", "raft-profile-total-nightly"),
        None
    );
    assert_eq!(canonical_simulator_check_id("pr", "raft-commit"), None);
}

#[test]
fn scheduled_seeds_have_a_stable_known_answer() {
    assert_eq!(
        scheduled_simulator_seeds("nightly", "abc123", 2).as_deref(),
        Some("0x4b1fa59abaf90497,0xa47b7c9ec1c34c43")
    );
    assert_eq!(scheduled_simulator_seeds("pr", "abc123", 2), None);
}
