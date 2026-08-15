//! Scenarios for scheduled simulator check and seed identities.

use super::{
    canonical_simulator_check_id, scheduled_model_profile, scheduled_simulator_seeds,
};

#[test]
fn canonical_ids_preserve_scheduled_soak_suffixes() {
    assert_eq!(
        canonical_simulator_check_id("nightly", "raft-nightly-soak-membership").as_deref(),
        Some("raft-soak-membership")
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

/// Weekly canonicalizes what its *model* profile emits, not its lane name.
///
/// Weekly runs nightly's profile, so its events arrive suffixed `-nightly`.
/// A weekly lane that still canonicalized `-weekly` would silently drop every
/// event the run actually produced and report an empty, green-looking layer.
#[test]
fn weekly_canonicalizes_the_model_profile_it_actually_runs() {
    assert_eq!(scheduled_model_profile("weekly"), Some("raft-nightly"));
    assert_eq!(
        canonical_simulator_check_id("weekly", "raft-commit-nightly").as_deref(),
        Some("raft-commit")
    );
    assert_eq!(
        canonical_simulator_check_id("weekly", "raft-nightly-soak-membership").as_deref(),
        Some("raft-soak-membership")
    );
    assert_eq!(
        canonical_simulator_check_id("weekly", "raft-profile-total-nightly"),
        None
    );
    // The deep profile's own identities are not produced by any lane today.
    assert_eq!(canonical_simulator_check_id("weekly", "raft-commit-weekly"), None);
}

#[test]
fn scheduled_seeds_have_a_stable_known_answer() {
    assert_eq!(
        scheduled_simulator_seeds("nightly", "abc123", 2).as_deref(),
        Some("0x4b1fa59abaf90497,0xa47b7c9ec1c34c43")
    );
    assert_eq!(scheduled_simulator_seeds("pr", "abc123", 2), None);
}
