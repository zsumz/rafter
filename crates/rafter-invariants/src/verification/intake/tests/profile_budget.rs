//! Scenarios: whole-profile budgets preserve report representability.

use crate::evidence::limits::{MAX_ARTIFACT_REFS_PER_BUNDLE, MAX_VERDICT_ARTIFACT_REFS};
use crate::verification::bundle::{BundleBudget, ProfileBudget, MAX_RECEIPT_BYTES};

#[test]
fn receipt_budget_is_derived_from_the_validated_runner_set() {
    let budget = ProfileBudget::for_trusted("pr", 3).expect("PR profile budget");
    assert_eq!(budget.receipt_bytes(), 3 * MAX_RECEIPT_BYTES);

    let layer_budget = ProfileBudget::for_trusted("pr", 1).expect("layer budget");
    assert_eq!(layer_budget.receipt_bytes(), MAX_RECEIPT_BYTES);
    assert!(ProfileBudget::for_trusted("unknown", 1).is_err());
}

#[test]
fn every_supported_profile_fits_the_verdict_artifact_schema() {
    for (profile, runners) in [("pr", 3_usize), ("nightly", 4), ("weekly", 4)] {
        let budget = ProfileBudget::for_trusted(profile, runners).expect("profile budget");
        assert_eq!(budget.artifact_refs(), MAX_VERDICT_ARTIFACT_REFS);
        assert!(runners * MAX_ARTIFACT_REFS_PER_BUNDLE <= budget.artifact_refs());
    }

    let nightly =
        BundleBudget::for_trusted("nightly", "maelstrom").expect("nightly Maelstrom budget");
    assert_eq!(nightly.artifact_refs(), 1_024);
    assert_eq!(nightly.artifact_declarations(), 2_048);

    let weekly = BundleBudget::for_trusted("weekly", "maelstrom").expect("weekly Maelstrom budget");
    assert_eq!(weekly.artifact_refs(), MAX_VERDICT_ARTIFACT_REFS);
    assert_eq!(weekly.artifact_declarations(), 8_192);
}
