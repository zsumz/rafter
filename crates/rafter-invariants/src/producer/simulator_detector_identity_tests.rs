//! Simulator detector identity uniqueness scenarios.

use super::unique_detector_identities;
use crate::TestIdentity;

fn identity(package: &str, kind: &str, target: &str) -> TestIdentity {
    TestIdentity {
        package: package.to_owned(),
        target_kind: kind.to_owned(),
        target: target.to_owned(),
        test_name: "same_test_name".to_owned(),
    }
}

#[test]
fn detector_inventory_deduplicates_only_complete_test_identities() {
    let first = identity("first", "lib", "first");
    let second = identity("second", "test", "second");
    let unique = unique_detector_identities(vec![first.clone(), first, second])
        .expect("complete identities remain distinct");
    assert_eq!(unique.len(), 2);
    assert_ne!(unique[0].check_id(), unique[1].check_id());
}

#[test]
fn detector_inventory_rejects_ambiguous_check_id_encoding() {
    let first = identity("a/b", "c", "d");
    let second = identity("a", "b/c", "d");
    assert_eq!(first.check_id(), second.check_id());
    assert!(unique_detector_identities(vec![first, second]).is_err());
}
