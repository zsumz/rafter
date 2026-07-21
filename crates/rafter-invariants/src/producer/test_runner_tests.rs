//! Tests-layer adaptation for identities that also serve as negative detectors.

use crate::contract::TestIdentity;

use super::test_evidence;

#[test]
fn dual_role_detector_uses_proof_bound_execution_without_changing_its_test_identity() {
    let (catalog, manifest) = crate::tests::loaded();
    let inventory = test_evidence(&catalog, &manifest.profiles["pr"]);
    let dual_role = identity(
        "model_check::tests::linearizability::linearizer_rejects_read_that_misses_completed_write",
    );
    let ordinary = identity(
        "model_check::tests::linearizability::linearizer_can_include_unknown_write_to_explain_later_read",
    );

    let dual_role_evidence = inventory
        .get(&dual_role)
        .expect("reviewed dual-role test identity");
    assert!(dual_role_evidence.requires_detector_proof);
    assert!(dual_role_evidence
        .descriptors
        .iter()
        .all(|descriptor| descriptor.test.as_ref() == Some(&dual_role)));
    assert!(
        !inventory
            .get(&ordinary)
            .expect("reviewed ordinary test identity")
            .requires_detector_proof
    );
}

fn identity(test_name: &str) -> TestIdentity {
    TestIdentity {
        package: "rafter-sim".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_sim".to_owned(),
        test_name: test_name.to_owned(),
    }
}
