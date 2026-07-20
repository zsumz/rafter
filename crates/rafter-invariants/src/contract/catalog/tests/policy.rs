//! Adversarial mutations for reviewed evidence policy.

use super::*;
use crate::PersistenceEvidenceKind;

#[test]
fn client_witness_cannot_be_removed() {
    let mut registry = load_registry();
    registry.evidence.retain(|record| {
        record.id != "RD-06"
            || record.layer != "maelstrom"
            || record.path != "scripts/maelstrom-lin-kv"
    });

    let error = Catalog::try_from(registry).expect_err("client witness removal must fail closed");
    assert_eq!(
        error.to_string(),
        "client-visible invariant RD-06 must retain its reviewed executable witness"
    );
}

#[test]
fn client_witness_cannot_be_replaced_by_relabeling_an_ordinary_row() {
    let mut registry = load_registry();
    registry.evidence.retain(|record| {
        record.id != "RD-06"
            || record.layer != "maelstrom"
            || record.path != "scripts/maelstrom-lin-kv"
    });
    let forged = registry
        .evidence
        .iter_mut()
        .find(|record| record.id == "RD-06")
        .expect("ordinary RD-06 evidence");
    forged.layer = "maelstrom".to_owned();
    forged.strength = "e2e".to_owned();

    let error = Catalog::try_from(registry).expect_err("forged client witness must fail closed");
    assert_eq!(
        error.to_string(),
        "client-visible invariant RD-06 must retain its reviewed executable witness"
    );
}

#[test]
fn reviewed_client_classification_cannot_be_downgraded() {
    let mut registry = load_registry();
    registry
        .invariants
        .iter_mut()
        .find(|invariant| invariant.id == "RD-06")
        .expect("RD-06 invariant")
        .tier = "feature".to_owned();

    let error = Catalog::try_from(registry).expect_err("client reclassification must fail");
    assert!(error
        .to_string()
        .starts_with("reviewed client invariant IDs changed:"));
}

#[test]
fn persistence_witness_cannot_be_replaced_by_relabeling_an_ordinary_test() {
    let mut registry = load_registry();
    let reviewed = registry
        .evidence
        .iter_mut()
        .find(|record| record.id == "PS-01" && record.persistence_evidence.is_some())
        .expect("reviewed PS-01 witness");
    reviewed.persistence_evidence = None;
    let forged = registry
        .evidence
        .iter_mut()
        .find(|record| record.id == "PS-01" && record.persistence_evidence.is_none())
        .expect("ordinary PS-01 test");
    forged.persistence_evidence = Some(PersistenceEvidenceKind::FailureInjection);

    let error = Catalog::try_from(registry).expect_err("forged persistence witness must fail");
    assert_eq!(
        error.to_string(),
        "persistence invariant PS-01 must retain its reviewed executable witness"
    );
}

#[test]
fn persistence_witness_cannot_move_to_another_layer() {
    let mut registry = load_registry();
    let reviewed = registry
        .evidence
        .iter_mut()
        .find(|record| record.id == "PS-02" && record.persistence_evidence.is_some())
        .expect("reviewed PS-02 witness");
    reviewed.layer = "simulator".to_owned();

    let error = Catalog::try_from(registry).expect_err("relocated witness must fail");
    assert_eq!(
        error.to_string(),
        "persistence invariant PS-02 must retain its reviewed executable witness"
    );
}

#[test]
fn persistence_witness_test_identity_is_immutable() {
    let mut registry = load_registry();
    let reviewed = registry
        .evidence
        .iter_mut()
        .find(|record| record.id == "SS-01" && record.persistence_evidence.is_some())
        .expect("reviewed SS-01 witness");
    reviewed
        .test
        .as_mut()
        .expect("tests-layer identity")
        .test_name = "a_different_test".to_owned();

    let error = Catalog::try_from(registry).expect_err("identity mutation must fail");
    assert_eq!(
        error.to_string(),
        "persistence invariant SS-01 must retain its reviewed executable witness"
    );
}

#[test]
fn persistence_witness_cannot_gain_unreviewed_qualification_metadata() {
    let mut registry = load_registry();
    let reviewed = registry
        .evidence
        .iter_mut()
        .find(|record| record.id == "PS-03" && record.persistence_evidence.is_some())
        .expect("reviewed PS-03 witness");
    reviewed.negative_fixture_exemption = Some("unreviewed exception".to_owned());

    let error = Catalog::try_from(registry).expect_err("auxiliary claim mutation must fail");
    assert_eq!(
        error.to_string(),
        "persistence invariant PS-03 must retain its reviewed executable witness"
    );
}

#[test]
fn unreviewed_persistence_claims_are_rejected() {
    let mut registry = load_registry();
    let record = registry
        .evidence
        .iter_mut()
        .find(|record| record.id == "PS-02" && record.persistence_evidence.is_none())
        .expect("ordinary PS-02 evidence row");
    record.persistence_evidence = Some(PersistenceEvidenceKind::CrashReopen);

    let error = Catalog::try_from(registry).expect_err("unreviewed claim must fail");
    assert_eq!(
        error.to_string(),
        "PS-02 persistence evidence is not a reviewed executable witness"
    );
}

#[test]
fn non_persistence_invariants_cannot_claim_persistence_evidence() {
    let mut registry = load_registry();
    let record = registry
        .evidence
        .iter_mut()
        .find(|record| record.id == "RD-06")
        .expect("RD-06 evidence row");
    record.persistence_evidence = Some(PersistenceEvidenceKind::CrashReopen);

    let error = Catalog::try_from(registry).expect_err("misplaced persistence tag must fail");
    assert_eq!(
        error.to_string(),
        "RD-06 persistence evidence is not a reviewed executable witness"
    );
}

#[test]
fn reviewed_persistence_classification_cannot_be_downgraded() {
    let mut registry = load_registry();
    registry
        .invariants
        .iter_mut()
        .find(|invariant| invariant.id == "PS-01")
        .expect("PS-01 invariant")
        .family = "commit".to_owned();

    let error = Catalog::try_from(registry).expect_err("persistence reclassification must fail");
    assert!(error
        .to_string()
        .starts_with("reviewed persistence invariant IDs changed:"));
}
