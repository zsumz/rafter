//! Scenarios: evidence wire shape and required-null semantics remain stable.

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{EvidenceStatus, FailureClassification, ResultBundle};

const RESULT_V13_GOLDEN: &str = include_str!("fixtures/result-v13-minimal.json");

#[test]
fn result_v13_golden_document_is_schema_valid_and_byte_stable() {
    let expected: Value = serde_json::from_str(RESULT_V13_GOLDEN).expect("valid golden JSON");
    super::validate_result_value(&expected).expect("schema-valid golden bundle");
    let decoded: ResultBundle =
        serde_json::from_value(expected.clone()).expect("decode golden bundle");
    let bytes = serde_json::to_vec(&decoded).expect("encode golden bundle bytes");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "0b955567a6e4052ee286518b8fd8dec51a7e78e7e34f062678512397ba46b658"
    );
    let encoded = serde_json::to_value(decoded).expect("encode golden bundle value");
    assert_eq!(encoded, expected);
    assert!(encoded["execution"]["plan"]["contract"]["runners"]["tests"]
        .get("simulator_checks")
        .is_none());
    assert!(encoded["results"][0].get("classification").is_none());
    assert!(encoded["results"][0].get("message").is_none());
    assert!(encoded["results"][0].get("artifacts").is_none());
}

#[test]
fn result_v13_wire_round_trip_is_stable() {
    let (catalog, manifest) = crate::tests::loaded();
    for bundle in crate::tests::passing_bundles(&catalog, &manifest) {
        let encoded = serde_json::to_value(&bundle).expect("encode result bundle");
        super::validate_result_value(&encoded).expect("schema-valid bundle");
        let decoded: ResultBundle =
            serde_json::from_value(encoded.clone()).expect("decode result bundle");
        assert_eq!(decoded, bundle);
        assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
    }
}

#[test]
fn simulator_liveness_is_required_but_nullable() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .next()
        .expect("bundle");
    let mut value = serde_json::to_value(bundle).expect("encode bundle");
    value["execution"]["checks"][0]["simulator_liveness"] = Value::Null;
    serde_json::from_value::<ResultBundle>(value.clone()).expect("present null remains valid");

    value["execution"]["checks"][0]
        .as_object_mut()
        .expect("check object")
        .remove("simulator_liveness");
    assert!(serde_json::from_value::<ResultBundle>(value).is_err());
}

#[test]
fn required_nullable_liveness_fields_remain_schema_required() {
    let (catalog, manifest) = crate::tests::loaded();
    let bundle = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| {
            bundle
                .execution
                .checks
                .iter()
                .any(|check| check.simulator_liveness.is_some())
        })
        .expect("bundle with structured liveness evidence");
    let value = serde_json::to_value(bundle).expect("encode bundle");
    let check_index = value["execution"]["checks"]
        .as_array()
        .unwrap()
        .iter()
        .position(|check| !check["simulator_liveness"].is_null())
        .unwrap();

    let mut missing_contract_option = value.clone();
    missing_contract_option["execution"]["checks"][check_index]["simulator_liveness"]["contract"]
        .as_object_mut()
        .unwrap()
        .remove("stable_leader_retained");
    assert!(super::validate_result_value(&missing_contract_option).is_err());

    let mut missing_execution_option = value;
    missing_execution_option["execution"]["checks"][check_index]["simulator_liveness"]["reports"]
        [0]["execution_contract"]
        .as_object_mut()
        .unwrap()
        .remove("tick_skew_node_id");
    assert!(super::validate_result_value(&missing_execution_option).is_err());
}

#[test]
fn status_and_classification_wire_names_are_stable() {
    for (status, expected) in [
        (EvidenceStatus::Pass, "pass"),
        (EvidenceStatus::Fail, "fail"),
        (EvidenceStatus::Incomplete, "incomplete"),
        (EvidenceStatus::Error, "error"),
    ] {
        assert_eq!(serde_json::to_value(status).unwrap(), expected);
    }
    for (classification, expected) in [
        (
            FailureClassification::InvariantViolation,
            "invariant_violation",
        ),
        (
            FailureClassification::CoverageNotReached,
            "coverage_not_reached",
        ),
        (FailureClassification::HarnessError, "harness_error"),
    ] {
        assert_eq!(serde_json::to_value(classification).unwrap(), expected);
    }
}
