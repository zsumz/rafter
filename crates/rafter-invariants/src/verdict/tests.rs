//! Scenarios: verdict wire omissions remain stable across schema evolution.

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{InvariantVerdict, VerdictIssue};

const GREEN_ROW_V2_GOLDEN: &str = include_str!("fixtures/green-row-v2.json");

#[test]
fn green_verdict_row_golden_document_preserves_empty_issue_omission() {
    let expected: Value =
        serde_json::from_str(GREEN_ROW_V2_GOLDEN).expect("valid golden verdict JSON");
    let decoded: InvariantVerdict =
        serde_json::from_value(expected.clone()).expect("decode golden verdict row");
    let bytes = serde_json::to_vec(&decoded).expect("encode golden verdict bytes");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "9f66cb3904f12b354d426ec6378b811999b2ad570cb47106c169f307ba013ab4"
    );
    let encoded = serde_json::to_value(decoded).expect("encode golden verdict row");
    assert_eq!(encoded, expected);
    assert!(encoded.get("issues").is_none());
    assert!(encoded["clauses"][0].get("issues").is_none());
}

#[test]
fn issue_without_artifacts_round_trips_as_the_schema_allows() {
    let expected = serde_json::json!({
        "evidence_id": "ST-01/direct/tests/golden",
        "status": "error",
        "classification": "harness_error",
        "message": "golden harness failure"
    });
    let decoded: VerdictIssue =
        serde_json::from_value(expected.clone()).expect("decode issue without artifacts");
    assert!(decoded.artifacts.is_empty());
    assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
}
