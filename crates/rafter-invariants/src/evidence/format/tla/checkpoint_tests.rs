//! Checkpoint wire-identity compatibility scenarios.

use std::collections::BTreeMap;

use super::CheckpointContract;

#[test]
fn checkpoint_contract_v1_has_stable_json_bytes_and_digest() {
    let contract = CheckpointContract {
        schema_version: 1,
        profile: "weekly".to_owned(),
        config: "Raft.cfg".to_owned(),
        runner_contract_sha256: "runner".to_owned(),
        input_sha256: BTreeMap::from([("tla-spec".to_owned(), "spec".to_owned())]),
    };
    let expected = br#"{"schema_version":1,"profile":"weekly","config":"Raft.cfg","runner_contract_sha256":"runner","input_sha256":{"tla-spec":"spec"}}"#;

    assert_eq!(
        serde_json::to_vec(&contract).expect("serialize contract"),
        expected
    );
    assert_eq!(
        contract.sha256().expect("digest contract"),
        "a46abc0286b64dd7d8168a0837ec8e6f5260ef8c35ca4fed871431d81e14d915"
    );
}
