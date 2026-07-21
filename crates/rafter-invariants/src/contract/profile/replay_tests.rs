//! Detector replay profile decoding scenarios.

use std::path::PathBuf;

use super::super::ProfileManifest;

#[test]
fn replay_identity_fields_reject_unsupported_values_at_decode_time() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let manifest = ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))
        .expect("load reviewed profile manifest");
    let encoded = serde_json::to_value(manifest).expect("encode reviewed manifest");
    for (field, expected) in [
        ("policy", "authenticated-source-fresh-execution-v1"),
        ("source", "private-authenticated-snapshot"),
        (
            "build",
            "locked-offline-authenticated-directory-source-no-default-features-v1",
        ),
        ("target_directory", "fresh-private-directory"),
        (
            "fixture_inventory",
            "all-profile-selected-direct-simulator-fixtures",
        ),
        ("challenge", "inherited-descriptor-pre-body-secret-v3"),
        ("artifact_policy", "json-and-process-logs"),
    ] {
        let mut changed = encoded.clone();
        changed["verifiers"]["pr"]["detector_replay"][field] = serde_json::json!("unsupported");

        let error = serde_json::from_value::<ProfileManifest>(changed)
            .expect_err("unsupported replay identity must fail")
            .to_string();
        assert!(error.contains(expected), "{field}: {error}");
    }
}
