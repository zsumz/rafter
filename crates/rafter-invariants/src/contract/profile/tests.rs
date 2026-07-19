//! Scenarios: profile-v5 wire shape and cross-profile policy remain strict.

use std::path::PathBuf;

use super::ProfileManifest;

#[test]
fn profile_v5_wire_round_trip_is_stable() {
    let manifest = load_manifest();
    let encoded = serde_json::to_value(&manifest).expect("encode profile manifest");
    let decoded: ProfileManifest = serde_json::from_value(encoded.clone()).expect("decode profile");
    assert_eq!(decoded, manifest);
    assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);

    let (catalog, _) = crate::tests::loaded();
    let bundles = crate::tests::passing_bundles(&catalog, &manifest);
    for bundle in bundles {
        assert_eq!(
            bundle.execution.plan.contract,
            manifest.profiles[&bundle.profile]
        );
    }
}

#[test]
fn profile_models_reject_unknown_fields_and_preserve_the_reviewed_default() {
    let manifest = load_manifest();
    let mut unknown = serde_json::to_value(&manifest).unwrap();
    unknown["profiles"]["pr"]["runners"]["tests"]["unreviewed"] = serde_json::json!(true);
    assert!(serde_json::from_value::<ProfileManifest>(unknown).is_err());

    let mut absent_checks = serde_json::to_value(&manifest).unwrap();
    absent_checks["profiles"]["pr"]["runners"]["simulator"]
        .as_object_mut()
        .unwrap()
        .remove("simulator_checks");
    let decoded: ProfileManifest = serde_json::from_value(absent_checks).unwrap();
    assert!(decoded.profiles["pr"].runners["simulator"]
        .simulator_checks
        .is_empty());
    assert!(
        serde_json::to_value(decoded).unwrap()["profiles"]["pr"]["runners"]["simulator"]
            .get("simulator_checks")
            .is_none()
    );
}

#[test]
fn validation_rejects_profile_inventory_and_policy_drift() {
    let (catalog, manifest) = crate::tests::loaded();
    manifest
        .validate(&catalog)
        .expect("control manifest validates");

    let mut wrong_version = manifest.clone();
    wrong_version.schema_version += 1;
    assert!(wrong_version.validate(&catalog).is_err());

    let mut missing_id = manifest.clone();
    missing_id.reviewed_ids.pop();
    assert!(missing_id.validate(&catalog).is_err());

    let mut missing_profile = manifest.clone();
    missing_profile.profiles.remove("weekly");
    assert!(missing_profile.validate(&catalog).is_err());

    let mut duplicate_layer = manifest.clone();
    duplicate_layer
        .profiles
        .get_mut("pr")
        .unwrap()
        .required_layers
        .push("tests".to_owned());
    assert!(duplicate_layer.validate(&catalog).is_err());

    let mut weak_canonical = manifest;
    weak_canonical
        .profiles
        .get_mut("pr")
        .unwrap()
        .canonical_minimum_independent_layers = 1;
    assert!(weak_canonical.validate(&catalog).is_err());
}

fn load_manifest() -> ProfileManifest {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))
        .expect("load profile manifest")
}
