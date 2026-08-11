//! Scenarios: profile-v10 wire shape and cross-profile policy remain strict.

use std::path::PathBuf;

use super::ProfileManifest;

#[test]
fn profile_v10_wire_round_trip_is_stable() {
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

/// Negative fixtures for the v9 -> v10 proof-obligation migration.
///
/// The migration is a contract identity change, so every retired identity must
/// fail closed: the old manifest schema, the old TLA producer, and the deleted
/// upstream tool pin. A checkout that still carries any of them must not be
/// able to produce or accept TLA evidence under the new contract.
#[test]
fn profile_v10_migration_rejects_every_retired_v9_identity() {
    let (catalog, manifest) = crate::tests::loaded();
    manifest
        .validate(&catalog)
        .expect("the v10 manifest validates");
    assert_eq!(manifest.schema_version, 10);

    let mut retired_schema = manifest.clone();
    retired_schema.schema_version = 9;
    assert!(retired_schema.validate(&catalog).is_err());

    let mut retired_producer = manifest.clone();
    retired_producer
        .profiles
        .get_mut("pr")
        .unwrap()
        .runners
        .get_mut("tla")
        .unwrap()
        .producer = "rafter-invariants-tla-v15".to_owned();
    assert!(retired_producer.validate(&catalog).is_err());

    for (key, retired) in [
        ("tool_asset_id", "481553986"),
        (
            "tool_sha256",
            "cc4803dce2a8ffaf0f5920a9dc39df4b5ee34ab4cb53fb58ac557277a7e516b3",
        ),
    ] {
        let mut retired_pin = manifest.clone();
        retired_pin
            .profiles
            .get_mut("nightly")
            .unwrap()
            .runners
            .get_mut("tla")
            .unwrap()
            .configuration
            .insert(key.to_owned(), retired.to_owned());
        assert!(
            retired_pin.validate(&catalog).is_err(),
            "retired tool {key} must be rejected"
        );
    }
}

/// The obligation vocabulary must decode as absent-is-empty and re-encode as
/// absent, so a manifest that predates it and one that declares an empty list
/// are the same contract. Without that, the v10 bump would silently change
/// every receipt that embeds a runner contract.
#[test]
fn an_absent_obligation_list_round_trips_as_empty() {
    let manifest = load_manifest();
    for profile in ["pr", "nightly", "weekly"] {
        assert!(manifest.profiles[profile].runners["tla"]
            .obligations
            .is_empty());
    }

    let mut absent = serde_json::to_value(&manifest).unwrap();
    absent["profiles"]["pr"]["runners"]["tla"]
        .as_object_mut()
        .unwrap()
        .remove("obligations");
    let decoded: ProfileManifest = serde_json::from_value(absent).unwrap();
    assert!(decoded.profiles["pr"].runners["tla"].obligations.is_empty());
    assert!(
        serde_json::to_value(&decoded).unwrap()["profiles"]["pr"]["runners"]["tla"]
            .get("obligations")
            .is_none()
    );
    assert_eq!(decoded, manifest);
}

#[test]
fn profile_policy_vocabulary_rejects_every_unknown_wire_identity() {
    let manifest = load_manifest();
    for (field, value) in [
        ("evidence_policy", "some_registry_evidence"),
        ("clause_policy", "some_required_clauses"),
        ("required_clause_strength", "indirect"),
    ] {
        let mut changed = serde_json::to_value(&manifest).expect("encode manifest");
        changed["profiles"]["pr"][field] = serde_json::Value::String(value.to_owned());
        assert!(serde_json::from_value::<ProfileManifest>(changed).is_err());
    }
    for (field, value) in [
        ("required_layers", "ceremonial"),
        ("required_strengths", "partial"),
    ] {
        let mut changed = serde_json::to_value(&manifest).expect("encode manifest");
        changed["profiles"]["pr"][field][0] = serde_json::Value::String(value.to_owned());
        assert!(serde_json::from_value::<ProfileManifest>(changed).is_err());
    }
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
        .push(super::EvidenceLayer::Tests);
    assert!(duplicate_layer.validate(&catalog).is_err());

    let mut missing_scheduled_layer = manifest.clone();
    let nightly = missing_scheduled_layer.profiles.get_mut("nightly").unwrap();
    nightly
        .required_layers
        .retain(|layer| *layer != super::EvidenceLayer::Maelstrom);
    nightly.runners.remove("maelstrom");
    assert!(missing_scheduled_layer.validate(&catalog).is_err());

    let mut missing_strength = manifest.clone();
    missing_strength
        .profiles
        .get_mut("weekly")
        .unwrap()
        .required_strengths
        .retain(|strength| *strength != super::EvidenceStrength::E2e);
    assert!(missing_strength.validate(&catalog).is_err());

    let mut extra_profile = manifest.clone();
    extra_profile
        .profiles
        .insert("ad-hoc".to_owned(), extra_profile.profiles["pr"].clone());
    assert!(extra_profile.validate(&catalog).is_err());

    let mut missing_verifier = manifest.clone();
    missing_verifier.verifiers.remove("pr");
    assert!(missing_verifier.validate(&catalog).is_err());

    let mut weak_canonical = manifest.clone();
    weak_canonical
        .profiles
        .get_mut("pr")
        .unwrap()
        .canonical_minimum_independent_layers = 1;
    assert!(weak_canonical.validate(&catalog).is_err());

    let mut unreviewed_canonical = manifest;
    unreviewed_canonical
        .profiles
        .get_mut("pr")
        .unwrap()
        .canonical_minimum_independent_layers = 3;
    assert!(unreviewed_canonical.validate(&catalog).is_err());
}

#[test]
fn validation_rejects_replay_identity_and_resource_drift() {
    let (catalog, manifest) = crate::tests::loaded();
    for mutate in [
        |contract: &mut super::DetectorReplayContract| contract.required_unique_fixtures = 76,
        |contract: &mut super::DetectorReplayContract| contract.required_evidence_bindings = 78,
        |contract: &mut super::DetectorReplayContract| contract.required_registry_packages = 246,
        |contract: &mut super::DetectorReplayContract| contract.required_targets = 1,
        |contract: &mut super::DetectorReplayContract| {
            contract.maximum_registry_expanded_bytes = u64::MAX;
        },
        |contract: &mut super::DetectorReplayContract| {
            contract.required_inventory_sha256 = "0".repeat(64);
        },
        |contract: &mut super::DetectorReplayContract| {
            contract.total_timeout_seconds -= 1;
        },
    ] {
        let mut changed = manifest.clone();
        mutate(&mut changed.verifiers.get_mut("pr").unwrap().detector_replay);
        assert!(changed.validate(&catalog).is_err());
    }
}

#[test]
fn every_profile_preserves_the_reviewed_detector_replay_budget() {
    let manifest = load_manifest();
    for profile in ["pr", "nightly", "weekly"] {
        assert_eq!(
            manifest.verifiers[profile]
                .detector_replay
                .total_timeout_seconds,
            super::replay::REVIEWED_DETECTOR_REPLAY_TOTAL_TIMEOUT_SECONDS,
            "{profile} detector replay budget drifted"
        );
    }
}

fn load_manifest() -> ProfileManifest {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))
        .expect("load profile manifest")
}
