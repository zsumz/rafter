//! Reviewed replay inventory and source-binding regression scenarios.

use std::path::PathBuf;

#[test]
fn every_profile_replay_inventory_has_the_exact_reviewed_identity() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let catalog = crate::Catalog::load(&root.join("verification/raft-invariants.yaml"))
        .expect("load reviewed registry");
    let manifest =
        crate::ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))
            .expect("load profile manifest");
    manifest.validate(&catalog).expect("validate contracts");
    for profile in ["pr", "nightly", "weekly"] {
        let plan = super::prepare(
            &catalog,
            &manifest.profiles[profile],
            &manifest.verifiers[profile].detector_replay,
            &root,
        )
        .expect("prepare detector replay");

        assert_eq!(plan.fixture_count(), 77);
        assert_eq!(plan.target_count(), 2);
        assert_eq!(plan.evidence_binding_count(), 79);
        assert_eq!(
            plan.inventory_sha256().expect("hash replay inventory"),
            manifest.verifiers[profile]
                .detector_replay
                .required_inventory_sha256
        );
        assert!(plan.targets().values().flatten().all(|fixture| {
            fixture
                .expected_witnesses
                .contains_key(&format!("expect-err:{}", fixture.registered_identity))
        }));
    }
}

#[test]
#[cfg(target_os = "linux")]
#[ignore = "requires a clean checkout and complete authenticated Cargo archive cache"]
fn current_source_compiles_and_replays_every_reviewed_detector() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let catalog = crate::Catalog::load(&root.join("verification/raft-invariants.yaml"))
        .expect("load reviewed registry");
    let manifest =
        crate::ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))
            .expect("load profile manifest");
    manifest.validate(&catalog).expect("validate contracts");
    let contract = &manifest.verifiers["pr"].detector_replay;
    let mut source = crate::verification::source::SourceVerifier::capture(&root)
        .expect("capture authenticated clean source");
    let replay = super::prepare(
        &catalog,
        &manifest.profiles["pr"],
        contract,
        source.source_root(),
    )
    .expect("prepare detector replay");
    let compilation_source = source
        .prepare_compilation_source(crate::verification::source::RegistryMaterializationPolicy {
            required_packages: contract.required_registry_packages,
            maximum_archive_bytes: contract.maximum_registry_archive_bytes,
            maximum_expanded_bytes: contract.maximum_registry_expanded_bytes,
            maximum_entries: contract.maximum_registry_entries,
            deadline: super::deadlines(contract)
                .expect("derive replay deadlines")
                .work(),
        })
        .expect("materialize authenticated registry");
    assert_eq!(
        compilation_source.registry_package_count(),
        contract.required_registry_packages
    );
    let source_ref = crate::plan::current_source_ref().expect("read source ref");

    let assessment = super::execute(
        &replay,
        &compilation_source,
        contract,
        "pr",
        &source_ref,
        super::deadlines(contract).expect("derive replay deadlines"),
    )
    .expect("compile and execute detector replay");

    assert_eq!(
        assessment.qualifications.len(),
        contract.required_evidence_bindings
    );
    assert!(assessment
        .qualifications
        .values()
        .all(|qualification| qualification.is_passed()));
    assert!(assessment
        .artifacts
        .iter()
        .any(|artifact| artifact.kind == "verifier-replay-report"));
    assessment
        .artifact_guard
        .as_ref()
        .expect("replay artifacts remain guarded")
        .revalidate()
        .expect("revalidate replay artifacts");
}
