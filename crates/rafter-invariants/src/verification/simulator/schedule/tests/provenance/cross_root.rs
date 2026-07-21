//! Cross-checkout source and compiler provenance scenarios.

use std::path::Path;

use super::super::verify_simulator_schedule;
use super::fixtures::{materialize_cross_root_fixture, ProvenanceSubstitution, RuntimeDefect};

#[test]
fn serialized_producer_root_a_provenance_verifies_at_aggregate_root_b() {
    let fixture = materialize_cross_root_fixture(RuntimeDefect::ProvenanceOnly);
    assert_ne!(fixture.producer_root, fixture.root);
    assert!(fixture.root.exists());
    assert!(
        !fixture.producer_root.exists(),
        "producer checkout A must not be available to the verifier"
    );
    let bundle = fixture.serialized_bundle();
    assert_eq!(
        Path::new(&bundle.execution.invocation.current_dir),
        fixture.producer_root
    );
    crate::verification::verify_bundle_integrity(&bundle, &fixture.root)
        .expect("serialized cross-root artifacts retain integrity");
    let authenticated = crate::verification::authenticate_bundle(
        &bundle,
        &fixture.root,
        crate::verification::BundleBudget::for_trusted("pr", "simulator")
            .expect("simulator bundle budget"),
        "simulator",
    )
    .expect("cross-root simulator artifacts authenticate");
    let (catalog, _) = crate::tests::loaded();
    crate::artifact_verify::compile::verify_compile_invocations(
        &bundle,
        &fixture.root,
        &catalog,
        &authenticated,
    )
    .expect("producer-root-A compilation verifies from aggregate root B");

    let diagnostics = verify_simulator_schedule(&bundle, &fixture.root)
        .expect("producer-root-A simulator provenance verifies from aggregate root B");
    assert!(diagnostics
        .iter()
        .any(|diagnostic| diagnostic.contains("did not run required profile raft-soak")));
}

#[test]
fn serialized_cross_root_provenance_rejects_adversarial_substitutions() {
    for (substitution, expected) in [
        (ProvenanceSubstitution::Package, "package_id"),
        (ProvenanceSubstitution::Source, "source path"),
        (ProvenanceSubstitution::TargetName, "found 0"),
        (ProvenanceSubstitution::TargetKind, "found 0"),
        (ProvenanceSubstitution::Executable, "exact release target"),
        (ProvenanceSubstitution::CompileRoot, "source contract"),
    ] {
        let fixture = materialize_cross_root_fixture(RuntimeDefect::ProvenanceOnly);
        fixture.substitute_provenance(substitution);
        let bundle = fixture.serialized_bundle();
        crate::verification::verify_bundle_integrity(&bundle, &fixture.root)
            .expect("substituted serialized artifact remains digest-bound");

        let error = match verify_simulator_schedule(&bundle, &fixture.root) {
            Ok(diagnostics) => {
                panic!("{substitution:?} substitution verified: {diagnostics:?}")
            }
            Err(error) => error,
        };
        assert!(
            error.to_string().contains(expected),
            "unexpected {substitution:?} error: {error}"
        );
    }
}
