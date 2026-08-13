//! Scenarios: Maelstrom verification remains independent, typed, and identity-stable.

use super::architecture_support::{display_path, is_test_module, read, workspace_root};

#[test]
fn maelstrom_acceptance_is_verifier_owned_without_producer_policy_edges() {
    let root = workspace_root();
    let verifier = read(&root.join("crates/rafter-invariants/src/verification/maelstrom/mod.rs"));
    for module in [
        "artifact",
        "configuration",
        "invocation",
        "lease",
        "observation",
        "receipt",
        "scenario",
        "status",
        "verify",
    ] {
        assert!(
            verifier.contains(&format!("mod {module};")),
            "Maelstrom verifier omitted domain module {module}"
        );
    }

    let verifier_root = root.join("crates/rafter-invariants/src/verification/maelstrom");
    for path in super::architecture_support::rust_files(&verifier_root) {
        if is_test_module(&display_path(&root, &path)) {
            continue;
        }
        let source = read(&path);
        assert!(
            !source.contains("crate::producer") && !source.contains("producer::"),
            "{} crossed from verification into producer policy",
            path.display()
        );
    }
}

#[test]
fn maelstrom_lease_and_history_semantics_have_independent_policy_owners() {
    let root = workspace_root();
    let producer_transcript = read(
        &root.join("crates/rafter-invariants/src/producer/maelstrom/trial/lease/transcript.rs"),
    );
    let producer_history =
        read(&root.join("crates/rafter-invariants/src/producer/maelstrom/trial/lease/history.rs"));
    let verifier_transcript =
        read(&root.join("crates/rafter-invariants/src/verification/maelstrom/lease/sequence.rs"));
    let verifier_history =
        read(&root.join("crates/rafter-invariants/src/verification/maelstrom/lease/history.rs"));

    assert!(producer_transcript.contains("fn validate_lease_transcript"));
    assert!(producer_history.contains("fn probe_completion_count"));
    assert!(verifier_transcript.contains("fn rederive"));
    assert!(verifier_history.contains("fn completion_count_with_limits"));
    assert!(!producer_transcript.contains("verification::maelstrom"));
    assert!(!verifier_transcript.contains("producer::maelstrom"));

    let compatibility = read(&root.join("crates/rafter-invariants/src/producer/maelstrom_exec.rs"));
    let history_compatibility =
        read(&root.join("crates/rafter-invariants/src/producer/maelstrom_exec/lease_history.rs"));
    assert!(compatibility.contains("maelstrom/lease_transcript_tests.rs"));
    assert!(history_compatibility.contains("lease_history_tests.rs"));
    assert!(!compatibility.contains("fn validate_lease_transcript"));
    assert!(!history_compatibility.contains("fn probe_completion_count"));
}

#[test]
fn neutral_maelstrom_formats_cannot_absorb_acceptance_policy() {
    let root = workspace_root();
    for relative in [
        "crates/rafter-invariants/src/evidence/format/maelstrom.rs",
        "crates/rafter-invariants/src/evidence/format/java.rs",
    ] {
        let source = read(&root.join(relative));
        for forbidden in [
            "EvidenceStatus",
            "CheckCompletion",
            "LeaseArtifactStatus",
            "trial_floors_met",
            "valid_counterexample_attribution",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} absorbed verifier policy {forbidden}"
            );
        }
    }
}

#[test]
fn maelstrom_compatibility_mounts_preserve_ci_test_identities() {
    let root = workspace_root();
    let mounts = [
        (
            "crates/rafter-invariants/src/artifact_verify_maelstrom.rs",
            "verification/maelstrom/tests/status.rs",
        ),
        (
            "crates/rafter-invariants/src/artifact_verify_maelstrom_support.rs",
            "verification/maelstrom/tests/lease.rs",
        ),
        (
            "crates/rafter-invariants/src/receipt_maelstrom.rs",
            "verification/maelstrom/tests/receipt.rs",
        ),
        (
            "crates/rafter-invariants/src/lib.rs",
            "verification/maelstrom/tests/full_bundle.rs",
        ),
    ];
    for (facade, test_path) in mounts {
        assert!(
            read(&root.join(facade)).contains(test_path),
            "{facade} no longer mounts stable Maelstrom test path {test_path}"
        );
    }

    let inventory = read(&root.join("verification/maelstrom-test-inventory.txt"));
    let names = inventory.lines().collect::<Vec<_>>();
    assert_eq!(names.len(), 61, "Maelstrom inventory must pin 61 names");
    let mut canonical = names.clone();
    canonical.sort_unstable();
    canonical.dedup();
    assert_eq!(
        names, canonical,
        "Maelstrom inventory must be sorted and unique"
    );
    for prefix in [
        "artifact_verify_maelstrom::tests::",
        "artifact_verify_maelstrom_support::tests::",
        "artifact_verify_maelstrom_tests::",
        "receipt_maelstrom::tests::",
    ] {
        assert!(
            names.iter().any(|name| name.starts_with(prefix)),
            "Maelstrom inventory omitted stable module identity {prefix}"
        );
    }

    let full_bundle = read(
        &root.join("crates/rafter-invariants/src/verification/maelstrom/tests/full_bundle.rs"),
    );
    for fragment in [
        "scenarios.inc",
        "serialized_fixture.inc",
        "bundle_fixture.inc",
    ] {
        assert!(
            full_bundle.contains(&format!("include!(\"full_bundle/{fragment}\")")),
            "full-bundle facade omitted {fragment}"
        );
        let source = read(&root.join(format!(
            "crates/rafter-invariants/src/verification/maelstrom/tests/full_bundle/{fragment}"
        )));
        assert!(
            source.lines().count() <= 400,
            "Maelstrom full-bundle fragment {fragment} exceeded 400 lines"
        );
    }
}
