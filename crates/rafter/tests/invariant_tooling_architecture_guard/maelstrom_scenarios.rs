//! Scenarios: Maelstrom verification remains independent, typed, and identity-stable.

use std::collections::BTreeSet;

use super::architecture_support::{
    declared_test_names, display_path, is_test_module, read, workspace_root,
};

/// The bare substring the Maelstrom lane filters its selection with.
const MAELSTROM_FILTER: &str = "maelstrom";

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

/// The Maelstrom lane runs an exact-count, exact-name selection over the
/// `maelstrom` filter. A count cannot see a rename: c7f802f6 renamed a
/// binding-class scenario, the selection stayed 61 tests, the length assertion
/// that used to live above passed, and the lane's by-name check failed an hour
/// later. This derives the real membership from the module graph, so adding,
/// renaming, or removing a scenario fails here instead — and the set equality
/// subsumes the count it replaced.
#[test]
fn the_maelstrom_selection_matches_its_reviewed_inventory() {
    let root = workspace_root();
    let inventory_path = "verification/maelstrom-test-inventory.txt";
    let inventory = read(&root.join(inventory_path));
    let reviewed = inventory.lines().collect::<Vec<_>>();

    let mut canonical = reviewed.clone();
    canonical.sort_unstable();
    canonical.dedup();
    assert_eq!(
        reviewed, canonical,
        "{inventory_path} must be sorted and free of duplicates"
    );

    let declared = declared_maelstrom_selection(&root);
    let reviewed = reviewed
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let missing = declared.difference(&reviewed).collect::<Vec<_>>();
    let stale = reviewed.difference(&declared).collect::<Vec<_>>();
    assert!(
        missing.is_empty() && stale.is_empty(),
        "{inventory_path} disagrees with the compiled selection\n  missing: {missing:#?}\n  stale: {stale:#?}"
    );

    for pin in [
        ".github/workflows/ci.yml",
        "crates/rafter/tests/ci_test_inventory_contract.rs",
        "crates/rafter/tests/invariant_ci_contract/pr_scenarios.rs",
    ] {
        assert!(
            read(&root.join(pin)).contains(&format!(
                "scripts/cargo-test-exact {} {MAELSTROM_FILTER} --inventory {inventory_path}",
                declared.len()
            )),
            "{pin} must select exactly the {} reviewed Maelstrom scenarios by name",
            declared.len()
        );
    }
}

/// Every `#[test]` the bare `maelstrom` filter matches in the invariant crate,
/// named the way libtest names it. The filter is a substring of the whole test
/// name rather than a module namespace, so a scenario joins this selection
/// through its module path or through its own function name, and the sweep is
/// crate-wide rather than scoped to the Maelstrom directories.
fn declared_maelstrom_selection(root: &std::path::Path) -> BTreeSet<String> {
    let declared = declared_test_names(root);
    let gated = declared
        .iter()
        .filter(|(name, linux_only)| **linux_only && name.contains(MAELSTROM_FILTER))
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    assert!(
        gated.is_empty(),
        "this lane runs on Linux, so a `cfg(target_os = \"linux\")` scenario would join its \
         selection while this derivation drops it: {gated:#?}"
    );
    declared
        .into_iter()
        .filter(|(name, linux_only)| !*linux_only && name.contains(MAELSTROM_FILTER))
        .map(|(name, _)| name)
        .collect()
}
