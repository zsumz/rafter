//! Scenarios: artifact acceptance remains verification-owned and fail-closed.

use std::collections::BTreeSet;

use super::{
    architecture_support::{
        declares_implementation, read, starts_with_module_contract, workspace_root,
    },
    invariant_tooling::REVIEWED_DOMAIN_IMPORT_EXCEPTIONS,
};

const ARTIFACT_MODULES: &[&str] = &[
    "crates/rafter-invariants/src/verification/artifact/mod.rs",
    "crates/rafter-invariants/src/verification/artifact/verify.rs",
    "crates/rafter-invariants/src/verification/artifact/metrics.rs",
    "crates/rafter-invariants/src/verification/artifact/test_execution.rs",
    "crates/rafter-invariants/src/verification/artifact/compiler/mod.rs",
    "crates/rafter-invariants/src/verification/artifact/compiler/cargo_output.rs",
    "crates/rafter-invariants/src/verification/artifact/compiler/invocation.rs",
    "crates/rafter-invariants/src/verification/artifact/compiler/model.rs",
    "crates/rafter-invariants/src/verification/artifact/compiler/outcome.rs",
    "crates/rafter-invariants/src/verification/artifact/compiler/receipt.rs",
    "crates/rafter-invariants/src/verification/artifact/compiler/simulator.rs",
    "crates/rafter-invariants/src/verification/artifact/compiler/test_target.rs",
    "crates/rafter-invariants/src/verification/artifact/test_runner/mod.rs",
    "crates/rafter-invariants/src/verification/artifact/test_runner/detector.rs",
    "crates/rafter-invariants/src/verification/artifact/test_runner/environment.rs",
    "crates/rafter-invariants/src/verification/artifact/test_runner/invocation.rs",
    "crates/rafter-invariants/src/verification/artifact/test_runner/outcome.rs",
    "crates/rafter-invariants/src/verification/artifact/test_runner/policy.rs",
    "crates/rafter-invariants/src/verification/artifact/test_runner/registry.rs",
    "crates/rafter-invariants/src/verification/artifact/test_runner/runner.rs",
];

#[test]
fn artifact_acceptance_is_verification_owned_bounded_and_typed() {
    let root = workspace_root();
    for relative in ARTIFACT_MODULES {
        let source = read(&root.join(relative));
        assert!(
            starts_with_module_contract(&source),
            "{relative} needs a module contract"
        );
        assert!(
            source.lines().count() <= 300,
            "{relative} exceeded the 300-line production target"
        );
        assert!(
            !source.contains("crate::artifact_verify"),
            "{relative} depends on the retired artifact-verifier owner"
        );
    }
    assert!(std::hint::black_box(REVIEWED_DOMAIN_IMPORT_EXCEPTIONS).is_empty());

    let compiler =
        read(&root.join("crates/rafter-invariants/src/verification/artifact/compiler/model.rs"));
    assert!(compiler.contains("struct CompilationEvidence"));
    assert!(compiler.contains("failed_execution_ids: BTreeSet<String>"));
    let runner = read(
        &root.join("crates/rafter-invariants/src/verification/artifact/test_runner/runner.rs"),
    );
    assert!(runner.contains("compilation.failed_for(execution_id)"));

    let metrics = read(&root.join("crates/rafter-invariants/src/verification/artifact/metrics.rs"));
    for model in [
        "enum ProcessArtifactKind",
        "enum MetricScope",
        "EvidenceLayer::Tla",
    ] {
        assert!(metrics.contains(model), "typed metrics omitted {model}");
    }

    let detector =
        read(&root.join("crates/rafter-invariants/src/verification/simulator/detector.rs"));
    assert!(detector.contains("trait DetectorLogVerifier: Sync"));
    assert!(!detector.contains("type InvocationVerifier"));
    assert!(!detector.contains("fn new("));
}

#[test]
fn legacy_artifact_mounts_are_test_only_declarative_and_inventory_pinned() {
    let root = workspace_root();
    let library = read(&root.join("crates/rafter-invariants/src/lib.rs"));
    for module in [
        "artifact_verify",
        "artifact_verify_maelstrom",
        "artifact_verify_maelstrom_support",
        "artifact_verify_tla",
    ] {
        assert!(
            library.contains(&format!("#[cfg(test)]\nmod {module};")),
            "root artifact module {module} must remain test-only"
        );
    }
    for relative in [
        "crates/rafter-invariants/src/artifact_verify.rs",
        "crates/rafter-invariants/src/artifact_verify/compile.rs",
        "crates/rafter-invariants/src/artifact_verify/simulator.rs",
        "crates/rafter-invariants/src/artifact_verify/simulator_schedule.rs",
        "crates/rafter-invariants/src/artifact_verify/simulator_schedule/events.rs",
        "crates/rafter-invariants/src/artifact_verify/test_logs.rs",
        "crates/rafter-invariants/src/artifact_verify_maelstrom.rs",
        "crates/rafter-invariants/src/artifact_verify_maelstrom_support.rs",
        "crates/rafter-invariants/src/artifact_verify_tla.rs",
    ] {
        let source = read(&root.join(relative));
        assert!(starts_with_module_contract(&source));
        for (line_index, line) in source.lines().enumerate() {
            assert!(
                !declares_implementation(line.trim_start()),
                "{relative}:{} contains implementation",
                line_index + 1
            );
        }
    }
    for retired in [
        "crates/rafter-invariants/src/artifact_verify/resource_metrics.rs",
        "crates/rafter-invariants/src/artifact_verify/test_logs/detector.rs",
        "crates/rafter-invariants/src/artifact_verify/test_logs/environment.rs",
        "crates/rafter-invariants/src/artifact_verify/test_logs/invocation.rs",
        "crates/rafter-invariants/src/artifact_verify/test_logs/outcome.rs",
        "crates/rafter-invariants/src/artifact_verify/test_logs/policy.rs",
        "crates/rafter-invariants/src/artifact_verify/test_logs/registry.rs",
        "crates/rafter-invariants/src/artifact_verify/test_logs/runner.rs",
    ] {
        assert!(
            !root.join(retired).exists(),
            "legacy implementation returned at {retired}"
        );
    }

    let inventory = read(&root.join("verification/artifact-verifier-test-inventory.txt"));
    let identities = inventory.lines().collect::<Vec<_>>();
    assert_eq!(identities.len(), 61);
    assert!(identities
        .iter()
        .all(|identity| identity.starts_with("artifact_verify::")));
    assert_eq!(
        identities.iter().copied().collect::<BTreeSet<_>>().len(),
        identities.len(),
        "artifact verifier inventory contains duplicate identities"
    );
}
