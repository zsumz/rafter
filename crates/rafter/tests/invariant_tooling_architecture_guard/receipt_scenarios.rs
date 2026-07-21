//! Scenarios: semantic receipt policy remains owned by verifier domains.

use super::{
    architecture_support::{
        declares_implementation, read, starts_with_module_contract, workspace_root,
    },
    invariant_tooling::REVIEWED_DOMAIN_IMPORT_EXCEPTIONS,
};

const RECEIPT_MODULES: &[&str] = &[
    "crates/rafter-invariants/src/verification/intake/receipt/mod.rs",
    "crates/rafter-invariants/src/verification/intake/receipt/checks.rs",
    "crates/rafter-invariants/src/verification/intake/receipt/collection.rs",
    "crates/rafter-invariants/src/verification/intake/receipt/execution.rs",
    "crates/rafter-invariants/src/verification/intake/receipt/runner.rs",
    "crates/rafter-invariants/src/verification/intake/receipt/structure.rs",
    "crates/rafter-invariants/src/verification/simulator/receipt.rs",
    "crates/rafter-invariants/src/verification/simulator/receipt/liveness.rs",
    "crates/rafter-invariants/src/verification/test_runner/mod.rs",
    "crates/rafter-invariants/src/verification/test_runner/receipt.rs",
];

#[test]
fn semantic_receipt_validation_is_verification_owned_and_bounded() {
    let root = workspace_root();
    for relative in RECEIPT_MODULES {
        let source = read(&root.join(relative));
        assert!(
            starts_with_module_contract(&source),
            "{relative} needs a module contract"
        );
        assert!(
            source.lines().count() <= 300,
            "{relative} exceeded the 300-line production target"
        );
    }
    for removed in [
        "crates/rafter-invariants/src/receipt_simulator.rs",
        "crates/rafter-invariants/src/receipt_tla.rs",
    ] {
        assert!(
            !root.join(removed).exists(),
            "legacy receipt implementation returned at {removed}"
        );
    }
    let intake = read(&root.join("crates/rafter-invariants/src/verification/intake/verify.rs"));
    assert!(!intake.contains("crate::receipt"));
    let dispatch =
        read(&root.join("crates/rafter-invariants/src/verification/intake/receipt/runner.rs"));
    for owner in [
        "verification::test_runner::validate_receipt",
        "verification::simulator::validate_receipt",
        "verification::tla::validate_receipt",
        "verification::maelstrom::validate_receipt",
    ] {
        assert!(dispatch.contains(owner), "receipt dispatch omitted {owner}");
    }
    assert!(dispatch.contains("match layer"));
    for layer in ["Tests", "Simulator", "Tla", "Maelstrom"] {
        assert!(
            dispatch.contains(&format!("EvidenceLayer::{layer}")),
            "receipt dispatch omitted typed layer {layer}"
        );
    }
    assert!(!dispatch.contains("bundle.runner.as_str()"));
    assert!(!dispatch.contains("_ =>"));
    assert_eq!(
        std::hint::black_box(REVIEWED_DOMAIN_IMPORT_EXCEPTIONS).len(),
        0,
        "completed verifier migrations must not retain architecture exceptions"
    );
}

#[test]
fn root_receipt_mounts_are_test_only_and_declarative() {
    let root = workspace_root();
    let library = read(&root.join("crates/rafter-invariants/src/lib.rs"));
    for module in ["receipt", "receipt_maelstrom", "receipt_tests"] {
        assert!(
            library.contains(&format!("#[cfg(test)]\nmod {module};")),
            "root receipt module {module} must remain test-only"
        );
    }
    for relative in [
        "crates/rafter-invariants/src/receipt.rs",
        "crates/rafter-invariants/src/receipt_maelstrom.rs",
        "crates/rafter-invariants/src/receipt_tests.rs",
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
    let test_mount = read(&root.join("crates/rafter-invariants/src/receipt_tests.rs"));
    assert!(
        test_mount.contains("#[path = \"verification/test_runner/tests/receipt.rs\"]\nmod tests;")
    );
    let scenario =
        read(&root.join("crates/rafter-invariants/src/verification/test_runner/tests/receipt.rs"));
    assert!(scenario.contains("fn nonpass_test_receipts_require_the_exact_status_matrix()"));
}
