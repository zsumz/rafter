//! Scenarios: transitional presentation and dependency debt can only shrink.

use super::{
    architecture_support::{
        assert_forbidden_domain_imports_absent, declared_module_graph, declares_implementation,
        invariant_rust_files, is_test_module, legacy_verifier_references, read,
        starts_with_module_contract, workspace_root,
    },
    invariant_tooling::{
        MAX_LEGACY_VERIFIER_PRODUCER_IMAGE_REFERENCES, MAX_LEGACY_VERIFIER_PRODUCER_REFERENCES,
        MAX_LEGACY_VERIFIER_RUST_TARGET_REFERENCES,
    },
};

const DECOMPOSED_FACADE_PATHS: &[&str] = &[
    "crates/rafter-invariants/src/verification/artifact/mod.rs",
    "crates/rafter-invariants/src/verification/detector_replay/artifact/mod.rs",
    "crates/rafter-invariants/src/verification/target/mod.rs",
];

#[test]
fn decomposed_verification_facades_remain_small_and_declarative() {
    let root = workspace_root();
    for relative in DECOMPOSED_FACADE_PATHS {
        let source = read(&root.join(relative));
        assert!(
            starts_with_module_contract(&source),
            "{relative} needs a module contract"
        );
        assert!(
            source.lines().count() <= 32,
            "{relative} grew beyond its 32-line facade budget"
        );
        for (line_index, line) in source.lines().enumerate() {
            assert!(
                !declares_implementation(line.trim_start()),
                "{relative}:{} contains implementation",
                line_index + 1
            );
        }
    }
}

#[test]
fn nested_scenario_directories_remain_test_owned() {
    for path in [
        "crates/example/src/tests/call_flow.rs",
        "crates/example/src/source_tests/call_flow.rs",
        "crates/example/src/source_module_graph_tests/compiler_boundaries.rs",
    ] {
        assert!(is_test_module(path), "{path} must remain test-owned");
    }
}

#[test]
fn producer_verifier_dependency_debt_only_shrinks() {
    let root = workspace_root();
    let modules = declared_module_graph(&root);
    let files = invariant_rust_files(&root);
    let references = legacy_verifier_references(&root, &files, "producer::");
    assert_eq!(
        references, MAX_LEGACY_VERIFIER_PRODUCER_REFERENCES,
        "legacy verifier-to-producer references returned at {references}"
    );
    let producer_image_references = legacy_verifier_references(&root, &files, "producer_image::");
    assert_eq!(
        producer_image_references, MAX_LEGACY_VERIFIER_PRODUCER_IMAGE_REFERENCES,
        "legacy verifier-to-producer-image references returned at {producer_image_references}"
    );
    let rust_target_references = legacy_verifier_references(&root, &files, "rust_target::");
    assert_eq!(
        rust_target_references, MAX_LEGACY_VERIFIER_RUST_TARGET_REFERENCES,
        "legacy verifier-to-rust-target references returned at {rust_target_references}"
    );

    assert_forbidden_domain_imports_absent(
        &root,
        &modules,
        "crates/rafter-invariants/src/producer",
        &["verification", "verdict"],
    );
    assert_forbidden_domain_imports_absent(
        &root,
        &modules,
        "crates/rafter-invariants/src/verification",
        &["producer"],
    );
}
