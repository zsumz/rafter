//! Scenarios: transitional presentation and dependency debt can only shrink.

use super::{
    architecture_support::{
        assert_forbidden_domain_imports_absent, declared_module_graph, declares_implementation,
        display_path, invariant_rust_files, is_test_module, legacy_verifier_references, read,
        starts_with_module_contract, workspace_root,
    },
    invariant_tooling::{
        MAX_FILES_WITHOUT_MODULE_CONTRACTS, MAX_LEGACY_VERIFIER_PRODUCER_IMAGE_REFERENCES,
        MAX_LEGACY_VERIFIER_PRODUCER_REFERENCES, MAX_LEGACY_VERIFIER_RUST_TARGET_REFERENCES,
        MAX_PRODUCTION_FILES_OVER_TARGET, MAX_TEST_FILES_OVER_TARGET, PRODUCTION_TARGET_LINES,
        TEST_TARGET_LINES,
    },
    readability_support::{FACADE_PATHS, TEST_FACADE_PATHS},
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
fn invariant_tooling_presentation_debt_only_shrinks() {
    let root = workspace_root();
    let files = invariant_rust_files(&root);
    let facades = invariant_facades();
    let mut production_over_target = 0;
    let mut tests_over_target = 0;
    let mut missing_contracts = 0;

    for path in &files {
        let relative = display_path(&root, path);
        let source = read(path);
        if !starts_with_module_contract(&source) {
            missing_contracts += 1;
        }
        if facades.contains(&relative.as_str()) {
            continue;
        }
        let lines = source.lines().count();
        if is_test_module(&relative) {
            tests_over_target += usize::from(lines > TEST_TARGET_LINES);
        } else {
            production_over_target += usize::from(lines > PRODUCTION_TARGET_LINES);
        }
    }

    assert!(
        production_over_target <= MAX_PRODUCTION_FILES_OVER_TARGET,
        "invariant production files over {PRODUCTION_TARGET_LINES} lines increased from {MAX_PRODUCTION_FILES_OVER_TARGET} to {production_over_target}"
    );
    assert!(
        tests_over_target <= MAX_TEST_FILES_OVER_TARGET,
        "invariant test files over {TEST_TARGET_LINES} lines increased from {MAX_TEST_FILES_OVER_TARGET} to {tests_over_target}"
    );
    assert!(
        missing_contracts <= MAX_FILES_WITHOUT_MODULE_CONTRACTS,
        "invariant modules without `//!` contracts increased from {MAX_FILES_WITHOUT_MODULE_CONTRACTS} to {missing_contracts}"
    );
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

fn invariant_facades() -> Vec<&'static str> {
    FACADE_PATHS
        .iter()
        .chain(TEST_FACADE_PATHS)
        .copied()
        .filter(|path| path.starts_with("crates/rafter-invariant"))
        .collect()
}
