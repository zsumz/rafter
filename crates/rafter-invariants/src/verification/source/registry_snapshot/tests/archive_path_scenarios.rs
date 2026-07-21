//! Adversarial archive path normalization scenarios.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use super::super::extract;

#[test]
fn archive_paths_reject_wrong_roots_and_traversal() {
    assert!(
        extract::package_relative(Path::new("other-1.2.3/src/lib.rs"), "sample-1.2.3")
            .expect_err("wrong package root")
            .contains("does not match")
    );
    assert!(
        extract::package_relative(Path::new("sample-1.2.3/../outside"), "sample-1.2.3")
            .expect_err("parent traversal")
            .contains("traversal")
    );
}

#[test]
fn archive_paths_reject_case_collisions_in_parent_components() {
    let mut seen = BTreeSet::new();
    let mut folded = BTreeMap::new();
    extract::require_unique_path(Path::new("Source/one.rs"), &mut seen, &mut folded)
        .expect("first path is unique");

    let error = extract::require_unique_path(Path::new("source/two.rs"), &mut seen, &mut folded)
        .expect_err("case-colliding parent must fail closed");

    assert!(error.contains("case-colliding paths"), "{error}");
}
