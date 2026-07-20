//! Scenarios: tracked source paths are canonical, relative, and index-backed.

use std::path::{Component, Path};

use super::{parse_tracked_source_paths, tracked_source_paths_at};

#[test]
fn raw_inventory_preserves_path_whitespace() {
    let paths = parse_tracked_source_paths(" leading.rs\0trailing.rs \0")
        .expect("parse raw NUL-delimited Git inventory");
    assert!(paths.contains(Path::new(" leading.rs")));
    assert!(paths.contains(Path::new("trailing.rs ")));
}

#[test]
fn raw_inventory_rejects_noncanonical_and_escaping_paths() {
    for path in [
        "/absolute.rs",
        "../outside.rs",
        "src/../outside.rs",
        "./local.rs",
    ] {
        let error = parse_tracked_source_paths(&format!("{path}\0"))
            .expect_err("noncanonical tracked paths fail closed");
        assert!(error.to_string().contains("non-relative tracked path"));
    }
}

#[test]
fn real_workspace_inventory_is_relative_and_index_backed() {
    let root = std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .expect("canonical workspace");
    let paths = tracked_source_paths_at(&root).expect("read the real Git index");

    for path in &paths {
        assert!(!path.is_absolute(), "{} is absolute", path.display());
        assert!(
            path.components()
                .all(|component| matches!(component, Component::Normal(_))),
            "{} is not canonical",
            path.display()
        );
    }
    for expected in [
        Path::new("Cargo.toml"),
        Path::new("crates/rafter-invariants/src/provenance/source/tracked.rs"),
    ] {
        assert!(
            paths.contains(expected),
            "Git index omitted {}",
            expected.display()
        );
    }
}
