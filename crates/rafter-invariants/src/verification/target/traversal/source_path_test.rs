//! Tracked module paths retain filesystem boundaries while resolving parent components.

use std::{collections::HashSet, fs, path::PathBuf};

use super::ModuleGraphCollector;

fn check(path: &str, tracked: bool) -> Result<PathBuf, String> {
    let directory = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(directory.path()).unwrap();
    fs::create_dir_all(root.join("src/node")).unwrap();
    fs::create_dir_all(root.join("tests/support")).unwrap();
    fs::write(root.join("tests/support/driver.rs"), "fn helper() {}\n").unwrap();
    let inventory = if tracked {
        HashSet::from([PathBuf::from("tests/support/driver.rs")])
    } else {
        HashSet::new()
    };
    ModuleGraphCollector::new("fixture", &root, &inventory, &[])
        .bound_source_path(&root.join(path))
        .map(|resolved| resolved.strip_prefix(&root).unwrap().to_owned())
}

#[test]
fn tracked_parent_module_path_resolves_to_its_bound_identity() {
    assert_eq!(
        check("src/node/../../tests/support/driver.rs", true).unwrap(),
        PathBuf::from("tests/support/driver.rs")
    );
}

#[test]
fn parent_module_path_cannot_resolve_untracked_source() {
    let error = check("src/node/../../tests/support/driver.rs", false).unwrap_err();
    assert!(error.contains("not tracked"), "{error}");
}

#[test]
fn parent_module_path_cannot_escape_and_reenter_the_checkout() {
    let directory = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(directory.path()).unwrap();
    fs::write(root.join("driver.rs"), "").unwrap();
    let inventory = HashSet::from([PathBuf::from("driver.rs")]);
    let escaped = root
        .join("..")
        .join(root.file_name().unwrap())
        .join("driver.rs");
    let error = ModuleGraphCollector::new("fixture", &root, &inventory, &[])
        .bound_source_path(&escaped)
        .unwrap_err();
    assert!(error.contains("bound source tree"), "{error}");
}

#[test]
fn canceled_path_component_must_be_an_existing_directory() {
    for path in [
        "missing/../tests/support/driver.rs",
        "tests/support/driver.rs/../driver.rs",
        "tests/support/driver.rs/.",
    ] {
        assert!(check(path, true).is_err(), "{path}");
    }
}

#[cfg(unix)]
#[test]
fn parent_components_cannot_hide_a_symlink_even_with_the_same_final_target() {
    let directory = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(directory.path()).unwrap();
    fs::create_dir(root.join("real")).unwrap();
    fs::write(root.join("driver.rs"), "").unwrap();
    std::os::unix::fs::symlink(root.join("real"), root.join("alias")).unwrap();
    let inventory = HashSet::from([PathBuf::from("driver.rs")]);
    let error = ModuleGraphCollector::new("fixture", &root, &inventory, &[])
        .bound_source_path(&root.join("alias/../driver.rs"))
        .unwrap_err();
    assert!(
        error.contains("alias") || error.contains("symlink"),
        "{error}"
    );
}
