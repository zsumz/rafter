//! Workspace discovery and source presentation helpers for architecture scenarios.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use crate::invariant_tooling::INVARIANT_SOURCE_ROOTS;

pub(crate) fn invariant_rust_files(root: &Path) -> Vec<PathBuf> {
    let output = Command::new("/usr/bin/git")
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
        ])
        .args(INVARIANT_SOURCE_ROOTS)
        .current_dir(root)
        .output()
        .expect("enumerate invariant-tooling Rust files");
    assert!(
        output.status.success(),
        "git ls-files failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut files = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| path.ends_with(b".rs"))
        .map(|path| root.join(String::from_utf8(path.to_vec()).unwrap()))
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    files.sort();
    files
}

pub(crate) fn rust_files(root: &Path) -> Vec<PathBuf> {
    if root.is_file() {
        assert_eq!(
            root.extension().and_then(|value| value.to_str()),
            Some("rs"),
            "modeled source file is not Rust: {}",
            root.display()
        );
        return vec![root.to_path_buf()];
    }
    assert!(
        root.is_dir(),
        "modeled source root does not exist: {}",
        root.display()
    );
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

pub(crate) fn legacy_verifier_references(root: &Path, files: &[PathBuf], namespace: &str) -> usize {
    files
        .iter()
        .filter(|path| is_legacy_verifier(&display_path(root, path)))
        .map(|path| read(path).matches(namespace).count())
        .sum()
}

pub(crate) fn is_test_module(path: &str) -> bool {
    path.contains("/tests/")
        || path.ends_with("/tests.rs")
        || path.ends_with("_test.rs")
        || path.ends_with("_tests.rs")
}

pub(crate) fn is_legacy_verifier(path: &str) -> bool {
    path.starts_with("crates/rafter-invariants/src/artifact_verify")
        || path.starts_with("crates/rafter-invariants/src/receipt")
        || path == "crates/rafter-invariants/src/aggregate.rs"
}

pub(crate) fn starts_with_module_contract(source: &str) -> bool {
    source
        .trim_start_matches(|character: char| {
            matches!(character, '\u{feff}' | '\n' | '\r' | '\t' | ' ')
        })
        .starts_with("//!")
}

pub(crate) fn declares_implementation(line: &str) -> bool {
    [
        "fn ",
        "pub fn ",
        "pub(crate) fn ",
        "pub(super) fn ",
        "struct ",
        "pub struct ",
        "enum ",
        "pub enum ",
        "trait ",
        "pub trait ",
        "impl ",
        "macro_rules! ",
    ]
    .iter()
    .any(|prefix| line.starts_with(prefix))
}

pub(crate) fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .to_path_buf()
}

pub(crate) fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

pub(crate) fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}
