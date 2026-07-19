//! Scenario: invariant tooling can only move toward its reviewed domain architecture.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

#[path = "support/invariant_tooling.rs"]
mod invariant_tooling;
#[path = "support/readability.rs"]
mod readability_support;

use invariant_tooling::{
    INVARIANT_DOMAINS, INVARIANT_SOURCE_ROOTS, MAX_FILES_WITHOUT_MODULE_CONTRACTS,
    MAX_LEGACY_VERIFIER_PRODUCER_REFERENCES, MAX_PRODUCTION_FILES_OVER_TARGET,
    MAX_TEST_FILES_OVER_TARGET, PRODUCTION_TARGET_LINES, TEST_TARGET_LINES,
};
use readability_support::{FACADE_PATHS, TEST_FACADE_PATHS};

#[test]
fn target_domain_dependencies_are_known_and_one_way() {
    let expected = [
        "contract",
        "evidence",
        "provenance",
        "execution",
        "plan",
        "producer",
        "verification",
        "verdict",
        "gate",
        "cli",
    ];
    assert_eq!(
        INVARIANT_DOMAINS
            .iter()
            .map(|domain| domain.name)
            .collect::<Vec<_>>(),
        expected
    );

    let positions = INVARIANT_DOMAINS
        .iter()
        .enumerate()
        .map(|(index, domain)| (domain.name, index))
        .collect::<BTreeMap<_, _>>();
    for (index, domain) in INVARIANT_DOMAINS.iter().enumerate() {
        for dependency in domain.may_depend_on {
            let dependency_index = positions
                .get(dependency)
                .unwrap_or_else(|| panic!("{} names unknown dependency {dependency}", domain.name));
            assert!(
                *dependency_index < index,
                "{} may only depend on an earlier domain; found {dependency}",
                domain.name
            );
        }
    }
    let producer = domain("producer");
    let verification = domain("verification");
    assert!(!producer.may_depend_on.contains(&"verification"));
    assert!(!verification.may_depend_on.contains(&"producer"));
}

#[test]
fn mature_invariant_facades_remain_declarative() {
    let root = workspace_root();
    let facades = invariant_facades();
    assert_eq!(facades, ["crates/rafter-invariants/src/registry_parse.rs"]);

    for relative in facades {
        let source = read(&root.join(relative));
        assert!(
            starts_with_module_contract(&source),
            "{relative} needs a `//!` contract"
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
    let mut production_over_target = 0;
    let mut tests_over_target = 0;
    let mut missing_contracts = 0;

    for path in &files {
        let relative = display_path(&root, path);
        let source = read(path);
        if !starts_with_module_contract(&source) {
            missing_contracts += 1;
        }
        if invariant_facades().contains(&relative.as_str()) {
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
fn producer_verifier_dependency_debt_only_shrinks() {
    let root = workspace_root();
    let files = invariant_rust_files(&root);
    let references = files
        .iter()
        .filter(|path| is_legacy_verifier(&display_path(&root, path)))
        .map(|path| read(path).matches("producer::").count())
        .sum::<usize>();
    assert!(
        references <= MAX_LEGACY_VERIFIER_PRODUCER_REFERENCES,
        "legacy verifier-to-producer references increased from {MAX_LEGACY_VERIFIER_PRODUCER_REFERENCES} to {references}"
    );

    assert_forbidden_import_absent(
        &root.join("crates/rafter-invariants/src/producer"),
        &["crate::verification", "crate::verdict"],
    );
    assert_forbidden_import_absent(
        &root.join("crates/rafter-invariants/src/verification"),
        &["crate::producer"],
    );
}

fn domain(name: &str) -> &'static invariant_tooling::InvariantDomain {
    INVARIANT_DOMAINS
        .iter()
        .find(|domain| domain.name == name)
        .unwrap()
}

fn invariant_facades() -> Vec<&'static str> {
    FACADE_PATHS
        .iter()
        .chain(TEST_FACADE_PATHS)
        .copied()
        .filter(|path| path.starts_with("crates/rafter-invariant"))
        .collect()
}

fn invariant_rust_files(root: &Path) -> Vec<PathBuf> {
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
        .collect::<Vec<_>>();
    files.sort();
    files
}

fn assert_forbidden_import_absent(source_root: &Path, forbidden: &[&str]) {
    if !source_root.is_dir() {
        return;
    }
    for path in rust_files(source_root) {
        let source = read(&path);
        for dependency in forbidden {
            assert!(
                !source.contains(dependency),
                "{} imports forbidden dependency {dependency}",
                path.display()
            );
        }
    }
}

fn rust_files(root: &Path) -> Vec<PathBuf> {
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
    files
}

fn is_test_module(path: &str) -> bool {
    path.contains("/tests/")
        || path.ends_with("/tests.rs")
        || path.ends_with("_test.rs")
        || path.ends_with("_tests.rs")
}

fn is_legacy_verifier(path: &str) -> bool {
    path.starts_with("crates/rafter-invariants/src/artifact_verify")
        || path.starts_with("crates/rafter-invariants/src/receipt")
        || path == "crates/rafter-invariants/src/aggregate.rs"
}

fn starts_with_module_contract(source: &str) -> bool {
    source
        .trim_start_matches(|character: char| {
            matches!(character, '\u{feff}' | '\n' | '\r' | '\t' | ' ')
        })
        .starts_with("//!")
}

fn declares_implementation(line: &str) -> bool {
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

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .to_path_buf()
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}
