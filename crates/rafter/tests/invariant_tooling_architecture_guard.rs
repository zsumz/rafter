//! Scenario: invariant tooling can only move toward its reviewed domain architecture.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};
use syn::{visit::Visit, ItemUse, UseTree};

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
    assert_eq!(
        facades,
        [
            "crates/rafter-invariant-test/src/lib.rs",
            "crates/rafter-invariant-test/src/detector/mod.rs",
            "crates/rafter-invariant-test/src/oracle/mod.rs",
            "crates/rafter-invariants/src/lib.rs",
            "crates/rafter-invariants/src/contract/mod.rs",
            "crates/rafter-invariants/src/contract/catalog/mod.rs",
            "crates/rafter-invariants/src/contract/profile/liveness/mod.rs",
            "crates/rafter-invariants/src/contract/profile/mod.rs",
            "crates/rafter-invariants/src/contract/profile/runner_contract/mod.rs",
            "crates/rafter-invariants/src/contract/registry/mod.rs",
            "crates/rafter-invariants/src/contract/registry/parse/mod.rs",
            "crates/rafter-invariants/src/contract/schema/mod.rs",
            "crates/rafter-invariants/src/evidence/liveness/mod.rs",
            "crates/rafter-invariants/src/evidence/mod.rs",
            "crates/rafter-invariants/src/evidence/receipt/mod.rs",
            "crates/rafter-invariants/src/producer/simulator/liveness/mod.rs",
            "crates/rafter-invariants/src/verification/mod.rs",
            "crates/rafter-invariants/src/verification/simulator/mod.rs",
            "crates/rafter-invariants/src/verification/simulator/liveness/mod.rs",
            "crates/rafter-invariants/src/verdict/mod.rs",
            "crates/rafter-invariants/src/contract/registry/parse/tests/mod.rs",
            "crates/rafter-invariants/src/verification/simulator/liveness/tests/mod.rs",
        ]
    );

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
fn modeled_invariant_domains_require_module_contracts_without_legacy_allowance() {
    let root = workspace_root();
    for relative in [
        "crates/rafter-invariants/src/contract",
        "crates/rafter-invariants/src/evidence",
        "crates/rafter-invariants/src/verdict",
        "crates/rafter-invariants/src/verification",
        "crates/rafter-invariants/src/producer/simulator/liveness",
    ] {
        for path in rust_files(&root.join(relative)) {
            assert!(
                starts_with_module_contract(&read(&path)),
                "{} needs a `//!` module contract",
                display_path(&root, &path)
            );
        }
    }
}

#[test]
fn implemented_domain_imports_follow_the_reviewed_dependency_graph() {
    let root = workspace_root();
    for name in ["contract", "evidence", "verification", "verdict"] {
        assert_domain_imports_follow_manifest(&root, name);
    }
}

#[test]
fn retired_internal_catalog_alias_cannot_return() {
    let root = workspace_root();
    for path in invariant_rust_files(&root) {
        let source = read(&path);
        assert!(
            !source.contains("crate::catalog"),
            "{} imports the retired internal catalog alias",
            display_path(&root, &path)
        );
    }
    assert!(
        !read(&root.join("crates/rafter-invariants/src/lib.rs"))
            .contains("pub(crate) use contract::catalog"),
        "the retired crate-root catalog alias returned"
    );
}

#[test]
fn liveness_wire_binding_cannot_absorb_raw_event_acceptance() {
    let root = workspace_root();
    let evidence = read(&root.join("crates/rafter-invariants/src/evidence/liveness/binding.rs"));
    for raw_acceptance in [
        "SimulatorIdentity",
        "expected_execution_contract",
        "LivenessReportError",
        "BTreeMap<String, Vec<Value>>",
        "validate_liveness_report",
    ] {
        assert!(
            !evidence.contains(raw_acceptance),
            "neutral evidence binding absorbed `{raw_acceptance}`"
        );
    }

    for relative in [
        "crates/rafter-invariants/src/producer/simulator/liveness/raw.rs",
        "crates/rafter-invariants/src/verification/simulator/liveness/raw.rs",
    ] {
        assert!(
            root.join(relative).is_file(),
            "missing independent {relative}"
        );
    }
}

#[test]
fn retired_flat_contract_files_cannot_return() {
    let root = workspace_root();
    for relative in [
        "crates/rafter-invariants/src/catalog.rs",
        "crates/rafter-invariants/src/registry.rs",
        "crates/rafter-invariants/src/registry_document.rs",
        "crates/rafter-invariants/src/registry_parse.rs",
        "crates/rafter-invariants/src/schema.rs",
        "crates/rafter-invariants/src/types.rs",
    ] {
        assert!(
            !root.join(relative).exists(),
            "retired flat contract file returned: {relative}"
        );
    }
}

#[test]
fn detector_test_macro_trust_is_bound_to_exact_domain_sources() {
    let root = workspace_root();
    let source = read(&root.join("crates/rafter-invariants/src/rust_target.rs"));

    for expected in [
        "crates/rafter-invariant-test/src/oracle/macros.rs",
        "crates/rafter-invariant-test/src/oracle/call.rs",
        "crates/rafter-invariant-test/src/detector/session.rs",
    ] {
        assert!(
            source.contains(expected),
            "missing exact trust path {expected}"
        );
    }
    assert!(
        !source.contains("Some(Path::new(\"crates/rafter-invariant-test/src/lib.rs\"))"),
        "the detector facade must not retain the old broad item-macro exception"
    );
}

#[test]
fn detector_proc_macro_root_is_a_thin_entrypoint() {
    let root = workspace_root();
    let relative = "crates/rafter-invariant-test-macros/src/lib.rs";
    let source = read(&root.join(relative));
    assert!(starts_with_module_contract(&source));
    assert!(
        source.lines().count() <= 20,
        "{relative} stopped being thin"
    );
    assert_eq!(source.matches("pub fn detector_test").count(), 1);
    for implementation_detail in ["parse_quote", "quote!", "ItemFn", "ReturnType"] {
        assert!(
            !source.contains(implementation_detail),
            "{relative} absorbed parser implementation `{implementation_detail}`"
        );
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

fn assert_domain_imports_follow_manifest(root: &Path, name: &str) {
    let owner = domain(name);
    let source_root = root.join("crates/rafter-invariants/src").join(name);
    for path in rust_files(&source_root) {
        let relative = display_path(root, &path);
        if is_test_module(&relative) {
            continue;
        }
        let source = read(&path);
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {relative} for dependency validation: {error}"));
        let mut imports = CrateImportCollector::default();
        imports.visit_file(&syntax);
        for import in imports.paths {
            let Some(dependency) = import.get(1) else {
                panic!("{relative} imports the crate root without a domain owner: {import:?}");
            };
            if dependency == name || owner.may_depend_on.contains(&dependency.as_str()) {
                continue;
            }
            if INVARIANT_DOMAINS
                .iter()
                .any(|domain| domain.name == dependency)
            {
                panic!("{relative} imports forbidden domain {dependency} via {import:?}");
            }
            panic!(
                "{relative} imports crate-root facade item {dependency} via {import:?}; import it from its owning domain"
            );
        }
    }
}

#[derive(Default)]
struct CrateImportCollector {
    paths: Vec<Vec<String>>,
}

impl<'ast> Visit<'ast> for CrateImportCollector {
    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        let mut paths = Vec::new();
        flatten_use_tree(&item.tree, Vec::new(), &mut paths);
        self.paths.extend(
            paths
                .into_iter()
                .filter(|path| path.first().is_some_and(|segment| segment == "crate")),
        );
        syn::visit::visit_item_use(self, item);
    }
}

fn flatten_use_tree(tree: &UseTree, prefix: Vec<String>, paths: &mut Vec<Vec<String>>) {
    match tree {
        UseTree::Path(path) => {
            let mut prefix = prefix;
            prefix.push(path.ident.to_string());
            flatten_use_tree(&path.tree, prefix, paths);
        }
        UseTree::Name(name) => {
            let mut path = prefix;
            path.push(name.ident.to_string());
            paths.push(path);
        }
        UseTree::Rename(rename) => {
            let mut path = prefix;
            path.push(rename.ident.to_string());
            paths.push(path);
        }
        UseTree::Glob(_) => {
            let mut path = prefix;
            path.push("*".to_owned());
            paths.push(path);
        }
        UseTree::Group(group) => {
            for tree in &group.items {
                flatten_use_tree(tree, prefix.clone(), paths);
            }
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
