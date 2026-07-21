//! Presentation and ownership ratchets for the storage reference implementation.

use std::{
    fs,
    path::{Path, PathBuf},
};

#[path = "support/readability.rs"]
mod readability_support;

use readability_support::FACADE_PATHS;

const STORAGE_SOURCE_ROOT: &str = "crates/rafter-storage/src";
const STORAGE_INTEGRATION_TEST_ROOT: &str = "crates/rafter-storage/tests";
const FACADE_TARGET_LINES: usize = 100;
const FACADE_HARD_LINES: usize = 225;
const SCENARIO_TARGET_LINES: usize = 400;
const SCENARIO_HARD_LINES: usize = 900;
const MAX_WARNINGS_TO_PRINT: usize = 20;
const PENDING_TRANSFER_FACADE_CANDIDATES: &[&str] = &[
    "crates/rafter-storage/src/raft_snapshot_store/pending_transfer.rs",
    "crates/rafter-storage/src/raft_snapshot_store/pending_transfer/mod.rs",
];

#[test]
fn storage_production_modules_begin_with_an_ownership_contract() {
    let workspace = workspace_root();
    let root = workspace.join(STORAGE_SOURCE_ROOT);
    let mut violations = Vec::new();

    for path in production_rust_files(&root) {
        if !begins_with_module_contract(&read(&path)) {
            violations.push(format!(
                "{} must begin with a `//!` ownership contract",
                display_path(&workspace, &path)
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "storage module-contract violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn storage_test_modules_begin_with_a_scenario_contract() {
    let workspace = workspace_root();
    let mut violations = Vec::new();

    for relative_root in [STORAGE_SOURCE_ROOT, STORAGE_INTEGRATION_TEST_ROOT] {
        for path in test_rust_files(&workspace.join(relative_root)) {
            if !begins_with_module_contract(&read(&path)) {
                violations.push(format!(
                    "{} must begin with a `//!` scenario or support contract",
                    display_path(&workspace, &path)
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "storage test-contract violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn storage_production_modules_keep_test_bodies_in_separate_files() {
    let workspace = workspace_root();
    let root = workspace.join(STORAGE_SOURCE_ROOT);
    let mut violations = Vec::new();

    for path in production_rust_files(&root) {
        let source = read(&path);
        let lines = source.lines().collect::<Vec<_>>();
        for (line_index, line) in lines.iter().enumerate() {
            if line.trim() != "#[cfg(test)]" {
                continue;
            }
            let Some((next_index, next)) = lines
                .iter()
                .enumerate()
                .skip(line_index + 1)
                .find(|(_, candidate)| !candidate.trim().is_empty())
            else {
                continue;
            };
            let next = next.trim_start();
            if next.starts_with("mod ") && next.contains('{') {
                violations.push(format!(
                    "{}:{} embeds a test module body; move it to a sibling test file",
                    display_path(&workspace, &path),
                    next_index + 1
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "embedded storage-test violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn storage_facades_remain_declarative() {
    let workspace = workspace_root();
    let mut violations = Vec::new();

    for relative in storage_facades(&workspace) {
        let source = read(&workspace.join(relative));
        for (line_index, line) in source.lines().enumerate() {
            if declares_implementation(line.trim_start()) {
                violations.push(format!(
                    "{relative}:{} contains implementation; move it behind the facade",
                    line_index + 1
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "storage facade violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn storage_facades_keep_tight_presentation_budgets() {
    let workspace = workspace_root();
    let mut warnings = Vec::new();
    let mut violations = Vec::new();

    for relative in storage_facades(&workspace) {
        let line_count = read(&workspace.join(relative)).lines().count();
        if line_count > FACADE_TARGET_LINES {
            warnings.push(format!(
                "{relative}:{line_count}: facade exceeds target of {FACADE_TARGET_LINES} lines"
            ));
        }
        if line_count > FACADE_HARD_LINES {
            violations.push(format!(
                "{relative}:{line_count}: facade exceeds hard limit of {FACADE_HARD_LINES} lines"
            ));
        }
    }

    print_warnings("storage facade-size targets", &warnings);
    assert!(
        violations.is_empty(),
        "storage facade-size violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn storage_scenarios_keep_focused_presentation_budgets() {
    let workspace = workspace_root();
    let mut warnings = Vec::new();
    let mut violations = Vec::new();

    for relative_root in [STORAGE_SOURCE_ROOT, STORAGE_INTEGRATION_TEST_ROOT] {
        for path in test_rust_files(&workspace.join(relative_root)) {
            let relative = display_path(&workspace, &path);
            let line_count = read(&path).lines().count();
            if line_count > SCENARIO_TARGET_LINES {
                warnings.push(format!(
                    "{relative}:{line_count}: scenario exceeds target of {SCENARIO_TARGET_LINES} lines"
                ));
            }
            if line_count > SCENARIO_HARD_LINES {
                violations.push(format!(
                    "{relative}:{line_count}: scenario exceeds hard limit of {SCENARIO_HARD_LINES} lines"
                ));
            }
        }
    }

    print_warnings("storage scenario-size targets", &warnings);
    assert!(
        violations.is_empty(),
        "storage scenario-size violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn storage_facades_map_the_reference_architecture() {
    let workspace = workspace_root();

    assert_declares(
        &workspace,
        "crates/rafter-storage/src/format/v1/mod.rs",
        &[
            "hard_state",
            "log_compaction",
            "log_entry",
            "pending_transfer",
            "snapshot",
            "snapshot_manifest",
            "snapshot_metadata",
        ],
    );
    assert_declares(
        &workspace,
        "crates/rafter-storage/src/raft_hard_state_store.rs",
        &["contract", "error", "file", "memory"],
    );
    assert_declares(
        &workspace,
        "crates/rafter-storage/src/raft_log_segment.rs",
        &[
            "continuity",
            "contract",
            "error",
            "file",
            "frames",
            "memory",
            "open",
            "rewrite",
            "state",
        ],
    );
    assert_declares(
        &workspace,
        "crates/rafter-storage/src/raft_snapshot_store.rs",
        &[
            "contract",
            "error",
            "file",
            "health",
            "in_memory",
            "inventory",
            "manifest",
            "open",
            "open_report",
            "pending_transfer",
            "publish",
            "source",
            "state",
            "validation",
        ],
    );
    assert_declares(
        &workspace,
        "crates/rafter-storage/src/raft_snapshot_store/inventory.rs",
        &["model", "prune", "scan"],
    );
    assert_declares(
        &workspace,
        pending_transfer_facade(&workspace),
        &[
            "body",
            "cleanup",
            "codec",
            "constants",
            "error",
            "filesystem",
            "manifest",
            "paths",
            "read",
            "status",
            "write",
        ],
    );

    for relative in [
        "crates/rafter-storage/src/format/v1/hard_state.rs",
        "crates/rafter-storage/src/format/v1/log_entry.rs",
        "crates/rafter-storage/src/format/v1/snapshot.rs",
        "crates/rafter-storage/src/file_store_ownership.rs",
        "crates/rafter-storage/src/raft_hard_state_store/file.rs",
        "crates/rafter-storage/src/raft_log_segment/file.rs",
        "crates/rafter-storage/src/raft_log_segment/rewrite.rs",
        "crates/rafter-storage/src/raft_snapshot_store/file.rs",
        "crates/rafter-storage/src/raft_snapshot_store/inventory/prune.rs",
        "crates/rafter-storage/src/raft_snapshot_store/publish.rs",
        "crates/rafter-storage/src/raft_snapshot_store/validation.rs",
        "crates/rafter-storage/src/durable_fs_test.rs",
        "crates/rafter-storage/src/file_node_stores_test.rs",
        "crates/rafter-storage/src/format/v1/log_entry_test.rs",
        "crates/rafter-storage/src/raft_log_segment/continuity_test.rs",
    ] {
        assert!(
            workspace.join(relative).is_file(),
            "storage architecture module is missing: {relative}"
        );
    }
}

#[test]
fn storage_compatibility_facades_route_to_version_one_grammars() {
    let workspace = workspace_root();

    for (relative, owner) in [
        (
            "crates/rafter-storage/src/raft_hard_state_codec.rs",
            "crate::format::v1::hard_state",
        ),
        (
            "crates/rafter-storage/src/raft_log_compaction.rs",
            "crate::format::v1::log_compaction",
        ),
        (
            "crates/rafter-storage/src/raft_log_entry_codec.rs",
            "crate::format::v1::log_entry",
        ),
        (
            "crates/rafter-storage/src/raft_snapshot_codec.rs",
            "crate::format::v1::snapshot",
        ),
    ] {
        assert!(
            read(&workspace.join(relative)).contains(owner),
            "{relative} must route through `{owner}`"
        );
    }

    for retired in [
        "crates/rafter-storage/src/raft_snapshot_codec/cursor.rs",
        "crates/rafter-storage/src/raft_snapshot_codec_test.rs",
    ] {
        assert!(
            !workspace.join(retired).exists(),
            "retired storage layout must not reappear: {retired}"
        );
    }
}

fn storage_facades(workspace: &Path) -> Vec<&'static str> {
    let mut facades = FACADE_PATHS
        .iter()
        .copied()
        .filter(|path| path.starts_with("crates/rafter-storage/"))
        .collect::<Vec<_>>();
    let pending = pending_transfer_facade(workspace);
    if !facades.contains(&pending) {
        facades.push(pending);
    }
    facades
}

fn pending_transfer_facade(workspace: &Path) -> &'static str {
    PENDING_TRANSFER_FACADE_CANDIDATES
        .iter()
        .copied()
        .find(|relative| workspace.join(relative).is_file())
        .expect("pending-transfer facade must exist as a file module or directory module")
}

fn assert_declares(workspace: &Path, relative: &str, modules: &[&str]) {
    let source = read(&workspace.join(relative));
    for module in modules {
        assert!(
            source.contains(&format!("mod {module};")),
            "{relative} must declare child module `{module}`"
        );
    }
}

fn print_warnings(label: &str, warnings: &[String]) {
    if warnings.is_empty() {
        return;
    }
    let rendered = warnings
        .iter()
        .take(MAX_WARNINGS_TO_PRINT)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    let omitted = warnings.len().saturating_sub(MAX_WARNINGS_TO_PRINT);
    if omitted == 0 {
        eprintln!("{label}:\n{rendered}");
    } else {
        eprintln!("{label}:\n{rendered}\n... {omitted} more warnings omitted");
    }
}

fn begins_with_module_contract(source: &str) -> bool {
    source
        .trim_start_matches(|character: char| {
            matches!(character, '\u{feff}' | '\n' | '\r' | '\t' | ' ')
        })
        .starts_with("//!")
}

fn declares_implementation(line: &str) -> bool {
    let mut declaration = strip_visibility(line.trim_start());
    while let Some(rest) = ["async ", "const ", "unsafe "]
        .into_iter()
        .find_map(|qualifier| declaration.strip_prefix(qualifier))
    {
        declaration = rest;
    }

    declaration.starts_with("fn ")
        || declaration.starts_with("impl ")
        || declaration.starts_with("impl<")
        || (declaration.starts_with("extern ") && declaration.contains(" fn "))
}

fn strip_visibility(line: &str) -> &str {
    if let Some(rest) = line.strip_prefix("pub ") {
        return rest;
    }
    if !line.starts_with("pub(") {
        return line;
    }

    line.find(')')
        .map_or(line, |closing| line[closing + 1..].trim_start())
}

fn test_rust_files(root: &Path) -> Vec<PathBuf> {
    rust_files(root)
        .into_iter()
        .filter(|path| is_test_source(path))
        .collect()
}

fn production_rust_files(root: &Path) -> Vec<PathBuf> {
    rust_files(root)
        .into_iter()
        .filter(|path| !is_test_source(path))
        .collect()
}

fn is_test_source(path: &Path) -> bool {
    let normalized = path.to_string_lossy().replace('\\', "/");
    normalized.contains("/tests/")
        || normalized.ends_with("/tests.rs")
        || normalized.ends_with("_test.rs")
        || normalized.ends_with("/test_support.rs")
}

fn rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_files(root, &mut files);
    files.sort();
    files
}

fn collect_rust_files(root: &Path, files: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(root).unwrap_or_else(|error| panic!("read {}: {error}", root.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry under {}: {error}", root.display()))
            .path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn display_path(workspace: &Path, path: &Path) -> String {
    path.strip_prefix(workspace)
        .unwrap_or(path)
        .display()
        .to_string()
        .replace('\\', "/")
}
