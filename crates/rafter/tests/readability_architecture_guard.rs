use std::{
    fs,
    path::{Path, PathBuf},
};

#[path = "support/mutation_ownership.rs"]
mod mutation_ownership;
#[path = "support/readability.rs"]
mod readability_support;

use mutation_ownership::MUTATION_RULES;
use readability_support::{FACADE_PATHS, TEST_FACADE_PATHS};

#[test]
fn production_modules_begin_with_an_architectural_contract() {
    let workspace = workspace_root();
    let mut violations = Vec::new();

    for path in production_rust_files(&workspace.join("crates/rafter/src")) {
        let source = read(&path);
        if !source
            .trim_start_matches(|character: char| {
                matches!(character, '\u{feff}' | '\n' | '\r' | '\t' | ' ')
            })
            .starts_with("//!")
        {
            violations.push(format!(
                "{} must begin with a `//!` module contract",
                display_path(&workspace, &path)
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "module-contract violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn test_modules_begin_with_a_scenario_contract() {
    let workspace = workspace_root();
    let mut violations = Vec::new();

    for path in test_rust_files(&workspace.join("crates/rafter/src")) {
        let source = read(&path);
        if !source
            .trim_start_matches(|character: char| {
                matches!(character, '\u{feff}' | '\n' | '\r' | '\t' | ' ')
            })
            .starts_with("//!")
        {
            violations.push(format!(
                "{} must begin with a `//!` scenario contract",
                display_path(&workspace, &path)
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "test module-contract violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn production_modules_keep_test_bodies_in_separate_files() {
    let workspace = workspace_root();
    let mut violations = Vec::new();

    for path in production_rust_files(&workspace.join("crates/rafter/src")) {
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
        "embedded production-test violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn architectural_facades_remain_declarative() {
    let workspace = workspace_root();
    let mut violations = Vec::new();

    for relative in FACADE_PATHS {
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
        "facade architecture violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn mature_test_facades_remain_declarative() {
    let workspace = workspace_root();
    let mut violations = Vec::new();

    for relative in TEST_FACADE_PATHS {
        let source = read(&workspace.join(relative));
        for (line_index, line) in source.lines().enumerate() {
            if line.trim_start().starts_with("#[test]")
                || declares_implementation(line.trim_start())
            {
                violations.push(format!(
                    "{relative}:{} contains a scenario; move it behind the test facade",
                    line_index + 1
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "test-facade architecture violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn load_bearing_state_mutations_stay_with_their_owning_modules() {
    let workspace = workspace_root();
    let source_root = workspace.join("crates/rafter/src/node");
    let mut violations = Vec::new();

    for path in production_rust_files(&source_root) {
        let relative = display_path(&workspace.join("crates/rafter/src"), &path);
        let compact = compact_whitespace(&read(&path));

        for rule in MUTATION_RULES {
            if compact.contains(rule.token)
                && !rule.owners.iter().any(|owner| relative.ends_with(owner))
            {
                violations.push(format!(
                    "{} mutates `{}`; owners are {}",
                    relative,
                    rule.token,
                    rule.owners.join(", ")
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "state-mutation ownership violations:\n{}",
        violations.join("\n")
    );
}

const MATURE_TEST_MODULES: &[&str] = &[
    "crates/rafter/src/node/tests/bootstrap.rs",
    "crates/rafter/src/node/tests/bootstrap/application.rs",
    "crates/rafter/src/node/tests/bootstrap/snapshot.rs",
    "crates/rafter/src/node/tests/bootstrap/snapshot/hydration.rs",
    "crates/rafter/src/node/tests/config.rs",
    "crates/rafter/src/node/tests/config/features.rs",
    "crates/rafter/src/node/tests/config/validation.rs",
    "crates/rafter/src/node/tests/dispatch.rs",
    "crates/rafter/src/node/tests/election.rs",
    "crates/rafter/src/node/tests/election/campaign.rs",
    "crates/rafter/src/node/tests/election/voting.rs",
    "crates/rafter/src/node/tests/membership.rs",
    "crates/rafter/src/node/tests/membership/authority.rs",
    "crates/rafter/src/node/tests/membership/commit.rs",
    "crates/rafter/src/node/tests/membership/learner.rs",
    "crates/rafter/src/node/tests/read.rs",
    "crates/rafter/src/node/tests/read/barrier.rs",
    "crates/rafter/src/node/tests/read/lease.rs",
    "crates/rafter/src/node/tests/replication.rs",
    "crates/rafter/src/node/tests/replication/leader.rs",
    "crates/rafter/src/node/tests/replication/leader/proposal.rs",
    "crates/rafter/src/node/tests/replication/pipelining.rs",
    "crates/rafter/src/node/tests/replication/pipelining/window.rs",
    "crates/rafter/src/node/tests/snapshot.rs",
    "crates/rafter/src/node/tests/snapshot/chunks.rs",
    "crates/rafter/src/node/tests/snapshot/chunks/receive.rs",
    "crates/rafter/src/node/tests/snapshot/install.rs",
    "crates/rafter/src/node/tests/snapshot/install/follower.rs",
    "crates/rafter/src/node/tests/snapshot/streaming.rs",
    "crates/rafter/src/node/tests/snapshot/streaming/support.rs",
    "crates/rafter/src/node/tests/transfer.rs",
    "crates/rafter/src/node/tests/transfer/handoff.rs",
    "crates/rafter/src/message/shared_entries_test.rs",
    "crates/rafter/src/node/commit/tracker_test.rs",
    "crates/rafter/src/node/replication/response_test.rs",
    "crates/rafter/src/node/state/derived_test.rs",
    "crates/rafter/src/node/state/election_test.rs",
    "crates/rafter/src/node/state/proposal_test.rs",
    "crates/rafter/src/types/id_test.rs",
    "crates/rafter/src/types/payload_test.rs",
    "crates/rafter/src/types/snapshot/tests.rs",
];

#[test]
fn mature_test_domains_mirror_the_source_tree() {
    let workspace = workspace_root();

    for &relative in MATURE_TEST_MODULES {
        assert!(
            workspace.join(relative).is_file(),
            "mirrored test module is missing: {relative}"
        );
    }

    let node_test_facade = read(&workspace.join("crates/rafter/src/node/tests.rs"));
    for module in [
        "bootstrap",
        "config",
        "dispatch",
        "election",
        "membership",
        "read",
        "replication",
        "snapshot",
        "transfer",
    ] {
        assert!(
            node_test_facade.contains(&format!("mod {module};")),
            "node test facade must declare `{module}`"
        );
    }

    let sibling_test_modules = [
        (
            "crates/rafter/src/message/mod.rs",
            "mod shared_entries_test;",
        ),
        ("crates/rafter/src/node/commit/mod.rs", "mod tracker_test;"),
        (
            "crates/rafter/src/node/replication/mod.rs",
            "mod response_test;",
        ),
        ("crates/rafter/src/node/state.rs", "mod derived_test;"),
        ("crates/rafter/src/node/state.rs", "mod election_test;"),
        ("crates/rafter/src/node/state.rs", "mod proposal_test;"),
        ("crates/rafter/src/types/mod.rs", "mod id_test;"),
        ("crates/rafter/src/types/mod.rs", "mod payload_test;"),
    ];
    for (facade, declaration) in sibling_test_modules {
        assert!(
            read(&workspace.join(facade)).contains(declaration),
            "{facade} must declare sibling test module `{declaration}`"
        );
    }

    let snapshot_facade = read(&workspace.join("crates/rafter/src/types/snapshot/mod.rs"));
    assert!(snapshot_facade.contains("mod tests;"));

    for retired in [
        "crates/rafter/src/node/tests/read_index.rs",
        "crates/rafter/src/node/tests/read_lease.rs",
        "crates/rafter/src/node/tests/replication_snapshot_chunks.rs",
        "crates/rafter/src/node/tests/replication_snapshot_streaming.rs",
        "crates/rafter/src/node/tests/replication_snapshot_support.rs",
        "crates/rafter/src/node/tests/replication_snapshots.rs",
        "crates/rafter/src/node/tests/replication_pipelining.rs",
    ] {
        assert!(
            !workspace.join(retired).exists(),
            "retired test layout must not reappear: {retired}"
        );
    }
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
    let mut files = Vec::new();
    collect_rust_files(root, &mut files);
    files.retain(|path| !is_production_source(path));
    files.sort();
    files
}

fn production_rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_files(root, &mut files);
    files.retain(|path| is_production_source(path));
    files.sort();
    files
}

fn is_production_source(path: &Path) -> bool {
    let normalized = path.to_string_lossy().replace('\\', "/");
    !normalized.contains("/tests/")
        && !normalized.ends_with("/tests.rs")
        && !normalized.ends_with("_test.rs")
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

fn compact_whitespace(source: &str) -> String {
    source
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
        .replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn implementation_detector_accepts_vocabulary_and_rejects_functions() {
        assert!(!declares_implementation("pub struct Node {"));
        assert!(!declares_implementation(
            "pub(in crate::node) use send::ReplicationDemand;"
        ));
        assert!(declares_implementation("fn transition() {}"));
        assert!(declares_implementation("pub const fn transition() {}"));
        assert!(declares_implementation(
            "pub(in crate::node) const fn transition() {}"
        ));
        assert!(declares_implementation("impl<T> Facade<T> {}"));
        assert!(declares_implementation(
            "pub extern \"C\" fn transition() {}"
        ));
    }

    #[test]
    fn test_sources_are_not_production_modules() {
        assert!(!is_production_source(Path::new(
            "crates/rafter/src/node/tests/election.rs"
        )));
        assert!(!is_production_source(Path::new(
            "crates/rafter/src/types/membership_test.rs"
        )));
        assert!(is_production_source(Path::new(
            "crates/rafter/src/node/election.rs"
        )));
    }
}
