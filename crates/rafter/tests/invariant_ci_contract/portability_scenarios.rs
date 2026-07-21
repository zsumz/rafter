//! Portability scenarios: CI starts from fresh local state without mutating runners.

use std::{
    collections::BTreeSet,
    fs,
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

use super::support::*;

const WORKFLOWS: [&str; 4] = [
    ".github/workflows/ci.yml",
    ".github/workflows/nightly.yml",
    ".github/workflows/weekly.yml",
    ".github/workflows/benchmarks.yml",
];

#[test]
fn repository_workflows_disable_checkout_credentials_and_artifact_overwrite() {
    let root = workspace_root();
    for relative in WORKFLOWS {
        let source = read(&root.join(relative));
        assert!(
            !source.contains("overwrite: true"),
            "{relative} must not permit artifact replacement"
        );
        for (index, step) in action_steps(&source, "actions/checkout@") {
            assert!(
                step.contains("persist-credentials: false"),
                "{relative}:{} checkout must not persist credentials",
                index + 1
            );
        }
    }
}

#[test]
fn hosted_jobs_use_explicit_supported_os_versions() {
    let root = workspace_root();
    for relative in WORKFLOWS {
        let source = read(&root.join(relative));
        for floating in ["runs-on: ubuntu-latest", "runs-on: macos-latest"] {
            assert!(
                !source.contains(floating),
                "{relative} contains floating hosted runner label {floating}"
            );
        }
    }
    let ci = read(&root.join(".github/workflows/ci.yml"));
    assert!(ci.contains("runs-on: ubuntu-24.04"));
    assert!(ci.contains("runs-on: macos-15"));
}

#[test]
fn every_tla_runtime_uses_the_exact_reviewed_jdk_build() {
    let root = workspace_root();
    for relative in [
        ".github/actions/setup-invariant-verifier/action.yml",
        ".github/workflows/ci.yml",
    ] {
        let source = read(&root.join(relative));
        for line in source.lines().filter(|line| line.contains("java-version:")) {
            assert_eq!(
                line.trim(),
                "java-version: \"21.0.11+10\"",
                "{relative} contains an unreviewed JDK identity"
            );
        }
        assert!(source.contains("architecture: x64"));
        assert!(source.contains("check-latest: false"));
    }
}

#[test]
fn every_invariant_producer_and_aggregate_uses_fresh_cargo_roots() {
    let root = workspace_root();
    for (workflow, jobs) in [
        (
            ".github/workflows/ci.yml",
            &[
                "invariants-tests",
                "invariants-launcher-macos",
                "invariants-simulator",
                "invariants-tla",
                "invariants-tla-validation",
                "invariants-maelstrom",
                "invariants-pr",
            ][..],
        ),
        (
            ".github/workflows/nightly.yml",
            &[
                "invariants-tests",
                "invariants-simulator",
                "invariants-tla",
                "invariants-maelstrom",
                "invariants-nightly",
            ][..],
        ),
        (
            ".github/workflows/weekly.yml",
            &[
                "invariants-tests",
                "invariants-simulator",
                "invariants-tla",
                "invariants-maelstrom",
                "invariants-weekly",
            ][..],
        ),
    ] {
        let source = read(&root.join(workflow));
        for job in jobs {
            let block = job_block(&source, job);
            let checkout = block.find("actions/checkout@").expect("checkout step");
            let isolation = block
                .find("./.github/actions/configure-invariant-cargo")
                .unwrap_or_else(|| panic!("{workflow} job {job} omitted Cargo isolation"));
            let setup = block
                .find("./.github/actions/setup-rust")
                .expect("Rust setup");
            let cache = block.find("Swatinem/rust-cache@").expect("Rust cache");
            let prefetch = block
                .find("cargo fetch --locked")
                .unwrap_or_else(|| panic!("{workflow} job {job} omitted locked registry prefetch"));
            assert!(
                checkout < isolation
                    && isolation < setup
                    && setup < cache
                    && cache < prefetch,
                "{workflow} job {job} must isolate Cargo before setup, cache restore, and locked registry prefetch"
            );
        }
    }
}

#[test]
fn invariant_jobs_do_not_restore_compiled_targets_or_cargo_binaries() {
    let root = workspace_root();
    for (workflow, jobs) in [
        (
            ".github/workflows/ci.yml",
            &[
                "invariants-tests",
                "invariants-launcher-macos",
                "invariants-simulator",
                "invariants-tla",
                "invariants-tla-validation",
                "invariants-maelstrom",
                "invariants-pr",
            ][..],
        ),
        (
            ".github/workflows/nightly.yml",
            &[
                "invariants-tests",
                "invariants-simulator",
                "invariants-tla",
                "invariants-maelstrom",
                "invariants-nightly",
            ][..],
        ),
        (
            ".github/workflows/weekly.yml",
            &[
                "invariants-tests",
                "invariants-simulator",
                "invariants-tla",
                "invariants-maelstrom",
                "invariants-weekly",
            ][..],
        ),
    ] {
        let source = read(&root.join(workflow));
        for job in jobs {
            let block = job_block(&source, job);
            let (_, cache) = action_steps(block, "Swatinem/rust-cache@")
                .into_iter()
                .next()
                .unwrap_or_else(|| panic!("{workflow} job {job} omitted Rust cache"));
            for setting in ["cache-targets: false", "cache-bin: false"] {
                assert!(
                    cache.contains(setting),
                    "{workflow} job {job} omitted {setting}"
                );
            }
        }
    }
}

#[test]
fn repository_workflows_use_exactly_the_reviewed_external_actions() {
    let root = workspace_root();
    let reviewed = read(&root.join("verification/github-actions.lock"))
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        reviewed.len(),
        read(&root.join("verification/github-actions.lock"))
            .lines()
            .filter(|line| !line.is_empty())
            .count(),
        "reviewed action lock contains a duplicate"
    );

    let mut observed = BTreeSet::new();
    for path in github_automation_sources(&root) {
        let relative = path
            .strip_prefix(&root)
            .expect("GitHub automation is under workspace root")
            .to_string_lossy();
        let source = read(&path);
        let specifications = external_action_specifications(&source)
            .unwrap_or_else(|error| panic!("{relative}: {error}"));
        for (line, specification) in specifications {
            let (_, revision) = specification
                .rsplit_once('@')
                .unwrap_or_else(|| panic!("{relative}:{line} action omits a revision"));
            assert!(
                revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "{relative}:{line} external action is not pinned to a full commit: {specification}"
            );
            observed.insert(specification);
        }
    }

    assert_eq!(
        observed, reviewed,
        "workflow action inventory differs from verification/github-actions.lock"
    );
}

#[test]
fn external_action_inventory_accepts_quoted_yaml_scalars() {
    let revision = "a".repeat(40);
    let source = format!(
        "steps:\n  - uses: 'actions/checkout@{revision}'\n  - uses: \"actions/upload-artifact@{revision}\"\n"
    );
    assert_eq!(
        external_action_specifications(&source).expect("parse quoted action scalars"),
        vec![
            (2, format!("actions/checkout@{revision}")),
            (3, format!("actions/upload-artifact@{revision}")),
        ]
    );
}

#[test]
fn external_action_inventory_parses_spaced_keys_and_rejects_flow_mappings() {
    let revision = "a".repeat(40);
    let source = format!("steps:\n  - uses : actions/checkout@{revision}\n");
    assert_eq!(
        external_action_specifications(&source).expect("parse spaced uses key"),
        vec![(2, format!("actions/checkout@{revision}"))]
    );
    assert!(external_action_specifications(&format!(
        "steps:\n  - {{ uses: actions/checkout@{revision} }}\n"
    ))
    .expect_err("flow mappings must fail closed")
    .contains("flow mapping"));
}

fn github_automation_sources(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut pending = vec![root.join(".github/workflows"), root.join(".github/actions")];
    let mut sources = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)
            .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()))
        {
            let path = entry.expect("GitHub automation directory entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if matches!(
                path.extension().and_then(|value| value.to_str()),
                Some("yml" | "yaml")
            ) {
                sources.push(path);
            }
        }
    }
    sources.sort();
    sources
}

#[test]
fn cargo_isolation_action_creates_once_and_rejects_stale_roots() {
    let root = workspace_root();
    let action = read(&root.join(".github/actions/configure-invariant-cargo/action.yml"));
    for required in [
        "${GITHUB_RUN_ID}-${GITHUB_JOB}-${GITHUB_RUN_ATTEMPT}",
        "$RUNNER_TEMP/rafter-invariants-cargo-$identity",
        "$RUNNER_TEMP/rafter-invariants-target-$identity",
        "[[ -e \"$root\" || -L \"$root\" ]]",
        "mkdir -m 0700 \"$root\"",
        "CARGO_HOME=$cargo_home",
        "CARGO_TARGET_DIR=$cargo_target",
        ">> \"$GITHUB_ENV\"",
    ] {
        assert!(
            action.contains(required),
            "Cargo isolation action omitted {required}"
        );
    }

    let script = composite_script(&action, "Create isolated invariant Cargo roots");
    let fixture = CargoIsolationFixture::new();
    let first = fixture.run(&script);
    assert_success(&first, "fresh invariant Cargo roots");
    assert!(fixture.cargo_home().is_dir());
    assert!(fixture.cargo_target().is_dir());
    let exports = read(&fixture.github_env);
    assert!(exports.contains(&format!("CARGO_HOME={}", fixture.cargo_home().display())));
    assert!(exports.contains(&format!(
        "CARGO_TARGET_DIR={}",
        fixture.cargo_target().display()
    )));

    let stale = fixture.run(&script);
    assert_failure(&stale, "preexisting invariant Cargo roots");
    assert!(String::from_utf8_lossy(&stale.stderr)
        .contains("refusing preexisting invariant Cargo root"));
}

#[test]
fn maelstrom_setup_preflights_tools_and_uses_a_fresh_extraction_root() {
    let root = workspace_root();
    let action = read(&root.join(".github/actions/setup-invariant-verifier/action.yml"));
    for required in [
        "command -v dot",
        "command -v gnuplot",
        "dot -V",
        "gnuplot --version",
        "${GITHUB_RUN_ID}-${GITHUB_JOB}-${GITHUB_RUN_ATTEMPT}",
        "$RUNNER_TEMP/rafter-maelstrom-$identity",
        "[[ -e \"$path\" || -L \"$path\" ]]",
        "tar -xjf \"$archive\" -C \"$install_root\"",
        "maelstrom_root=\"$install_root/maelstrom\"",
    ] {
        assert!(
            action.contains(required),
            "Maelstrom setup omitted {required}"
        );
    }
    for forbidden in ["apt-get", "sudo ", "-C \"$RUNNER_TEMP\""] {
        assert!(
            !action.contains(forbidden),
            "Maelstrom setup must not contain {forbidden}"
        );
    }
}

fn action_steps<'a>(source: &'a str, action: &str) -> Vec<(usize, &'a str)> {
    let lines = source.lines().collect::<Vec<_>>();
    let mut steps = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if !line.trim_start().starts_with("- uses: ") || !line.contains(action) {
            continue;
        }
        let indentation = line.len() - line.trim_start().len();
        let end = lines[index + 1..]
            .iter()
            .position(|candidate| {
                candidate.len() - candidate.trim_start().len() == indentation
                    && candidate.trim_start().starts_with("- ")
            })
            .map_or(lines.len(), |offset| index + 1 + offset);
        steps.push((
            index,
            &source[line_offset(source, index)..line_offset(source, end)],
        ));
    }
    steps
}

fn external_action_specifications(source: &str) -> Result<Vec<(usize, String)>, String> {
    let mut specifications = Vec::new();
    for (index, line) in source.lines().enumerate() {
        let Some(specification) = action_specification(line, index + 1)? else {
            continue;
        };
        if !specification.starts_with("./") {
            specifications.push((index + 1, specification));
        }
    }
    Ok(specifications)
}

fn action_specification(line: &str, line_number: usize) -> Result<Option<String>, String> {
    let trimmed = line.trim_start();
    if trimmed.starts_with('#') {
        return Ok(None);
    }
    let entry = trimmed.strip_prefix("- ").unwrap_or(trimmed).trim_start();
    if entry.starts_with('{') {
        if entry.contains("uses") {
            return Err(format!(
                "line {line_number} uses a flow mapping; action steps require block-style uses keys"
            ));
        }
        return Ok(None);
    }
    let value = ["uses", "'uses'", "\"uses\""]
        .into_iter()
        .find_map(|key| entry.strip_prefix(key))
        .and_then(|rest| rest.trim_start().strip_prefix(':'));
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim_start();
    if value.is_empty() {
        return Err(format!("line {line_number} has an empty uses value"));
    }
    Ok(Some(quoted_or_plain_scalar(value, line_number)?.to_owned()))
}

fn quoted_or_plain_scalar(value: &str, line_number: usize) -> Result<&str, String> {
    let quote = value
        .as_bytes()
        .first()
        .copied()
        .ok_or_else(|| format!("line {line_number} has an empty uses value"))?;
    if matches!(quote, b'\'' | b'"') {
        let end = value.as_bytes()[1..]
            .iter()
            .position(|byte| *byte == quote)
            .map(|offset| offset + 1)
            .ok_or_else(|| format!("line {line_number} has an unterminated quoted uses value"))?;
        let remainder = value[end + 1..].trim_start();
        if !remainder.is_empty() && !remainder.starts_with('#') {
            return Err(format!("line {line_number} has trailing uses syntax"));
        }
        return Ok(&value[1..end]);
    }
    value
        .split_whitespace()
        .next()
        .ok_or_else(|| format!("line {line_number} has an empty uses value"))
}

fn line_offset(source: &str, line: usize) -> usize {
    source.split_inclusive('\n').take(line).map(str::len).sum()
}

fn composite_script(action: &str, name: &str) -> String {
    let marker = format!("    - name: {name}\n");
    let step = action
        .split_once(&marker)
        .unwrap_or_else(|| panic!("composite step {name} is missing"))
        .1;
    let script = step
        .split_once("      run: |\n")
        .unwrap_or_else(|| panic!("composite step {name} omitted run script"))
        .1;
    script
        .lines()
        .take_while(|line| line.is_empty() || line.starts_with("        "))
        .map(|line| line.strip_prefix("        ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
}

static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

struct CargoIsolationFixture {
    root: std::path::PathBuf,
    github_env: std::path::PathBuf,
}

impl CargoIsolationFixture {
    fn new() -> Self {
        let id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "rafter-invariant-cargo-isolation-{}-{id}",
            std::process::id()
        ));
        fs::create_dir(&root).expect("create Cargo isolation fixture");
        let github_env = root.join("github-env");
        Self { root, github_env }
    }

    fn run(&self, script: &str) -> Output {
        Command::new("bash")
            .args(["-eu", "-o", "pipefail", "-c", script])
            .env("RUNNER_TEMP", &self.root)
            .env("GITHUB_RUN_ID", "42")
            .env("GITHUB_JOB", "invariants-tests")
            .env("GITHUB_RUN_ATTEMPT", "3")
            .env("GITHUB_ENV", &self.github_env)
            .output()
            .expect("execute Cargo isolation action")
    }

    fn cargo_home(&self) -> std::path::PathBuf {
        self.root
            .join("rafter-invariants-cargo-42-invariants-tests-3")
    }

    fn cargo_target(&self) -> std::path::PathBuf {
        self.root
            .join("rafter-invariants-target-42-invariants-tests-3")
    }
}

impl Drop for CargoIsolationFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
