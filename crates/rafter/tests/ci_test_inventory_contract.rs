//! Scenario: targeted CI test lanes fail when their reviewed inventory changes.

use std::{
    fs,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::{
    os::unix::fs::PermissionsExt,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

#[test]
fn filtered_ci_test_lanes_declare_exact_nonzero_inventories() {
    let root = workspace_root();
    let ci = read(&root.join(".github/workflows/ci.yml"));
    for selection in [
        "scripts/cargo-test-exact 6 dependency_boundary --workspace",
        "scripts/cargo-test-exact 2 source_boundary -p rafter",
        "scripts/cargo-test-exact 1 file_size_guard -p rafter -- --nocapture",
        "scripts/cargo-test-exact 3 - -p rafter --test test_location_guard -- --nocapture",
        "scripts/cargo-test-exact 8 - -p rafter --test public_api_docs_guard -- --nocapture",
        "scripts/cargo-test-exact 1 - -p rafter-runtime --test replicated_kv_example -- --ignored --exact replicated_kv_process_per_node_tcp_survives_kill_restart --nocapture",
        "scripts/cargo-test-exact 53 execution::process::tests --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 18 producer::process::tests --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 4 producer::test_exec::detector_proof::tests --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 1 producer::test_exec::process_tests --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 6 detector_proof::tests --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 43 artifact_verify::detector_source::tests --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 21 artifact_verify::detector_source::adversarial_tests --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 14 artifact_verify::simulator::event_semantics_tests --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 7 artifact_verify::test_logs::detector_witness_tests --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 4 producer_image::tests --locked -p rafter-invariants -- --test-threads=1",
        "scripts/cargo-test-exact 1 - --locked -p rafter-invariants --test producer_reexec -- --test-threads=1",
        "scripts/cargo-test-exact 34 producer::tla_exec::mutation_tests --locked -p rafter-invariants --lib -- --ignored --test-threads=1",
        "scripts/cargo-test-exact 4 artifact_verify_tla::full_bundle_tests::serialized_tests --locked -p rafter-invariants --lib -- --ignored --test-threads=1",
        "scripts/cargo-test-exact 43 maelstrom --locked -p rafter-invariants",
    ] {
        assert!(ci.contains(selection), "CI omitted exact inventory: {selection}");
    }

    let nightly = read(&root.join(".github/workflows/nightly.yml"));
    assert!(nightly
        .contains("scripts/cargo-test-exact 1 multi_gigabyte -p rafter --release -- --ignored"));

    assert_eq!(
        direct_cargo_test_commands(&root),
        vec![(
            ".github/workflows/ci.yml".to_owned(),
            "cargo test --workspace".to_owned(),
        )],
        "targeted workflow tests must use scripts/cargo-test-exact"
    );
}

#[cfg(unix)]
#[test]
fn exact_inventory_runner_checks_before_executing() {
    let root = workspace_root();
    let fixture = Fixture::new();
    let fake_cargo = fixture.path.join("cargo");
    let log = fixture.path.join("cargo.log");
    fs::write(
        &fake_cargo,
        r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$CARGO_TEST_EXACT_LOG"
for arg in "$@"; do
    if [[ "$arg" == "--list" ]]; then
        printf '%s\n' 'family::alpha: test' 'family::beta: test'
        exit 0
    fi
done
printf '%s\n' 'test execution reached'
"#,
    )
    .unwrap();
    let mut permissions = fs::metadata(&fake_cargo).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&fake_cargo, permissions).unwrap();

    let success = run_fixture(&root, &fixture.path, &log, "2");
    assert!(success.status.success(), "{}", stderr(&success));
    assert_eq!(
        read(&log).lines().collect::<Vec<_>>(),
        [
            "test -p demo family -- --list --ignored",
            "test -p demo family -- --ignored",
        ]
    );

    fs::write(&log, "").unwrap();
    let mismatch = run_fixture(&root, &fixture.path, &log, "3");
    assert!(!mismatch.status.success());
    assert!(stderr(&mismatch).contains("matched 2 tests; expected exactly 3"));
    assert_eq!(
        read(&log).lines().collect::<Vec<_>>(),
        ["test -p demo family -- --list --ignored"]
    );
}

fn direct_cargo_test_commands(root: &Path) -> Vec<(String, String)> {
    let mut commands = Vec::new();
    for entry in fs::read_dir(root.join(".github/workflows")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|value| value.to_str()) != Some("yml") {
            continue;
        }
        for line in read(&path).lines() {
            let command = line.trim().strip_prefix("run: ").unwrap_or(line.trim());
            if command.starts_with("cargo test ") {
                commands.push((display_path(root, &path), command.to_owned()));
            }
        }
    }
    commands.sort();
    commands
}

#[cfg(unix)]
fn run_fixture(root: &Path, bin: &Path, log: &Path, expected: &str) -> std::process::Output {
    let system_path = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![bin.to_path_buf()];
    paths.extend(std::env::split_paths(&system_path));
    Command::new("bash")
        .arg(root.join("scripts/cargo-test-exact"))
        .args([expected, "family", "-p", "demo", "--", "--ignored"])
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("CARGO_TEST_EXACT_LOG", log)
        .output()
        .unwrap()
}

#[cfg(unix)]
fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
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

#[cfg(unix)]
static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
struct Fixture {
    path: PathBuf,
}

#[cfg(unix)]
impl Fixture {
    fn new() -> Self {
        let id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rafter-cargo-test-exact-{}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

#[cfg(unix)]
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
