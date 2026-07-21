//! Minimal source checkout used by simulator provenance fixtures.

use std::{fs, path::Path, process::Command};

use super::{io::git, model::RuntimeDefect};

const SIMULATOR_FIXTURE_SOURCE: &str = r##"use std::{
    io::{self, Write as _},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

static TERMINATED: AtomicBool = AtomicBool::new(false);

unsafe extern "C" {
    fn signal(signal: i32, handler: extern "C" fn(i32)) -> usize;
}

extern "C" fn handle_term(_signal: i32) {
    TERMINATED.store(true, Ordering::SeqCst);
}

fn main() {
    unsafe {
        let _ = signal(15, handle_term);
    }
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    writeln!(stdout, "{}", "RAFTER_FIXTURE_READY").expect("write readiness marker");
    writeln!(stdout, "{}", r#"RAFTER_EVENT __EVENT__"#)
        .expect("write semantic event");
__MALFORMED_EVENT__
    stdout.flush().expect("flush fixture output");
    drop(stdout);
__TERMINATION_WAIT__
}
"##;

pub(super) fn materialize_fixture_checkout(workspace: &Path, root: &Path, defect: RuntimeDefect) {
    fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"crates/rafter\", \"crates/rafter-sim\", \"crates/rafter-invariant-test\", \"crates/rafter-invariant-test-macros\"]\nresolver = \"2\"\n",
    )
    .expect("write fixture workspace manifest");
    let rafter_dir = root.join("crates/rafter");
    fs::create_dir_all(&rafter_dir).expect("create fixture rafter package");
    fs::write(
        rafter_dir.join("Cargo.toml"),
        "[package]\nname = \"rafter\"\nversion = \"0.0.1\"\nedition = \"2021\"\n\n[dev-dependencies]\nrafter-invariant-test = { path = \"../rafter-invariant-test\" }\n",
    )
    .expect("write fixture rafter manifest");
    copy_source_tree(
        &workspace.join("crates/rafter/src"),
        &rafter_dir.join("src"),
    );
    let oracle_dir = root.join("crates/rafter-invariant-test");
    fs::create_dir_all(oracle_dir.join("src")).expect("create fixture oracle package");
    fs::write(
        oracle_dir.join("Cargo.toml"),
        "[package]\nname = \"rafter-invariant-test\"\nversion = \"0.0.1\"\nedition = \"2021\"\n\n[dependencies]\nrafter-invariant-test-macros = { path = \"../rafter-invariant-test-macros\" }\n",
    )
    .expect("write fixture oracle manifest");
    fs::write(oracle_dir.join("src/lib.rs"), "").expect("write fixture oracle library source");
    let macros_dir = root.join("crates/rafter-invariant-test-macros");
    fs::create_dir_all(macros_dir.join("src")).expect("create fixture oracle macros package");
    fs::write(
        macros_dir.join("Cargo.toml"),
        "[package]\nname = \"rafter-invariant-test-macros\"\nversion = \"0.0.1\"\nedition = \"2021\"\n\n[lib]\nproc-macro = true\n",
    )
    .expect("write fixture oracle macros manifest");
    fs::write(macros_dir.join("src/lib.rs"), "").expect("write fixture oracle macros source");
    let package_dir = root.join("crates/rafter-sim");
    fs::create_dir_all(package_dir.join("src/bin")).expect("create fixture package source tree");
    fs::write(
        package_dir.join("Cargo.toml"),
        concat!(
            "[package]\n",
            "name = \"rafter-sim\"\n",
            "version = \"0.0.1\"\n",
            "edition = \"2021\"\n\n",
            "autolib = false\n\n",
            "[[bin]]\n",
            "name = \"rafter-model-check-fast\"\n",
            "path = \"src/bin/rafter-model-check-fast.rs\"\n",
        ),
    )
    .expect("write fixture package manifest");
    copy_source_tree(
        &workspace.join("crates/rafter-sim/src"),
        &package_dir.join("src"),
    );
    fs::write(
        package_dir.join("src/bin/rafter-model-check-fast.rs"),
        simulator_fixture_source(defect),
    )
    .expect("write fixture simulator source");
    fs::write(root.join(".gitignore"), "/artifacts/\n/target/\n")
        .expect("ignore generated fixture evidence");
    let environment = crate::producer::process::base_environment();
    let output = Command::new("cargo")
        .args(["generate-lockfile", "--offline"])
        .env_clear()
        .envs(environment)
        .current_dir(root)
        .output()
        .expect("generate fixture Cargo.lock");
    assert!(
        output.status.success(),
        "generate fixture Cargo.lock failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn copy_source_tree(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("create copied source directory");
    for entry in fs::read_dir(source).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type().expect("read source entry type").is_dir() {
            copy_source_tree(&source_path, &destination_path);
        } else {
            fs::copy(&source_path, &destination_path).expect("copy source fixture file");
        }
    }
}

fn simulator_fixture_source(defect: RuntimeDefect) -> String {
    let event = if matches!(defect, RuntimeDefect::PassExitOne) {
        serde_json::json!({
            "event": "exhaustive-check",
            "check_id": "raft-commit",
            "status": "pass",
            "unique_protocol_states": 20_000,
            "unique_verifier_states": 20_000,
            "observations": {
                "commit_floor_advances": 1,
                "commit_index_within_local_log_bounds_checks": 1,
            },
        })
    } else {
        serde_json::json!({
            "event": "check-failure",
            "event_version": 2,
            "check_id": "raft-commit",
            "status": "fail",
            "classification": "invariant-violation",
            "invariant_id": "CM-02",
            "invariant": "CM-02 commit requires effective quorum",
            "message": if matches!(defect, RuntimeDefect::CounterexampleExitOne) {
                "real exit-one fixture found a counterexample"
            } else {
                "real timeout fixture found a counterexample"
            },
            "unique_protocol_states": 1,
            "unique_verifier_states": 1,
        })
    };
    let malformed = if matches!(defect, RuntimeDefect::MalformedEvent) {
        "    writeln!(stdout, \"{}\", \"RAFTER_EVENT {not-json}\")\n        .expect(\"write malformed event\");"
    } else {
        ""
    };
    let termination_wait = match defect {
        RuntimeDefect::Timeout | RuntimeDefect::MalformedEvent => concat!(
            "    while !TERMINATED.load(Ordering::SeqCst) {\n",
            "        thread::sleep(Duration::from_millis(10));\n",
            "    }",
        ),
        RuntimeDefect::ProvenanceOnly | RuntimeDefect::LaunchFailure => "",
        RuntimeDefect::PassExitOne | RuntimeDefect::CounterexampleExitOne => {
            "    std::process::exit(1);"
        }
    };
    SIMULATOR_FIXTURE_SOURCE
        .replace("__EVENT__", &event.to_string())
        .replace("__MALFORMED_EVENT__", malformed)
        .replace("__TERMINATION_WAIT__", termination_wait)
}

pub(super) fn initialize_fixture_repository(root: &Path) {
    git(root, &["init", "-q"]);
    git(root, &["add", "."]);
    git(
        root,
        &[
            "-c",
            "user.name=Rafter Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "-m",
            "test: materialize timeout evidence fixture",
        ],
    );
}
