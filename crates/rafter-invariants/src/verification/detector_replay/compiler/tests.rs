//! Adversarial Cargo compiler-transcript acceptance scenarios.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use serde_json::json;

use super::{bind_fresh_executables, CompiledReplayTarget};
use crate::{
    execution::filesystem::HeldDirectory, verification::detector_replay::metadata::CompilationGraph,
};

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

#[test]
fn malformed_and_truncated_messages_are_rejected() {
    with_fixture(|fixture| {
        for transcript in [
            b"not-json\n".as_slice(),
            b"{\"reason\":\"compiler".as_slice(),
        ] {
            let error = parse(fixture, transcript).expect_err("malformed transcript must fail");
            assert!(error.contains("is not JSON"), "{error}");
        }
    });
}

#[test]
fn unknown_message_reason_is_rejected() {
    with_fixture(|fixture| {
        let transcript = line(&json!({"reason": "future-cargo-message"}));
        let error = parse(fixture, &transcript).expect_err("unknown reason must fail");
        assert!(error.contains("unknown reason"), "{error}");
    });
}

#[test]
fn cached_compiler_artifact_is_rejected() {
    with_fixture(|fixture| {
        let transcript = line(&json!({
            "reason": "compiler-artifact",
            "package_id": "fixture-package",
            "target": {
                "name": "fixture",
                "kind": ["lib"],
                "src_path": fixture.source,
            },
            "profile": {"test": false},
            "fresh": true,
            "executable": null,
        }));
        let error = parse(fixture, &transcript).expect_err("fresh artifact must fail");
        assert!(error.contains("cached compiler artifact"), "{error}");
    });
}

#[test]
fn duplicate_build_completion_is_rejected() {
    with_fixture(|fixture| {
        let mut transcript = line(&json!({"reason": "build-finished", "success": true}));
        transcript.extend(line(&json!({"reason": "build-finished", "success": true})));
        let error = parse(fixture, &transcript).expect_err("duplicate completion must fail");
        assert!(error.contains("build_finished=2"), "{error}");
    });
}

#[test]
fn compiler_target_source_escape_is_rejected() {
    with_fixture(|fixture| {
        let transcript = line(&json!({
            "reason": "compiler-message",
            "package_id": "fixture-package",
            "target": {
                "name": "fixture",
                "kind": ["lib"],
                "src_path": fixture.outside,
            },
        }));
        let error = parse(fixture, &transcript).expect_err("source escape must fail");
        assert!(
            error.contains("escapes its authenticated package"),
            "{error}"
        );
    });
}

#[test]
fn compiler_target_identity_must_match_authenticated_metadata() {
    with_fixture(|fixture| {
        let transcript = line(&json!({
            "reason": "compiler-message",
            "package_id": "fixture-package",
            "target": {
                "name": "substituted",
                "kind": ["lib"],
                "src_path": fixture.source,
            },
        }));
        let error = parse(fixture, &transcript).expect_err("target substitution must fail");
        assert!(
            error.contains("does not match authenticated metadata"),
            "{error}"
        );
    });
}

#[test]
fn compiled_executable_detects_same_inode_content_mutation() {
    with_fixture(|fixture| {
        let executable = fixture.target.external_path().join("fixture-bin");
        fs::write(&executable, b"compiler-produced").expect("write executable");
        let handle = fixture
            .target
            .hold_file(Path::new("fixture-bin"))
            .expect("hold executable");
        let compiled =
            CompiledReplayTarget::bind(executable.clone(), handle).expect("bind executable bytes");
        fs::write(&executable, b"mutated-in-place").expect("mutate executable in place");

        let error = compiled
            .revalidate()
            .expect_err("content mutation must fail");
        assert!(error.contains("bytes changed after compilation"), "{error}");
    });
}

fn parse(fixture: &Fixture<'_>, transcript: &[u8]) -> Result<(), String> {
    bind_fresh_executables(
        transcript,
        fixture.graph,
        Path::new("."),
        fixture.target,
        std::iter::empty(),
    )
    .map(|_| ())
}

fn line(value: &serde_json::Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).expect("serialize compiler message");
    bytes.push(b'\n');
    bytes
}

fn with_fixture(test: impl FnOnce(&Fixture<'_>)) {
    let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let root = PathBuf::from("target/rafter-invariants/compiler-transcript-tests")
        .join(format!("{}-{id}", std::process::id()));
    let package = root.join("package");
    let source = package.join("src/lib.rs");
    let outside = root.join("outside.rs");
    fs::create_dir_all(source.parent().expect("source parent")).expect("create package source");
    fs::write(&source, "pub fn fixture() {}\n").expect("write package source");
    fs::write(&outside, "pub fn outside() {}\n").expect("write outside source");
    let target = HeldDirectory::replace_tree(
        &root.join("target"),
        crate::execution::filesystem::TREE_LIMITS,
        crate::execution::filesystem::OperationDeadline::none("compiler transcript fixture"),
    )
    .expect("create held target");
    let package = fs::canonicalize(package).expect("canonical package");
    let graph = CompilationGraph::fixture("fixture-package", "fixture", package);
    let source = fs::canonicalize(source).expect("canonical source");
    let outside = fs::canonicalize(outside).expect("canonical outside source");
    test(&Fixture {
        graph: &graph,
        target: &target,
        source: &source,
        outside: &outside,
    });
}

struct Fixture<'a> {
    graph: &'a CompilationGraph,
    target: &'a HeldDirectory,
    source: &'a Path,
    outside: &'a Path,
}
