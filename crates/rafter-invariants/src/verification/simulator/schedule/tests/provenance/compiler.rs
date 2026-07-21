//! Cargo compiler-artifact identity and executable-binding scenarios.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use super::super::{simulator_compiler_artifact_executable, simulator_program_matches};

static NEXT_SOURCE_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct CompilerSourceFixture {
    active_root: PathBuf,
    producer_root: PathBuf,
}

impl CompilerSourceFixture {
    fn new(label: &str) -> Self {
        let active_root = std::env::temp_dir().join(format!(
            "rafter-simulator-provenance-{label}-{}-{}",
            std::process::id(),
            NEXT_SOURCE_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&active_root);
        let source = active_root.join("crates/rafter-sim/src/bin/rafter-model-check-fast.rs");
        fs::create_dir_all(source.parent().expect("simulator source parent"))
            .expect("create active simulator source tree");
        fs::write(&source, "fn main() {}\n").expect("write active simulator source");
        let active_root = fs::canonicalize(active_root).expect("canonical active source root");
        let producer_root = active_root.with_extension("producer-root-a");
        let _ = fs::remove_dir_all(&producer_root);
        Self {
            active_root,
            producer_root,
        }
    }
}

impl Drop for CompilerSourceFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.active_root);
        let _ = fs::remove_dir_all(&self.producer_root);
    }
}

#[test]
fn simulator_runtime_path_and_digest_must_both_match_cargo_output() {
    let emitted = Path::new("/workspace/target/rafter-model-check-fast");
    let invocation = crate::InvocationReceipt {
        program: emitted.to_string_lossy().into_owned(),
        program_sha256: "a".repeat(64),
        arguments: vec!["--profile".to_owned(), "fast".to_owned()],
        current_dir: "/workspace".to_owned(),
        environment: BTreeMap::new(),
        environment_sha256: crate::provenance::invocation::digest_environment(&BTreeMap::new())
            .expect("valid fixture environment"),
        launchers: crate::receipt::fixture_launchers(false),
    };
    assert!(simulator_program_matches(
        &invocation,
        emitted,
        &"a".repeat(64),
    ));

    let mut substituted = invocation.clone();
    substituted.program = "/workspace/target/substituted-simulator".to_owned();
    assert!(!simulator_program_matches(
        &substituted,
        emitted,
        &"a".repeat(64),
    ));

    let mut wrong_digest = invocation;
    wrong_digest.program_sha256 = "b".repeat(64);
    assert!(!simulator_program_matches(
        &wrong_digest,
        emitted,
        &"a".repeat(64),
    ));
}

#[test]
fn simulator_compiler_artifact_rejects_provenance_substitutions() {
    let roots = CompilerSourceFixture::new("substitutions");
    let target_dir = roots.producer_root.join("target/simulator-build/exact");
    let exact = simulator_compiler_message(&roots.producer_root, &target_dir);
    simulator_compiler_artifact_executable(
        exact.to_string().as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .expect("exact simulator compiler-artifact verifies");

    let mut wrong_package = exact.clone();
    wrong_package["package_id"] = serde_json::json!(format!(
        "path+file://{}#0.0.1",
        roots.producer_root.join("crates/substituted").display()
    ));
    assert!(simulator_compiler_artifact_executable(
        wrong_package.to_string().as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("package_id"));

    let mut wrong_source = exact.clone();
    wrong_source["target"]["src_path"] = serde_json::json!(roots
        .producer_root
        .join("crates/rafter-sim/src/bin/substituted-model-check.rs"));
    assert!(simulator_compiler_artifact_executable(
        wrong_source.to_string().as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("source path"));

    let mut escaped_target = exact;
    escaped_target["executable"] = serde_json::json!(
        "/workspace/target/simulator-build/substituted/release/rafter-model-check-fast"
    );
    assert!(simulator_compiler_artifact_executable(
        escaped_target.to_string().as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("exact release target"));
}

#[test]
fn simulator_compiler_artifact_rejects_missing_and_ambiguous_outputs() {
    let roots = CompilerSourceFixture::new("missing-ambiguous");
    let target_dir = roots.producer_root.join("target/simulator-build/exact");
    let exact = simulator_compiler_message(&roots.producer_root, &target_dir);

    let mut missing_executable = exact.clone();
    missing_executable
        .as_object_mut()
        .expect("compiler message object")
        .remove("executable");
    assert!(simulator_compiler_artifact_executable(
        missing_executable.to_string().as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("omitted its executable"));

    let duplicated = format!("{exact}\n{exact}\n");
    assert!(simulator_compiler_artifact_executable(
        duplicated.as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("found 2"));

    assert!(simulator_compiler_artifact_executable(
        b"not a Cargo message\n",
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .unwrap_err()
    .to_string()
    .contains("found 0"));
}

#[test]
fn simulator_compiler_artifact_preserves_prefix_malformed_and_suffix_filtering() {
    let roots = CompilerSourceFixture::new("filtering");
    let target_dir = roots.producer_root.join("target/simulator-build/exact");
    let exact = simulator_compiler_message(&roots.producer_root, &target_dir);
    let mut prefix = exact.clone();
    prefix["target"]["name"] = serde_json::json!("prefix-rafter-model-check-fast");
    let mut suffix = exact.clone();
    suffix["target"]["name"] = serde_json::json!("rafter-model-check-fast-suffix");
    let stdout = format!("{prefix}\n{{malformed Cargo JSON\n{exact}\n{suffix}\n");

    let executable = simulator_compiler_artifact_executable(
        stdout.as_bytes(),
        &roots.producer_root,
        &roots.active_root,
        &target_dir,
    )
    .expect("only the exact target name is selected");
    assert_eq!(
        executable,
        target_dir.join("release/rafter-model-check-fast")
    );
}

fn simulator_compiler_message(source_root: &Path, target_dir: &Path) -> serde_json::Value {
    serde_json::json!({
        "reason": "compiler-artifact",
        "package_id": format!(
            "path+file://{}#0.0.1",
            source_root.join("crates/rafter-sim").to_string_lossy()
        ),
        "target": {
            "name": "rafter-model-check-fast",
            "kind": ["bin"],
            "crate_types": ["bin"],
            "src_path": source_root.join(
                "crates/rafter-sim/src/bin/rafter-model-check-fast.rs"
            ),
        },
        "executable": target_dir.join("release/rafter-model-check-fast"),
        "fresh": false,
    })
}
