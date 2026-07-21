//! Scenarios for invocation capture and immutable plan-input binding.

use std::{
    ffi::OsString,
    sync::atomic::{AtomicU64, Ordering},
};

use sha2::{Digest, Sha256};

use crate::evidence::{PlanInput, ResultBundle};

use super::{
    capture::{capture_invocation_from, capture_invocation_from_program},
    validate::{confined_path, verify_bundle_plan, verify_plan_input},
};

#[test]
fn invocation_records_actual_argv_without_manifest_substitution() {
    let captured = capture_invocation_from(vec![
        OsString::from("target/debug/rafter-invariants"),
        OsString::from("run"),
        OsString::from("--profile"),
        OsString::from("pr"),
    ])
    .expect("invocation captures");
    let receipt = captured.receipt;
    assert_eq!(
        receipt.program,
        std::fs::canonicalize(std::env::current_exe().expect("current executable"))
            .expect("current executable canonicalizes")
            .to_string_lossy()
    );
    assert_eq!(receipt.arguments, ["run", "--profile", "pr"]);
    assert_eq!(receipt.environment_sha256.len(), 64);
}

#[test]
fn invocation_freezes_program_bytes_before_later_cargo_rebuilds() {
    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "rafter-invariants-invocation-{}-{}",
        std::process::id(),
        NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&directory).expect("create temporary program directory");
    let program = directory.join("rafter-invariants");
    let initial = b"initial producer image";
    std::fs::write(&program, initial).expect("write initial producer image");

    let captured = capture_invocation_from_program(
        vec![
            OsString::from("rafter-invariants"),
            OsString::from("run-all"),
            OsString::from("--profile"),
            OsString::from("pr"),
        ],
        &program,
    )
    .expect("invocation captures immutable program bytes");
    std::fs::write(&program, b"later cargo rebuild").expect("replace producer path");

    assert_eq!(captured.program_bytes, initial);
    assert_eq!(
        captured.receipt.program_sha256,
        format!("{:x}", Sha256::digest(initial))
    );
    assert_ne!(
        captured.receipt.program_sha256,
        format!(
            "{:x}",
            Sha256::digest(std::fs::read(&program).expect("read replacement"))
        )
    );
    std::fs::remove_dir_all(directory).expect("remove temporary program directory");
}

#[test]
fn plan_paths_reject_absolute_and_parent_traversal() {
    let root = std::fs::canonicalize(".").expect("workspace root");
    assert!(confined_path(std::path::Path::new("/tmp/input"), &root).is_err());
    assert!(confined_path(std::path::Path::new("../input"), &root).is_err());
}

#[test]
fn plan_input_digest_detects_exact_byte_drift() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let bytes =
        std::fs::read(root.join("verification/raft-invariants.yaml")).expect("registry reads");
    let mut changed = PlanInput {
        path: "verification/raft-invariants.yaml".to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        size_bytes: bytes.len() as u64,
    };
    verify_plan_input(&changed, &root).expect("unchanged registry verifies");
    changed.sha256 = "0".repeat(64);
    assert!(verify_plan_input(&changed, &root).is_err());
}

#[test]
fn active_plan_binding_rejects_alternate_input_paths() {
    let (_, manifest) = crate::tests::loaded();
    let expected = crate::tests::plan_receipt(&manifest, "pr");
    let mut bundle: ResultBundle =
        crate::tests::passing_bundles(&crate::tests::loaded().0, &manifest).remove(0);
    bundle.execution.plan.registry.path = "verification/alternate.yaml".to_owned();
    assert!(verify_bundle_plan(&bundle, &expected).is_err());
}
