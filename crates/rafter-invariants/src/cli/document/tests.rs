//! Scenarios for stale and canonical rendered registry documents.

use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use super::execute;

static DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn document_check_fails_stale_and_accepts_canonical_output() {
    let id = DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "rafter-invariants-document-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).expect("test directory exists");
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let registry = workspace.join("verification/raft-invariants.yaml");
    let output = root.join("raft-invariants.md");
    std::fs::write(&output, "stale\n").expect("stale fixture writes");

    assert!(execute(&registry, &output, true).is_err());
    assert!(execute(&registry, &output, false).expect("render document"));
    assert!(execute(&registry, &output, true).expect("check current document"));
    let _ = std::fs::remove_dir_all(root);
}
