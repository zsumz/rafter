//! Nightly checkpoint recovery must bind the current model inputs.

use super::workflow_job_block;
use std::{fs, path::Path};

#[test]
fn nightly_tla_job_uses_exact_compatible_checkpointing() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let workflow = fs::read_to_string(root.join(".github/workflows/nightly.yml"))
        .expect("read nightly workflow");
    let job = workflow_job_block(&workflow, "  invariants-tla:");

    for required in [
        "Restore exact-compatible nightly TLC checkpoint",
        "target/rafter-invariants/tla-checkpoint/nightly",
        "tla-nightly-checkpoint-v2-${{ hashFiles(",
        "cargo run --locked -p rafter-invariants -- run --profile nightly --layer tla",
        "Save exact-compatible nightly TLC checkpoint",
        "specs/tla/raft/RaftNightly.cfg",
    ] {
        assert!(
            job.contains(required),
            "nightly source-bound TLA job omitted: {required}"
        );
    }
    assert_eq!(
        job.matches("tla-nightly-checkpoint-v2-${{ hashFiles(")
            .count(),
        3,
        "restore, fallback, and save keys must all bind the current model inputs"
    );
    assert!(
        !job.contains("tla-nightly-checkpoint-v1-"),
        "legacy checkpoint fallback must not recover an incompatible model"
    );
}
