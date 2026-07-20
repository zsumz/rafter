//! Scenarios for official profile orchestration and plan reloading.

use std::sync::atomic::{AtomicU64, Ordering};

use super::{run_all, RunAllOptions, RunAllOutcome};
use crate::gate::verify_and_write_report;

static TEST_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn run_all_does_not_accept_a_caller_authored_invocation_receipt() {
    let _: fn(&RunAllOptions) -> Result<RunAllOutcome, Box<dyn std::error::Error>> = run_all;
}

#[test]
fn official_writer_reloads_and_rejects_unverified_passing_bundles() {
    let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "rafter-invariants-official-writer-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).expect("create report scratch directory");
    let (catalog, manifest) = crate::tests::loaded();
    let fabricated = crate::tests::passing_bundles(&catalog, &manifest)
        .into_iter()
        .find(|bundle| bundle.runner == "tests")
        .expect("fabricated passing tests bundle");
    let plan = crate::plan::ExecutionPlan {
        catalog,
        manifest,
        receipt: fabricated.execution.plan.clone(),
    };
    let evidence = root.join("fabricated.json");
    std::fs::write(
        &evidence,
        serde_json::to_vec_pretty(&fabricated).expect("serialize fabricated bundle"),
    )
    .expect("write fabricated evidence");

    let error = verify_and_write_report(
        &plan,
        &fabricated.source_ref,
        &[evidence],
        &root.join("report"),
    )
    .expect_err("official writer must independently reload its active plan");
    assert!(error
        .to_string()
        .contains("verification/raft-invariants.yaml"));
    assert!(!root.join("report/pr.json").exists());
    std::fs::remove_dir_all(root).expect("remove report scratch directory");
}
