//! Checkpoint scenarios: scheduled TLC state is complete, source-bound, and fail-closed.

use super::support::*;

#[test]
fn weekly_full_tlc_is_source_bound_checkpointed_and_fail_closed() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/weekly.yml"));
    assert!(!workflow.contains("\n  tlc-full:\n"));
    assert!(!workflow.contains("best-effort"));

    let tla = job_block(&workflow, "invariants-tla");
    for required in [
        "timeout-minutes: 400",
        "timeout-minutes: 360",
        "runs-on: [self-hosted, linux, X64]",
        "actions/cache/restore@0057852bfaa89a56745cba8c7296529d2fc39830",
        "Restore exact-compatible weekly TLC checkpoint",
        "target/rafter-invariants/tla-checkpoint/weekly",
        "tla-weekly-checkpoint-v1-",
        "cargo run --locked -p rafter-invariants -- run --profile weekly --layer tla",
        "actions/cache/save@0057852bfaa89a56745cba8c7296529d2fc39830",
        "Save exact-compatible weekly TLC checkpoint",
        "if: always()",
    ] {
        assert!(
            tla.contains(required),
            "weekly source-bound TLA job omitted: {required}"
        );
    }
    verify_checkpoint_source_inputs(tla, "weekly");

    let profile = read(&root.join("verification/raft-invariant-profiles.json"));
    for required in [
        "\"config\": \"Raft.cfg\"",
        "\"soft_timeout\": \"295m\"",
        "\"total_timeout\": \"350m\"",
        "\"finalization_reserve\": \"10m\"",
        "\"workers\": \"auto\"",
        "\"checkpoint_minutes\": \"30\"",
        "\"checkpoint_gzip\": \"required\"",
        "\"max_heap\": \"4g\"",
        "\"fp_mem\": \"0.45\"",
        "\"checkpoint_recovery\": \"strict-compatible-if-present\"",
        "\"unsymmetrized_exploration\": \"required\"",
    ] {
        assert!(
            profile.contains(required),
            "weekly TLA profile omitted: {required}"
        );
    }
}

#[test]
fn nightly_tlc_checkpoint_hashes_complete_invariant_sources() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/nightly.yml"));
    let tla = job_block(&workflow, "invariants-tla");
    for required in [
        "actions/cache/restore@0057852bfaa89a56745cba8c7296529d2fc39830",
        "Restore exact-compatible nightly TLC checkpoint",
        "target/rafter-invariants/tla-checkpoint/nightly",
        "tla-nightly-checkpoint-v1-",
        "actions/cache/save@0057852bfaa89a56745cba8c7296529d2fc39830",
        "Save exact-compatible nightly TLC checkpoint",
    ] {
        assert!(
            tla.contains(required),
            "nightly source-bound TLA job omitted: {required}"
        );
    }
    verify_checkpoint_source_inputs(tla, "nightly");
}

fn verify_checkpoint_source_inputs(tla_job: &str, profile: &str) {
    for source_input in [
        "'Cargo.toml'",
        "'Cargo.lock'",
        "'crates/rafter-invariants/Cargo.toml'",
        "'crates/rafter-invariants/src/**/*.rs'",
    ] {
        assert_eq!(
            tla_job.matches(source_input).count(),
            3,
            "{profile} checkpoint restore/save keys must all hash {source_input}"
        );
    }
    for retired_glob in [
        "'crates/rafter-invariants/src/producer/*.rs'",
        "'crates/rafter-invariants/src/producer/filesystem/**/*.rs'",
        "'crates/rafter-invariants/src/producer/process/**/*.rs'",
        "'crates/rafter-invariants/src/producer/tla_checkpoint/**/*.rs'",
        "'crates/rafter-invariants/src/receipt_tla.rs'",
        "'crates/rafter-invariants/src/artifact_verify_tla.rs'",
    ] {
        assert!(
            !tla_job.contains(retired_glob),
            "{profile} checkpoint hash still uses brittle input {retired_glob}"
        );
    }
}
