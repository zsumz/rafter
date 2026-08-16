//! Checkpoint scenarios: scheduled TLC state is complete, spec-bound, and fail-closed.

use super::support::*;

#[test]
fn weekly_full_tlc_is_source_bound_checkpointed_and_fail_closed() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/weekly.yml"));
    assert!(!workflow.contains("\n  tlc-full:\n"));
    assert!(!workflow.contains("best-effort"));

    let tla = job_block(&workflow, "invariants-tla");
    for required in [
        "timeout-minutes: 360",
        "timeout-minutes: 330",
        "runs-on: ubuntu-24.04",
        "actions/cache/restore@0057852bfaa89a56745cba8c7296529d2fc39830",
        "Restore exact-compatible weekly TLC checkpoint",
        "target/rafter-invariants/tla-checkpoint/weekly",
        "cargo run --locked -p rafter-invariants -- run --profile weekly --layer tla",
        "actions/cache/save@0057852bfaa89a56745cba8c7296529d2fc39830",
        "Save exact-compatible weekly TLC checkpoint",
        "if: always()",
        "Prune superseded weekly TLC checkpoints",
        "actions/caches?key=tla-weekly-checkpoint-&sort=created_at&direction=desc",
        "actions: write",
    ] {
        assert!(
            tla.contains(required),
            "weekly source-bound TLA job omitted: {required}"
        );
    }
    verify_checkpoint_tlc_inputs(tla, "weekly", "'specs/tla/raft/Raft.cfg'");

    let profile = read(&root.join("verification/raft-invariant-profiles.json"));
    // Scoped to the weekly runner's own configuration line. Searching the whole
    // document let another profile's value satisfy a weekly assertion, which is
    // how weekly's budget moved off 265m while this guard still read 265m and
    // stayed green.
    let profile = tla_configuration_line(&profile, "Raft.cfg");
    for required in [
        "\"config\": \"Raft.cfg\"",
        "\"soft_timeout\": \"110m\"",
        "\"total_timeout\": \"320m\"",
        "\"finalization_reserve\": \"10m\"",
        "\"workers\": \"auto\"",
        "\"checkpoint_minutes\": \"30\"",
        "\"checkpoint_gzip\": \"required\"",
        "\"max_heap\": \"8g\"",
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
fn nightly_tlc_checkpoint_hashes_complete_tlc_model_inputs() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/nightly.yml"));
    let tla = job_block(&workflow, "invariants-tla");
    for required in [
        "actions/cache/restore@0057852bfaa89a56745cba8c7296529d2fc39830",
        "Restore exact-compatible nightly TLC checkpoint",
        "target/rafter-invariants/tla-checkpoint/nightly",
        "actions/cache/save@0057852bfaa89a56745cba8c7296529d2fc39830",
        "Save exact-compatible nightly TLC checkpoint",
        "Prune superseded nightly TLC checkpoints",
        "actions/caches?key=tla-nightly-checkpoint-&sort=created_at&direction=desc",
        "actions: write",
    ] {
        assert!(
            tla.contains(required),
            "nightly source-bound TLA job omitted: {required}"
        );
    }
    verify_checkpoint_tlc_inputs(tla, "nightly", "'specs/tla/raft/RaftNightly.cfg'");
}

/// The one `configuration` line belonging to the runner pinned to `config`.
///
/// The profiles manifest keeps each runner's configuration map on a single
/// line, so this isolates one profile's pins from every other profile's.
fn tla_configuration_line<'a>(profile: &'a str, config: &str) -> &'a str {
    let needle = format!("\"config\": \"{config}\"");
    let matching = profile
        .lines()
        .filter(|line| line.contains(&needle))
        .collect::<Vec<_>>();
    match matching.as_slice() {
        [line] => line,
        found => panic!(
            "expected exactly one TLA configuration for {config}, found {}",
            found.len()
        ),
    }
}

/// Checkpoint reuse is sound exactly when every input TLC state depends on is
/// part of the cache key: the profile pins (seed, fp, symmetry, heap), the
/// specification and model configuration, the launcher script, and the pinned
/// tool. Workspace sources deliberately stay out of the key — reusing TLC
/// state across producer-code changes is what lets scheduled exploration
/// accumulate to a drained queue, and `strict-compatible-if-present` recovery
/// still fails closed if a restored checkpoint is not exactly compatible.
fn verify_checkpoint_tlc_inputs(tla_job: &str, profile: &str, config: &str) {
    for (tlc_input, occurrences) in [
        ("'verification/raft-invariant-profiles.json'", 3),
        ("'specs/tla/raft/Raft.tla'", 3),
        (config, 3),
        ("'specs/tla/raft/RaftMembershipTraceSample.tla'", 3),
        ("'specs/tla/raft/RaftMembershipTraceSample.cfg'", 3),
        ("'specs/tla/raft/RafterInvariantDetectorNegative.tla'", 3),
        ("'specs/tla/raft/RafterInvariantDetectorNegative.cfg'", 3),
        ("'scripts/tla-model-check'", 3),
        // The tool pins also key the tla2tools.jar download cache.
        ("'tools/tla/ASSET_ID'", 4),
        ("'tools/tla/SHA256SUMS'", 4),
    ] {
        assert_eq!(
            tla_job.matches(tlc_input).count(),
            occurrences,
            "{profile} checkpoint restore/save keys must all hash {tlc_input}"
        );
    }
    assert_eq!(
        tla_job
            .matches(&format!("tla-{profile}-checkpoint-v2-"))
            .count(),
        3,
        "{profile} checkpoint restore/save keys must all use the spec-bound v2 namespace"
    );
    for retired_input in [
        "'Cargo.toml'",
        "'Cargo.lock'",
        "'crates/rafter-invariants/Cargo.toml'",
        "'crates/rafter-invariants/src/**/*.rs'",
        "'crates/rafter-invariants/src/producer/*.rs'",
        "'crates/rafter-invariants/src/producer/filesystem/**/*.rs'",
        "'crates/rafter-invariants/src/producer/process/**/*.rs'",
        "'crates/rafter-invariants/src/producer/tla_checkpoint/**/*.rs'",
        "'crates/rafter-invariants/src/receipt_tla.rs'",
        "'crates/rafter-invariants/src/artifact_verify_tla.rs'",
    ] {
        assert!(
            !tla_job.contains(retired_input),
            "{profile} checkpoint hash reintroduced retired input {retired_input}: \
             hashing workspace sources resets TLC accumulation on every push"
        );
    }
}
