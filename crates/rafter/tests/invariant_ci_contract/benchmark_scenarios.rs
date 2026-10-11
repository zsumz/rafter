//! Benchmark scenarios: repeated evidence and allocation gates stay executable.

use super::support::*;

#[test]
fn wal_recovery_allocation_gate_runs_in_ci_and_local_lint() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/ci.yml"));
    let step = workflow_step(job_block(&workflow, "lint"), "WAL recovery allocation gate");
    assert!(step.contains("run: cargo run --manifest-path bench-compare/Cargo.toml --locked --no-default-features --bin check-wal-recovery-memory"));
    assert!(!step.contains("if:"));
    assert!(!step.contains("continue-on-error:"));
    let local = read(&root.join("zcheck.toml"));
    let task = local
        .split("[tasks.wal-recovery-memory]")
        .nth(1)
        .expect("local allocation task")
        .split("[tasks.")
        .next()
        .expect("task body");
    assert!(task.contains("run = [\"cargo\", \"run\", \"--manifest-path\", \"bench-compare/Cargo.toml\", \"--locked\", \"--no-default-features\", \"--bin\", \"check-wal-recovery-memory\"]"));
    let lint = local
        .split("[tasks.lint]")
        .nth(1)
        .expect("lint task")
        .split("[tasks.")
        .next()
        .expect("lint body");
    assert!(lint.contains("\"wal-recovery-memory\","));
}

#[test]
fn model_check_overhead_evidence_is_repeated_and_durable() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/benchmarks.yml"));
    let smoke = job_block(&workflow, "smoke");
    assert!(smoke.contains("python3 -m unittest scripts/tests/test_model_check_profile_report.py"));
    assert!(smoke.contains("test -x scripts/model-check-profile-compare"));

    let evidence = job_block(&workflow, "model-check-evidence");
    for required in [
        "fetch-depth: 0",
        "MODEL_CHECK_PROFILES: fast",
        "MODEL_CHECK_RUNS: \"6\"",
        "scripts/model-check-profile-compare",
        "if: always()",
        "GITHUB_STEP_SUMMARY",
        "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
        "if-no-files-found: error",
        "retention-days: 30",
    ] {
        assert!(
            evidence.contains(required),
            "model-check evidence omitted required contract fragment: {required}"
        );
    }
}
