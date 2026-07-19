//! Benchmark scenarios: verifier overhead evidence stays repeated and durable.

use super::support::*;

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
        "actions/upload-artifact@v4",
        "if-no-files-found: error",
        "retention-days: 30",
    ] {
        assert!(
            evidence.contains(required),
            "model-check evidence omitted required contract fragment: {required}"
        );
    }
}
