//! Scenarios: multi-trial checks resolve exactly one of each shared input.

use super::{group_trials, unique};
use crate::{ArtifactRef, CheckCompletion, CheckReceipt};

fn artifact(path: &str, kind: &str) -> ArtifactRef {
    ArtifactRef {
        kind: kind.to_owned(),
        path: format!("artifacts/invariants/evidence/{path}"),
        sha256: format!("{:064x}", path.len()),
        size_bytes: 1,
    }
}

/// Shared tool inputs live outside any `trial-N` directory; per-trial evidence
/// lives inside one. `trials` is how many trial directories the check carries.
fn check(trials: u64, shared_repeats: usize) -> CheckReceipt {
    let mut artifacts = Vec::new();
    for _ in 0..shared_repeats {
        for (path, kind) in [
            ("inputs/runner", "maelstrom-runner"),
            ("inputs/binary", "maelstrom-binary"),
            ("inputs/maelstrom.jar", "maelstrom-tool-jar"),
        ] {
            artifacts.push(artifact(path, kind));
        }
    }
    for trial in 0..trials {
        for (name, kind) in [
            ("results.edn", "maelstrom-results"),
            ("process.json", "maelstrom-process-log"),
        ] {
            artifacts.push(artifact(&format!("trial-{trial}/{name}"), kind));
        }
    }
    CheckReceipt {
        execution_id: "maelstrom-execution-0".to_owned(),
        check_id: "maelstrom/base".to_owned(),
        evidence_ids: vec!["RD-06/test".to_owned()],
        completion: CheckCompletion::Completed,
        observations: std::collections::BTreeMap::new(),
        simulator_liveness: None,
        tla_continuation: None,
        duration_ms: 1,
        peak_rss_kib: 1,
        artifacts,
    }
}

/// The weekly profile runs three trials against one captured runner script.
/// Grouping must put the shared inputs aside once, not once per trial, and each
/// trial must keep its own evidence.
#[test]
fn a_multi_trial_check_resolves_one_shared_input_per_kind() {
    for trials in 1..=3 {
        let check = check(trials, 1);
        let grouped = group_trials(&check).expect("multi-trial check groups");

        assert_eq!(
            grouped.trials.len(),
            usize::try_from(trials).expect("small")
        );
        for kind in ["maelstrom-runner", "maelstrom-binary", "maelstrom-tool-jar"] {
            unique(&grouped.shared, kind)
                .unwrap_or_else(|error| panic!("{trials} trials, {kind}: {error}"));
        }
        for artifacts in grouped.trials.values() {
            unique(artifacts, "maelstrom-results").expect("one result set per trial");
            unique(artifacts, "maelstrom-process-log").expect("one process log per trial");
        }
    }
}

/// The pre-fix producer listed each shared input once per trial, describing one
/// file as three artifacts. The verifier's invariant is right to refuse that,
/// and keeps refusing it: the fix belongs in the claim, not in this check.
#[test]
fn a_shared_input_repeated_per_trial_is_still_ambiguous() {
    let repeated = check(3, 3);
    let grouped = group_trials(&repeated).expect("check groups");
    let error = unique(&grouped.shared, "maelstrom-runner")
        .expect_err("a shared input listed once per trial is ambiguous");
    assert!(error.to_string().contains("ambiguous"), "{error}");
}
