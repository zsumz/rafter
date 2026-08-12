//! Scenarios: a multi-trial check claims each shared input exactly once.

use super::deduplicated;
use crate::ArtifactRef;

fn artifact(path: &str, kind: &str) -> ArtifactRef {
    ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_owned(),
        sha256: format!("{:064x}", path.len()),
        size_bytes: 1,
    }
}

fn trial_artifacts(trial: u64) -> Vec<ArtifactRef> {
    vec![
        artifact("inputs/runner", "maelstrom-runner"),
        artifact("inputs/maelstrom.jar", "maelstrom-tool-jar"),
        artifact(
            &format!("trial-{trial}/process.json"),
            "maelstrom-process-log",
        ),
        artifact(&format!("trial-{trial}/results.edn"), "maelstrom-results"),
    ]
}

/// A single-trial check has nothing to collapse, so nightly's receipt must come
/// out of this byte-identical to the list that went in -- same entries, same
/// order.
#[test]
fn a_single_trial_receipt_is_unchanged() {
    let artifacts = trial_artifacts(0);
    assert_eq!(deduplicated(artifacts.clone()), artifacts);
}

/// Weekly runs three trials against one captured runner script and one jar.
/// Those are one file each, so the receipt names them once each, while every
/// trial keeps its own evidence.
#[test]
fn repeated_shared_inputs_collapse_and_trial_evidence_survives() {
    let claimed = (0..3).flat_map(trial_artifacts).collect::<Vec<_>>();
    assert_eq!(claimed.len(), 12);

    let deduped = deduplicated(claimed);

    assert_eq!(deduped.len(), 8);
    for kind in ["maelstrom-runner", "maelstrom-tool-jar"] {
        assert_eq!(
            deduped.iter().filter(|a| a.kind == kind).count(),
            1,
            "{kind} is one file however many trials referenced it"
        );
    }
    for trial in 0..3 {
        assert!(deduped
            .iter()
            .any(|a| a.path == format!("trial-{trial}/results.edn")));
    }
    // Order is preserved: the first trial's shared inputs still lead.
    assert_eq!(deduped[0].kind, "maelstrom-runner");
}

/// Two different files never collapse, however similar their identities.
#[test]
fn distinct_artifacts_are_never_collapsed() {
    let mut shifted = artifact("inputs/runner", "maelstrom-runner");
    shifted.sha256 = "1".repeat(64);
    let artifacts = vec![artifact("inputs/runner", "maelstrom-runner"), shifted];
    assert_eq!(deduplicated(artifacts.clone()), artifacts);
}
