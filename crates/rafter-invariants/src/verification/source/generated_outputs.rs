//! Verifier-owned allowlist for ignored generated checkout outputs.

use std::path::{Component, Path};

use crate::provenance::source::GeneratedOutputPolicy;

pub(super) struct VerifierGeneratedOutputs;

impl GeneratedOutputPolicy for VerifierGeneratedOutputs {
    fn permits(&self, path: &Path) -> bool {
        let components = path
            .components()
            .filter_map(|component| match component {
                Component::Normal(value) => Some(value.to_string_lossy()),
                _ => None,
            })
            .collect::<Vec<_>>();
        matches!(components.as_slice(), [first, ..] if first == "target" || first == "store")
            || matches!(components.as_slice(), [first, second, ..]
                if (first == "artifacts"
                    && (second == "invariants" || reviewed_tla_artifact(second)))
                    || (first == "bench-compare" && second == "target")
                    || (first == "fuzz" && second == "target")
                    || (first == "tools" && second == "cache"))
            || matches!(components.as_slice(), [first, second, third, ..]
                if first == "crates" && second == "rafter-invariants" && third == "target")
            || matches!(components.as_slice(), [first, second, rest @ ..]
                if first == "specs" && second == "tla" && rest.iter().any(|value| value == "states"))
            || components.iter().any(|value| value == "__pycache__")
            || path.extension().is_some_and(|extension| extension == "pyc")
    }
}

fn reviewed_tla_artifact(name: &str) -> bool {
    const FIXTURE_SUFFIXES: &[&str] = &[
        "ElectionSafety",
        "LogMatching-LogMatchingRecorderOnly",
        "LogMatching-SnapshotPrefixRecorderOnly",
        "LeaderCompleteness-LeaderCompletenessRecorderOnly",
        "CommittedPrefixStability-CommittedPrefixRecorderOnly",
        "StateMachineSafety",
        "StateMachineSafety-ApplicationEpochRecorderOnly",
        "StaleLeaderFencing-HigherTermRecorderOnly",
        "StaleLeaderFencing-StaleAuthorityRecorderOnly",
        "CommittedEntriesHaveQuorum-CommitQuorumRecorderOnly",
        "ReadBarrierLinearizability-ReadBarrierRecorderOnly",
    ];
    matches!(
        name,
        "tla-log"
            | "tla.log"
            | "tla-trace-log"
            | "tla-tool"
            | "tla-spec"
            | "tla-trace-spec"
            | "tla-detector-spec"
            | "tla-runner"
            | "tla-tool-asset-id"
            | "tla-tool-checksums"
            | "tla-config"
            | "tla-trace-config"
            | "tla-detector-config"
            | "tla-mutation-log"
            | "tla-producer"
            | "tla-checkpoint-contract"
            | "tla-checkpoint-inventory"
            | "tla-checkpoint-recovered-contract"
            | "tla-checkpoint-recovered-inventory"
            | "tla-checkpoint-recovery-report"
    ) || ["tla-detector-log-", "tla-detector-config-"]
        .into_iter()
        .any(|prefix| {
            name.strip_prefix(prefix)
                .is_some_and(|suffix| FIXTURE_SUFFIXES.contains(&suffix))
        })
}
