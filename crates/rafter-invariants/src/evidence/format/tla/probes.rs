//! Registered detector probes and their artifact-kind vocabulary.
//!
//! A probe is a registered predicate paired with the fixture mode that drives
//! it to a counterexample. Only the reviewed pairs name an artifact, so an
//! unregistered probe cannot mint an evidence identity.

use super::{DEFAULT_FIXTURE_MODE, REGISTERED_PREDICATES};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DetectorProbe {
    pub(crate) predicate: &'static str,
    pub(crate) mode: &'static str,
}

pub(crate) const DETECTOR_PROBES: [DetectorProbe; 11] = [
    DetectorProbe {
        predicate: "ElectionSafety",
        mode: DEFAULT_FIXTURE_MODE,
    },
    DetectorProbe {
        predicate: "LogMatching",
        mode: "LogMatchingRecorderOnly",
    },
    DetectorProbe {
        predicate: "LogMatching",
        mode: "SnapshotPrefixRecorderOnly",
    },
    DetectorProbe {
        predicate: "LeaderCompleteness",
        mode: "LeaderCompletenessRecorderOnly",
    },
    DetectorProbe {
        predicate: "CommittedPrefixStability",
        mode: "CommittedPrefixRecorderOnly",
    },
    DetectorProbe {
        predicate: "StateMachineSafety",
        mode: DEFAULT_FIXTURE_MODE,
    },
    DetectorProbe {
        predicate: "StateMachineSafety",
        mode: "ApplicationEpochRecorderOnly",
    },
    DetectorProbe {
        predicate: "StaleLeaderFencing",
        mode: "HigherTermRecorderOnly",
    },
    DetectorProbe {
        predicate: "StaleLeaderFencing",
        mode: "StaleAuthorityRecorderOnly",
    },
    DetectorProbe {
        predicate: "CommittedEntriesHaveQuorum",
        mode: "CommitQuorumRecorderOnly",
    },
    DetectorProbe {
        predicate: "ReadBarrierLinearizability",
        mode: "ReadBarrierRecorderOnly",
    },
];

pub(super) fn is_registered_predicate(predicate: &str) -> bool {
    REGISTERED_PREDICATES.contains(&predicate)
}

pub(super) fn is_registered_probe(probe: DetectorProbe) -> bool {
    DETECTOR_PROBES.contains(&probe)
}

pub(super) fn is_valid_fixture_probe(probe: DetectorProbe) -> bool {
    is_registered_predicate(probe.predicate)
        && (probe.mode == DEFAULT_FIXTURE_MODE
            || matches!(
                (probe.predicate, probe.mode),
                ("ElectionSafety", "ElectionRecorderOnly")
                    | (
                        "LogMatching",
                        "LogMatchingRecorderOnly" | "SnapshotPrefixRecorderOnly"
                    )
                    | ("LeaderCompleteness", "LeaderCompletenessRecorderOnly")
                    | ("CommittedPrefixStability", "CommittedPrefixRecorderOnly")
                    | (
                        "StateMachineSafety",
                        "ApplicationRecorderOnly" | "ApplicationEpochRecorderOnly"
                    )
                    | (
                        "StaleLeaderFencing",
                        "HigherTermRecorderOnly" | "StaleAuthorityRecorderOnly"
                    )
                    | ("CommittedEntriesHaveQuorum", "CommitQuorumRecorderOnly")
                    | ("ReadBarrierLinearizability", "ReadBarrierRecorderOnly")
            ))
}

pub(super) fn artifact_kind(prefix: &str, probe: DetectorProbe) -> String {
    if probe.mode == DEFAULT_FIXTURE_MODE {
        format!("{prefix}:{}", probe.predicate)
    } else {
        format!("{prefix}:{}:{}", probe.predicate, probe.mode)
    }
}
