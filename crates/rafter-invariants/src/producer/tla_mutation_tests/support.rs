use std::{fs, path::Path, process::Command};

use crate::producer::tla_output::{render_detector_config, DetectorProbe};

pub(super) const ELECTION_PROBE: DetectorProbe = DetectorProbe {
    predicate: "ElectionSafety",
    mode: "ElectionRecorderOnly",
};
pub(super) const LOG_MATCHING_PROBE: DetectorProbe = DetectorProbe {
    predicate: "LogMatching",
    mode: "LogMatchingRecorderOnly",
};
pub(super) const SNAPSHOT_PREFIX_PROBE: DetectorProbe = DetectorProbe {
    predicate: "LogMatching",
    mode: "SnapshotPrefixRecorderOnly",
};
pub(super) const LEADER_COMPLETENESS_PROBE: DetectorProbe = DetectorProbe {
    predicate: "LeaderCompleteness",
    mode: "LeaderCompletenessRecorderOnly",
};
pub(super) const COMMITTED_PREFIX_PROBE: DetectorProbe = DetectorProbe {
    predicate: "CommittedPrefixStability",
    mode: "CommittedPrefixRecorderOnly",
};
pub(super) const HIGHER_TERM_PROBE: DetectorProbe = DetectorProbe {
    predicate: "StaleLeaderFencing",
    mode: "HigherTermRecorderOnly",
};
pub(super) const STALE_AUTHORITY_PROBE: DetectorProbe = DetectorProbe {
    predicate: "StaleLeaderFencing",
    mode: "StaleAuthorityRecorderOnly",
};
pub(super) const APPLICATION_PROBE: DetectorProbe = DetectorProbe {
    predicate: "StateMachineSafety",
    mode: "ApplicationRecorderOnly",
};
pub(super) const APPLICATION_EPOCH_PROBE: DetectorProbe = DetectorProbe {
    predicate: "StateMachineSafety",
    mode: "ApplicationEpochRecorderOnly",
};
pub(super) const COMMIT_QUORUM_PROBE: DetectorProbe = DetectorProbe {
    predicate: "CommittedEntriesHaveQuorum",
    mode: "CommitQuorumRecorderOnly",
};
pub(super) const READ_BARRIER_PROBE: DetectorProbe = DetectorProbe {
    predicate: "ReadBarrierLinearizability",
    mode: "ReadBarrierRecorderOnly",
};

pub(super) const REMOVED_CANDIDATE_VOTE_GUARD_CONFIG: &str = r#"SPECIFICATION RemovedCandidateVoteGuardSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "ElectionSafety"

INVARIANT TypeOK
INVARIANT RemovedCandidateVoteGuardInvariant

PROPERTY RemovedCandidateVoteGuardCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const SHORTER_CONFLICT_REPAIR_CONFIG: &str = r#"SPECIFICATION ShorterConflictRepairSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LogMatching"

INVARIANT ShorterConflictRepairInvariant

PROPERTY ShorterConflictRepairCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const FROZEN_APPEND_AUTHORITY_CONFIG: &str = r#"SPECIFICATION FrozenAppendAuthoritySpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LogMatching"

INVARIANT FrozenAppendAuthorityInvariant

PROPERTY FrozenAppendAuthorityCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const JOINT_QUORUM_REGRESSION_CONFIG: &str = r#"SPECIFICATION JointQuorumRegressionSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT JointQuorumOldSideCannotCommit
INVARIANT CommittedEntriesHaveQuorum
INVARIANT StateMachineSafety

PROPERTY JointQuorumRegressionCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const EFFECTIVE_OVERWRITE_REGRESSION_CONFIG: &str = r#"SPECIFICATION EffectiveOverwriteRegressionSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT EffectiveOverwriteRegressionInvariant
INVARIANT CommittedEntriesHaveQuorum

PROPERTY EffectiveOverwriteRegressionCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const DELAYED_HEARTBEAT_REGRESSION_CONFIG: &str = r#"SPECIFICATION DelayedHeartbeatRegressionSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT DelayedHeartbeatRegressionInvariant
INVARIANT CommittedEntriesHaveQuorum
INVARIANT StateMachineSafety

PROPERTY DelayedHeartbeatRegressionCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const SNAPSHOT_LIFECYCLE_CONFIG: &str = r#"SPECIFICATION SnapshotLifecycleSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LogMatching"

INVARIANT TypeOK
INVARIANT SnapshotLifecycleInvariant

PROPERTY SnapshotLifecycleCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const SIX_ENTRY_REPLAY_CONFIG: &str = r#"SPECIFICATION SixEntryReplaySpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 6
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "StateMachineSafety"

INVARIANT TypeOK
INVARIANT SixEntryReplayInvariant

CHECK_DEADLOCK FALSE
"#;

pub(super) const APPLICATION_EPOCH_LIFECYCLE_CONFIG: &str = r#"SPECIFICATION ApplicationEpochLifecycleSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "StateMachineSafety"

INVARIANT TypeOK
INVARIANT ApplicationEpochLifecycleInvariant

PROPERTY ApplicationEpochLifecycleCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const STALE_MESSAGE_LIFECYCLE_CONFIG: &str = r#"SPECIFICATION StaleMessageLifecycleSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "StaleLeaderFencing"

INVARIANT StaleMessageLifecycleInvariant

PROPERTY StaleMessageLifecycleCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const CLOSED_ELECTION_LIFECYCLE_CONFIG: &str = r#"SPECIFICATION ClosedElectionLifecycleSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "ElectionSafety"

INVARIANT ClosedElectionLifecycleInvariant

PROPERTY ClosedElectionLifecycleCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const COMMIT_AUTHORITY_TERM_REGRESSION_CONFIG: &str = r#"SPECIFICATION CommitAuthorityTermRegressionSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 3
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LeaderCompleteness"

INVARIANT CommitAuthorityTermRegressionInvariant

PROPERTY CommitAuthorityTermRegressionCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const COMMITTED_LEDGER_CANONICALIZATION_CONFIG: &str = r#"SPECIFICATION CommittedLedgerCanonicalizationSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 3
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "LeaderCompleteness"

INVARIANT TypeOK

PROPERTY CommittedLedgerCanonicalizationCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) const SELF_REMOVAL_COMMIT_CONFIG: &str = r#"SPECIFICATION SelfRemovalCommitSpec

CONSTANTS
  Nodes = {n1, n2, n3}
  Values = {v1, v2}
  MaxTerm = 2
  MaxLogLen = 2
  ReadRequests = {r1}
  FixtureA = n1
  FixtureB = n2
  FixtureC = n3
  FixtureValueA = v1
  FixtureValueB = v2
  FixtureRead = r1
  FixtureMode = "Default"
  TargetPredicate = "CommittedEntriesHaveQuorum"

INVARIANT TypeOK
INVARIANT SelfRemovalCommitInvariant
INVARIANT CommittedEntriesHaveQuorum

PROPERTY SelfRemovalCommitCompletes

CHECK_DEADLOCK FALSE
"#;

pub(super) fn workspace_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

pub(super) fn replace_operator(source: &str, operator: &str, next: &str, body: &str) -> String {
    let start = format!("{operator} ==");
    let end = format!("{next} ==");
    let (prefix, rest) = source.split_once(&start).expect("operator exists");
    let (_, suffix) = rest.split_once(&end).expect("next operator exists");
    format!("{prefix}{operator} == {body}\n\n{end}{suffix}")
}

pub(super) fn replace_exactly_once(source: &str, from: &str, to: &str) -> String {
    assert_eq!(source.matches(from).count(), 1, "mutation target is exact");
    source.replacen(from, to, 1)
}

pub(super) fn replace_exactly_once_in_operator(
    source: &str,
    operator: &str,
    next: &str,
    from: &str,
    to: &str,
) -> String {
    let start = format!("{operator} ==");
    let end = format!("{next} ==");
    let (prefix, rest) = source.split_once(&start).expect("operator exists");
    let (body, suffix) = rest.split_once(&end).expect("next operator exists");
    assert_eq!(body.matches(from).count(), 1, "operator mutation is exact");
    let mutated = body.replacen(from, to, 1);
    format!("{prefix}{start}{mutated}{end}{suffix}")
}

pub(super) fn run_tlc_mutation(
    root: &Path,
    name: &str,
    raft: &str,
    detector: &str,
    probe: DetectorProbe,
) -> std::process::Output {
    let template =
        fs::read_to_string(root.join("specs/tla/raft/RafterInvariantDetectorNegative.cfg"))
            .expect("read detector config");
    let config = render_detector_config(&template, probe).expect("render detector config");
    run_tlc_with_config(root, name, raft, detector, &config)
}

pub(super) fn run_tlc_with_config(
    root: &Path,
    name: &str,
    raft: &str,
    detector: &str,
    config: &str,
) -> std::process::Output {
    let directory = root
        .join("target/rafter-invariants/tla-mutations")
        .join(format!("{}-{name}", std::process::id()));
    if directory.exists() {
        fs::remove_dir_all(&directory).expect("remove stale mutation directory");
    }
    fs::create_dir_all(&directory).expect("create mutation directory");
    fs::write(directory.join("Raft.tla"), raft).expect("write mutated Raft spec");
    fs::write(
        directory.join("RafterInvariantDetectorNegative.tla"),
        detector,
    )
    .expect("write mutated detector spec");
    fs::write(
        directory.join("RafterInvariantDetectorNegative.cfg"),
        config,
    )
    .expect("write detector config");
    Command::new("java")
        .args([
            "-XX:+UseParallelGC",
            "-cp",
            &root.join("tools/cache/tla2tools.jar").to_string_lossy(),
            "tlc2.TLC",
            "-tool",
            "-workers",
            "1",
            "-seed",
            "2026071101",
            "-fp",
            "0",
            "-metadir",
            &directory.join("states").to_string_lossy(),
            "-config",
            "RafterInvariantDetectorNegative.cfg",
            "RafterInvariantDetectorNegative.tla",
        ])
        .current_dir(&directory)
        .output()
        .expect("run TLC mutation")
}
